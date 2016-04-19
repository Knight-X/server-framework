#include "async.h"

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>

enum {
    ASYNC_ERROR,
    ASYNC_WARNING,
    ASYNC_DEBUG
};

#define debug(level, ...) do { \
    if (level < ASYNC_DEBUG) { \
        flockfile(stdout); \
        printf("###%p.%s: ", (void *)pthread_self(), __func__); \
        printf(__VA_ARGS__); \
        putchar('\n'); \
        fflush(stdout); \
        funlockfile(stdout); \
    } \
} while (0)

#define WORK_QUEUE_POWER 16 
#define JOB_QUEUE_SIZE (1 << WORK_QUEUE_POWER)
#define JOB_QUEUE_MASK (JOB_QUEUE_SIZE - 1)

#define queue_out_val(thread) (__sync_val_compare_and_swap(&(thread)->out, 0, 0))
#define queue_len(thread) ((thread)->in - queue_out_val(thread))
#define queue_empty(thread) (queue_len(thread) == 0)
#define queue_full(thread) (queue_len(thread) == JOB_QUEUE_SIZE)
#define job_offset(val) ((val) & JOB_QUEUE_MASK)

static volatile int global_num_threads = 0;
static pthread_t main_thread;

/* suppress compilation warnings */
static inline ssize_t write_wrapper(int fd, const void *buf, size_t count)
{
    ssize_t s;
    if ((s = write(fd, buf, count)) < count) perror("write");
    return s;
}
#undef write
#define write write_wrapper

/* the actual working thread */
static void *worker_thread_cycle(void *async);


/* the destructor */
static void async_destroy(async_p queue);

static void *join_thread(pthread_t thr)
{
    void *ret;
    pthread_join(thr, &ret);
    return ret;
}


static int wait_for_registration(int expected) {
    sigset_t zeromask, newmask, oldmask;
        //debug(ASYNC_ERROR, "WAITING");

    sigemptyset(&zeromask);
    sigemptyset(&newmask);
    sigaddset(&newmask, SIGUSR1);

    if (sigprocmask(SIG_BLOCK, &newmask, &oldmask) < 0) {
        debug(ASYNC_ERROR, "SIG_BLOCK_FAILED");
        return -1;
    }

        //debug(ASYNC_ERROR, "GO TO SLEEP");
    while (global_num_threads < expected) {
        //debug(ASYNC_ERROR, "SLEEP");
        sigsuspend(&zeromask);
    }
        //debug(ASYNC_ERROR, "STOP SLEEP");

    if (sigprocmask(SIG_SETMASK, &oldmask, NULL) < 0) {
        return -1;
    }

    return 0;
}


/** A task node */
struct AsyncTask {
    struct AsyncTask *next;
    void (*task)(void *);
    void *arg;
};

typedef struct {
    pthread_t       id;
    unsigned int    in;
    unsigned int    out;
    unsigned run : 1; /**< the running flag */
    struct AsyncTask       job_queue[JOB_QUEUE_SIZE];
} thread_u;

typedef thread_u *(*schedule_func)(struct Async *);
/** The Async struct */
struct Async {
    /** the task queue - MUST be first in the struct */

    int count; /**< the number of initialized threads */

    schedule_func schedule_thread;
    
    /** the thread pool */
    thread_u threads[];
};

static int create_threads(async_p async, int index)
{
    memset(&async->threads[index], 0, sizeof(thread_u));
    return pthread_create(&async->threads[index].id, NULL, worker_thread_cycle, (void *)(&async->threads[index]));
}

static thread_u *round_robin_schedule(async_p async)
{
    static int cur_thr_index = -1;

    cur_thr_index = (cur_thr_index + 1) % async->count;

    return &async->threads[cur_thr_index];
}

static int dispatch_2thread(async_p async, thread_u *thread, void (*task)(void *), void *arg) {
    struct AsyncTask *job = NULL;

    job = &thread->job_queue[job_offset(thread->in)];
    job->task = task;
    job->next = NULL;
    job->arg = arg;
    thread->in++;
    //debug(ASYNC_ERROR, "START dispatch");
    if (queue_len(thread) >= 0) {
        pthread_kill(thread->id, SIGUSR1);
    }

    return 0;
}

struct AsyncTask *get_workcurrently(thread_u *thread) 
{
    struct AsyncTask *job = NULL;
    unsigned int tmp;
    do {

        if (queue_len(thread) <= 0) {
            //debug(ASYNC_ERROR, "thread set");
            break;
        }

        tmp = thread->out;
   
        job = &thread->job_queue[job_offset(tmp)];
    } while (!__sync_bool_compare_and_swap(&thread->out, tmp, tmp + 1));
    //debug(ASYNC_ERROR, "GET WORK");
        return job;
}

/* Task Management - add a task and perform all tasks in queue */

static int async_run(async_p async, void (*task)(void *), void *arg)
{
    thread_u *thread;
    thread = async->schedule_thread(async);
    //debug(ASYNC_ERROR, "START SCHEDULE");
    return dispatch_2thread(async, thread, task, arg);
}

/** Performs all the existing tasks in the queue. */
static void perform_tasks(thread_u *thread)
{
    struct AsyncTask *c = NULL;  /* c == container, will store the task */

    c = get_workcurrently(thread);
    //debug(ASYNC_ERROR, "get work");

    if (c) {
    //debug(ASYNC_ERROR, "perform work");
        (*(c->task))(c->arg);
    }
}

/* The worker threads */

/* The worker cycle */
static void *worker_thread_cycle(void *_thread)
{
    /* setup signal and thread's local-storage async variable. */
    thread_u *thread = _thread;
    thread->run = 1;
    sigset_t zeromask, oldmask, newmask;

    __sync_fetch_and_add(&global_num_threads, 1);
    pthread_kill(main_thread, SIGUSR1);
    sigemptyset(&zeromask);
    sigemptyset(&newmask);
    sigaddset(&newmask, SIGUSR1);

    //debug(ASYNC_ERROR, "worker_thread");
    while (1) {
    //debug(ASYNC_ERROR, "running");

        if (sigprocmask(SIG_BLOCK, &newmask, &oldmask) < 0) {
            perror("sig failed");
            pthread_exit(NULL);
        }

        while (queue_empty(thread) && thread->run) {
    //debug(ASYNC_ERROR, "sleep");
            sigsuspend(&zeromask);
        }

        if (sigprocmask(SIG_SETMASK, &oldmask, NULL) < 0) {
            debug(ASYNC_ERROR, "set mask failed");
            pthread_exit(NULL);
        }

        if (thread->run == 0) {
    debug(ASYNC_ERROR, "dead");
            pthread_exit(NULL);
        }

        perform_tasks(thread);

        if (queue_empty(thread)) {
            pthread_kill(main_thread, SIGUSR1);
        }
    }

    return 0;
}

void sig_do_nothing()
{
    return;
}

static int async_queue_empty(async_p async)
{
    int i;
    for (i = 0; i < async->count; i++) {
        if (!queue_empty(&async->threads[i]))
            return 0;
    }
    return 1;
}

/* Signal and finish */


static void async_wait(async_p async)
{
    sigset_t zeromask, oldmask, newmask;
    sigemptyset(&zeromask);
    sigemptyset(&newmask);
    sigaddset(&newmask, SIGUSR1);

    if (sigprocmask(SIG_BLOCK, &newmask, &oldmask)) {
            debug(ASYNC_ERROR, "SIG BLOCK failed");
            pthread_exit(NULL);
    }

    while (!async_queue_empty(async))
        sigsuspend(&zeromask);

    if (sigprocmask(SIG_SETMASK, &oldmask, NULL) < 0) {
        debug(ASYNC_ERROR, "SIG_SETMASK failed");
        pthread_exit(NULL);
    }
}

static void async_finish(async_p async)
{
    async_wait(async);
    async_destroy(async);
}

/* Object creation and destruction */

/** Destroys the Async object, releasing its memory. */
static void async_destroy(async_p async)
{
    int i;
    for (i = 0; i < async->count; i++) {
        async->threads[i].run = 0;

        pthread_kill(async->threads[i].id, SIGUSR1);
    }

    for (i = 0; i < async->count; i++)
        join_thread(async->threads[i].id);

    free(async);
}

static async_p async_create(int threads)
{
    int i;
    async_p async = malloc(sizeof(*async) + (threads * sizeof(thread_u)));

    memset(async, 0, sizeof(*async) + (threads * sizeof(thread_u)));
    async->count = threads;
    debug(ASYNC_ERROR, "new in");
    async->schedule_thread = round_robin_schedule;
    /* create threads */
    if (signal(SIGUSR1, sig_do_nothing) == SIG_ERR) {
        return NULL;
    }

    main_thread = pthread_self();

    for (i = 0; i < threads; i++) {
        create_threads(async, i);
    debug(ASYNC_ERROR, "threads");
    }
    debug(ASYNC_ERROR, "threads");
    if (wait_for_registration(threads) < 0)
        pthread_exit(NULL);

    debug(ASYNC_ERROR, "threads");
    return async;
}

/* API gateway */
struct __ASYNC_API__ Async = {
    .create = async_create,
    .wait = async_wait,
    .finish = async_finish,
    .run = async_run,
};
