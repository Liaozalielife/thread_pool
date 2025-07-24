#include <pthread.h>
#include <stdint.h>
#include <stddef.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <sys/eventfd.h>
#include <stdio.h>
#include <stdalign.h>
#include "thrd_pool_eventfd.h"

#define TASKS_RING_BUFFER_size 1024
#define MASK (TASKS_RING_BUFFER_size - 1)

typedef struct task_s {
    handler_pt func;
    void *arg;
} task_t;

typedef struct task_queue_s {
    task_t* tasks[TASKS_RING_BUFFER_size];
    alignas(64) atomic_size_t head;
    alignas(64) atomic_size_t tail;
    alignas(64) atomic_size_t uncommit_tail;
    int efd;
} task_queue_t;

typedef struct _task_thread_t {
    pthread_t tid;
    task_queue_t thread_tasks;
} task_thread_t

struct thrdpool_s {
    task_queue_t *task_queue;
    atomic_int quit;
    int thrd_count;
    pthread_t *threads;
};

static task_queue_t *
__taskqueue_create() {
    task_queue_t *queue = (task_queue_t *) malloc(sizeof(*queue));
    if (!queue) {
        goto RETURN;
    }

    if ((queue->efd = eventfd(0, EFD_SEMAPHORE)) == -1) {
        goto FREE_QUEUE;
    }

    atomic_init(&queue->head, 0);
    atomic_init(&queue->tail, 0);
    atomic_init(&queue->uncommit_tail, 0);
    return queue;

FREE_QUEUE:
    free(queue);
RETURN:
    return NULL;
}

static void
__nonblock(task_queue_t *queue) {
    uint64_t awake = 8;
    write(queue->efd, &awake, sizeof(uint64_t));
}

static inline int
__enqueue(task_queue_t *queue, void* task, size_t index)
{
    return queue->tasks[index] = task;
}

static inline int 
__add_task(task_queue_t *queue, void *task) {
    size_t current_uncommit_tail;
    size_t next_tail;
    atomic_int flag;
    do {
        current_uncommit_tail = atomic_load(&queue->uncommit_tail);
        next_tail = (atomic_load(&queue->tail) + 1) & MASK;
        if (next_tail == atomic_load(&queue->head)) {
            return -1;
        }
    } while(!atomic_compare_exchange_weak(&queue->tail, &current_uncommit_tail, next_tail));
    queue->tasks[current_uncommit_tail] = task;
    atomic_store(&queue->uncommit_tail, atomic_load(&queue->tail));

    const static uint64_t task_added = 1;
    write(queue->efd, &task_added, sizeof(uint64_t));
    return 0;
}

static inline void * 
__pop_task(task_queue_t *queue, size_t pos) {
    return queue->tasks[pos];
}

static inline void * 
__get_task(thrdpool_t *thrd_pool) {
    void* task = NULL;
    size_t current_head;
    size_t next_head;
    uint64_t read_res;
    do {
        current_head = atomic_load(&thrd_pool->task_queue->head);
        if (current_head == atomic_load(&thrd_pool->task_queue->uncommit_tail)) {
            read(thrd_pool->task_queue->efd, &read_res, sizeof(uint64_t));
        }
        if (atomic_load(&thrd_pool->quit)) {
            return NULL;
        }
        task = __pop_task(thrd_pool->task_queue, current_head);
        next_head = (current_head + 1) & MASK;
    } while(!atomic_compare_exchange_weak(&thrd_pool->task_queue->head, &current_head, next_head));
    if (task) {
        read(thrd_pool->task_queue->efd, &read_res, sizeof(uint64_t));
    }
    return task;
}

static void
__taskqueue_destroy(task_queue_t *queue) {
    task_t *task;
    for(size_t index = 0; index != TASKS_RING_BUFFER_size; index++) {
        free(__pop_task(queue, index));
    }
    if (queue->efd != -1) {
        close(queue->efd);
    }
    free(queue);
}

static void *
__thrdpool_worker(void *arg) {
    thrdpool_t *pool = (thrdpool_t*) arg;
    task_t *task;
    void *ctx;

    while (atomic_load(&pool->quit) == 0) {
        task = (task_t*)__get_task(pool);
        if (!task) break;
        handler_pt func = task->func;
        ctx = task->arg;
        free(task);
        func(ctx);
    }

    return NULL;
}

static void 
__threads_terminate(thrdpool_t * pool) {
    atomic_store(&pool->quit, 1);
    __nonblock(pool->task_queue);
    int i;
    for (i=0; i<pool->thrd_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    free(pool->threads);
}

static int 
__threads_create(thrdpool_t *pool, size_t thrd_count) {
    pthread_attr_t attr;
	int ret;

    if ((ret = pthread_attr_init(&attr)) != 0) {
        goto RETURN;
    }

    if (!(pool->threads = (pthread_t *)malloc(sizeof(pthread_t) * thrd_count))) {
        ret = -1;
        goto DESTORY_ATTR;
    }

    int i = 0;
    for (; i < thrd_count; i++) {
        if (pthread_create(&pool->threads[i], &attr, __thrdpool_worker, pool) != 0) {
            break;
        }
    }
    if (i != thrd_count) {
        ret = -1;
        goto TERMINATE_THRDS;
    }

    pool->thrd_count = thrd_count;
    return 0;

TERMINATE_THRDS:
    __threads_terminate(pool);
DESTORY_ATTR:
    pthread_attr_destroy(&attr);
RETURN:
    return ret;
}

void
thrdpool_terminate(thrdpool_t * pool) {
    atomic_store(&pool->quit, 1);
    __nonblock(pool->task_queue);
}

thrdpool_t *
thrdpool_create(int thrd_count) {
    thrdpool_t *pool;

    pool = (thrdpool_t *) malloc(sizeof(*pool));
    if (!pool) {
        goto RETURN;
    }

    task_queue_t *queue = __taskqueue_create();
    if (!queue) {
        goto FREE_POOL;
    }
    pool->task_queue = queue;
    atomic_init(&pool->quit, 0);
    if (__threads_create(pool, thrd_count) != 0) {
        goto FREE_QUEUE;
    }
    return pool;

FREE_QUEUE:
    __taskqueue_destroy(pool->task_queue);
FREE_POOL:
    free(pool);
RETURN:
    return NULL;
}

int
thrdpool_post(thrdpool_t *pool, handler_pt func, void *arg) {
    if (atomic_load(&pool->quit) == 1) {
        return -1;
    }

    task_t *task = malloc(sizeof(task_t));
    if (!task) return -1;
    task->func = func;
    task->arg = arg;
    if (__add_task(pool->task_queue, task) == -1) {
        usleep(1);
    }

    return 0;
}

void
thrdpool_waitdone(thrdpool_t *pool) {
    int i;
    for (i=0; i<pool->thrd_count; i++) {
        pthread_join(pool->threads[i], NULL);
    }
    __taskqueue_destroy(pool->task_queue);
    free(pool->threads);
    free(pool);
}