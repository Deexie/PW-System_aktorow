#include <stdlib.h>
#include "queue.h"
#include <string.h>

#define QUEUE_CAP 4

int change_capacity(queue_t *queue, size_t new_size) {
    size_t old_size = queue->size;
    size_t old_capacity = queue->capacity;

    queue->q = realloc(queue->q, new_size * sizeof(void *));
    if (queue->q == NULL)
        return -1;

    if (queue->end < queue->begin || old_capacity == old_size) {
        memcpy(queue->q + queue->capacity, queue->q, (queue->end) * sizeof(void *));
        queue->end += queue->capacity;
    }

    queue->capacity = new_size;

    return 0;
}

int push(queue_t *queue, void *el) {
    if (queue->size == queue->capacity) {
        if (change_capacity(queue, 2 * queue->capacity) == -1)
            return -1;
    }

    queue->q[queue->end] = el;
    ++queue->end;
    queue->end %= queue->capacity;
    ++queue->size;

    return 0;
}

void *pop(queue_t *queue) {
    void *el = queue->q[queue->begin];
    ++queue->begin;
    queue->begin %= queue->capacity;
    --queue->size;

    return el;
}

size_t get_size(queue_t *queue) {
    return queue->size;
}

bool empty(queue_t *queue) {
    return queue->size == 0;
}

int create_queue(queue_t *queue) {
    queue->q = malloc(QUEUE_CAP * sizeof(void *));
    if (queue->q == NULL)
        return -1;

    queue->begin = 0;
    queue->end = 0;
    queue->capacity = QUEUE_CAP;
    queue->size = 0;

    return 0;
}

void delete_queue(queue_t *queue) {
    free(queue->q);
}