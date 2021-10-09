#ifndef CACTI_QUEUE_H
#define CACTI_QUEUE_H

#include <stddef.h>
#include <stdbool.h>

typedef struct queue {
    void **q;
    size_t capacity;
    size_t begin;
    size_t end;
    size_t size;
} queue_t;

int push(queue_t *queue, void *el);

void *pop(queue_t *queue);

size_t get_size(queue_t *queue);

bool empty(queue_t *queue);

int create_queue(queue_t *queue);

void delete_queue(queue_t *queue);

#endif //CACTI_QUEUE_H
