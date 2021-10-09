#include "cacti.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#define MSG_MEET_CHILD 1
#define MSG_COMPUTE 2
#define MSG_GETDATA 3

typedef struct mat_cell {
    int sleep_time;
    long long value;
} matrix_cell_t;

typedef struct row {
    long long sum;
    size_t row;
} row_t;

typedef struct mat mat_t;

typedef void (*read_t)(mat_t *matrix, size_t row);

struct mat {
    size_t k, n;
    size_t column;
    matrix_cell_t *matrix;
    row_t *row_sums;
    read_t read_val;
};

typedef struct state {
    actor_id_t father_id;
    actor_id_t my_id;
    actor_id_t child_id;
    mat_t matrix;
} state_t;

void read_and_sum(mat_t *matrix, size_t row);
void hello(void **stateptr, __attribute__((unused))size_t nbytes, void *data);
void hello_first_actor(__attribute__((unused))void **stateptr, __attribute__((unused))size_t nbytes,
        __attribute__((unused))void *data);
void get_data(void **stateptr, __attribute__((unused))size_t nbytes, void *data);
void meet_your_child(void **stateptr, size_t nbytes, void *data);
void compute(void **stateptr, __attribute__((unused))size_t nbytes, void *data);

act_t prompts[3] = {hello, meet_your_child, compute};
act_t prompts_first_actor[4] = {hello_first_actor, meet_your_child, compute, get_data};

role_t role = {.prompts = prompts, .nprompts = 3};

void read_and_sum(mat_t *matrix, size_t row) {
    size_t index = row * matrix->n + matrix->column;
    usleep(matrix->matrix[index].sleep_time * 1000);
    matrix->row_sums[row].sum += matrix->matrix[index].value;
}

void hello(void **stateptr, __attribute__((unused))size_t nbytes, void *data) {
    *stateptr = malloc(sizeof(state_t));
    ((state_t *)(*stateptr))->father_id = (actor_id_t)data;
    ((state_t *)(*stateptr))->my_id = actor_id_self();

    send_message(((state_t *)(*stateptr))->father_id,
            (message_t){.message_type = MSG_MEET_CHILD,
                             .nbytes = sizeof(state_t *), .data = *stateptr});
}

void hello_first_actor(__attribute__((unused))void **stateptr, __attribute__((unused))size_t nbytes,
        __attribute__((unused))void *data) {}

void get_data(void **stateptr, __attribute__((unused))size_t nbytes, void *data) {
    *stateptr = malloc(sizeof(state_t));
    ((state_t *)(*stateptr))->my_id = actor_id_self();
    ((state_t *)(*stateptr))->father_id = ((state_t *)(*stateptr))->my_id;
    ((state_t *)(*stateptr))->matrix = *((mat_t *)data);

    if (((state_t *)(*stateptr))->matrix.column + 1 < ((state_t *)(*stateptr))->matrix.n) {
        send_message(((state_t *)(*stateptr))->my_id,
                (message_t){.message_type = MSG_SPAWN,
                            .nbytes = sizeof(role_t), .data = &role});
    }
    else {
        send_message(((state_t *)(*stateptr))->my_id,
                (message_t){.message_type = MSG_COMPUTE, nbytes = sizeof(row_t),
                .data = &((state_t *)(*stateptr))->matrix.row_sums[0]});
    }
}

void meet_your_child(void **stateptr, __attribute__((unused))size_t nbytes, void *data) {
    ((state_t *)(*stateptr))->child_id = ((state_t *)data)->my_id;
    ((state_t *)data)->father_id = ((state_t *)(*stateptr))->father_id;
    ((state_t *)data)->matrix.read_val = ((state_t *)(*stateptr))->matrix.read_val;
    ((state_t *)data)->matrix.matrix = ((state_t *)(*stateptr))->matrix.matrix;
    ((state_t *)data)->matrix.n = ((state_t *)(*stateptr))->matrix.n;
    ((state_t *)data)->matrix.k = ((state_t *)(*stateptr))->matrix.k;
    ((state_t *)data)->matrix.column = ((state_t *)(*stateptr))->matrix.column + 1;
    ((state_t *)data)->matrix.row_sums = ((state_t *)(*stateptr))->matrix.row_sums;

    if (((state_t *)data)->matrix.column + 1 < ((state_t *)data)->matrix.n) {
        send_message(((state_t *)(*stateptr))->child_id,
                (message_t){.message_type = MSG_SPAWN, .nbytes = sizeof(role_t),
                            .data = &role});
    }
    else {
        send_message(((state_t *)(*stateptr))->father_id,
                     (message_t){.message_type = MSG_COMPUTE, .nbytes = sizeof(row_t),
                                 .data = &((state_t *)(*stateptr))->matrix.row_sums[0]});
    }
}

void compute(void **stateptr, __attribute__((unused))size_t nbytes, void *data) {
    size_t row = ((row_t *)data)->row;
    ((state_t *)(*stateptr))->matrix.read_val(&((state_t *)(*stateptr))->matrix, row);

    // If current actor counts values in the last column.
    if (((state_t *)(*stateptr))->matrix.column + 1 == ((state_t *)(*stateptr))->matrix.n) {
        printf("%lld\n", ((state_t *)(*stateptr))->matrix.row_sums[row].sum);
    }
    else {
        send_message(((state_t *)(*stateptr))->child_id,
                     (message_t){.message_type = MSG_COMPUTE, .nbytes = sizeof(row_t),
                             .data = &((state_t *)(*stateptr))->matrix.row_sums[row]});
    }

    // If current actor did all his work.
    if (row + 1 == ((state_t *)(*stateptr))->matrix.k) {
        size_t my_id = ((state_t *)(*stateptr))->my_id;
        free(*stateptr);
        send_message(my_id, (message_t){.message_type = MSG_GODIE, .nbytes = 0,
                                        .data = NULL});
    }
    // If current actor is the first actor.
    else if (((state_t *)(*stateptr))->my_id == ((state_t *)(*stateptr))->father_id) {
        send_message(((state_t *)(*stateptr))->my_id,
                     (message_t){.message_type = MSG_COMPUTE, .nbytes = sizeof(row_t),
                             .data = &((state_t *)(*stateptr))->matrix.row_sums[row + 1]});
    }
}

int main(){
    size_t k, n;
    actor_id_t actor;
    scanf("%zu", &k);
    scanf("%zu", &n);

    matrix_cell_t *matrix = malloc(k * n * sizeof(matrix_cell_t));
    for (size_t i = 0; i < k * n; ++i) {
        scanf("%lld", &matrix[i].value);
        scanf("%d", &matrix[i].sleep_time);
    }

    row_t *rows = malloc(k * sizeof(row_t));
    for (size_t i = 0; i < k; ++i) {
        rows[i].sum = 0;
        rows[i].row = i;
    }

    mat_t mat;
    mat.matrix = matrix;
    mat.row_sums = rows;
    mat.n = n;
    mat.k = k;
    mat.column = 0;
    mat.read_val = read_and_sum;

    role_t first_actor_role;
    first_actor_role.prompts = prompts_first_actor;
    first_actor_role.nprompts = 4;
    if (actor_system_create(&actor, &first_actor_role) != 0)
        exit(1);

    send_message(actor, (message_t){.message_type = MSG_GETDATA, .nbytes = sizeof(mat_t),
                                    .data = &mat});

    actor_system_join(actor);

    free(rows);
    free(matrix);
}
