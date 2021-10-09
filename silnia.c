#include "cacti.h"
#include <stdlib.h>
#include <stdio.h>

#define MSG_MEET_CHILD 1
#define MSG_COMPUTE 2
#define MSG_KYS 3
#define MSG_GETDATA 4

typedef long long factorial_type;

typedef struct fact {
    factorial_type factor;
    factorial_type factorial;
    factorial_type num;
} factorial_t;

typedef struct state {
    factorial_t f;
    actor_id_t father_id;
    actor_id_t my_id;
    actor_id_t child_id;
} state_t;

void hello(void **stateptr, size_t nbytes, void *data);
void hello_first_actor( __attribute__((unused))void **stateptr,  __attribute__((unused))size_t nbytes,
        __attribute__((unused))void *data);
void get_data(void **stateptr, size_t nbytes, void *data);
void meet_your_child(void **stateptr, size_t nbytes, void *data);
void compute(void **stateptr, size_t nbytes, void *data);
void kill_yourself(void **stateptr, size_t nbytes, void *data);

act_t prompts[4] = {hello, meet_your_child, compute, kill_yourself};
act_t prompts_first_actor[5] = {hello_first_actor, meet_your_child, compute, kill_yourself, get_data};

role_t role = {.prompts = prompts, .nprompts = 4};

void hello(void **stateptr, __attribute__((unused))size_t nbytes, void *data) {
    *stateptr = malloc(sizeof(state_t));
    ((state_t *)(*stateptr))->father_id = (actor_id_t)data;
    ((state_t *)(*stateptr))->my_id = actor_id_self();

    send_message(((state_t *)(*stateptr))->father_id,
            (message_t){.message_type = MSG_MEET_CHILD, .nbytes = sizeof(actor_id_t),
                        .data = &((state_t *)(*stateptr))->my_id});
}

void hello_first_actor( __attribute__((unused))void **stateptr,
        __attribute__((unused))size_t nbytes,  __attribute__((unused))void *data) {}

void get_data(void **stateptr, __attribute__((unused))size_t nbytes, void *data) {
    *stateptr = malloc(sizeof(state_t));
    ((state_t *)(*stateptr))->f.num = *((factorial_type *)data);
    ((state_t *)(*stateptr))->f.factor = 1;
    ((state_t *)(*stateptr))->f.factorial = 1;
    ((state_t *)(*stateptr))->my_id = actor_id_self();
    ((state_t *)(*stateptr))->father_id = ((state_t *)(*stateptr))->my_id;

    send_message(((state_t *)(*stateptr))->my_id,
            (message_t){.message_type = MSG_SPAWN, .nbytes = sizeof(role_t),
                        .data = &role});
}

void meet_your_child(void **stateptr, __attribute__((unused))size_t nbytes, void *data) {
    ((state_t *)(*stateptr))->child_id = *((actor_id_t *)data);

    send_message(((state_t *)(*stateptr))->child_id,
                 (message_t){.message_type = MSG_COMPUTE, .nbytes = sizeof(factorial_t),
                             .data = &((state_t *)(*stateptr))->f});
}

void kill_yourself(void **stateptr, __attribute__((unused))size_t nbytes,
        __attribute__((unused))void *data) {
    if (((state_t *)(*stateptr))->my_id != ((state_t *)(*stateptr))->father_id) {
        send_message(((state_t *) (*stateptr))->father_id,
                     (message_t) {.message_type = MSG_KYS, .nbytes = 0, .data = NULL});
    }

    actor_id_t my_id = ((state_t *) (*stateptr))->my_id;

    free(*stateptr);
    send_message(my_id,(message_t) {.message_type = MSG_GODIE, .nbytes = 0, .data = NULL});
}

void compute(void **stateptr, __attribute__((unused))size_t nbytes, void *data) {
    ((state_t *)(*stateptr))->f.num = ((factorial_t *)data)->num;
    ((state_t *)(*stateptr))->f.factor = ((factorial_t *)data)->factor + 1;
    ((state_t *)(*stateptr))->f.factorial =
            ((factorial_t *)data)->factorial * ((state_t *)(*stateptr))->f.factor;

    if (((state_t *)(*stateptr))->f.num == ((state_t *)(*stateptr))->f.factor) {
        printf("%lld", ((state_t *)(*stateptr))->f.factorial);
        send_message(((state_t *)(*stateptr))->my_id,
                     (message_t){.message_type = MSG_KYS, .nbytes = 0, .data = NULL});
    }
    else {
        send_message(((state_t *)(*stateptr))->my_id,
                (message_t){.message_type = MSG_SPAWN, .nbytes = sizeof(role_t),
                                                                  .data = &role});
    }
}

int main(){
    actor_id_t actor;
    factorial_type num;
    scanf("%lld", &num);

    if (num < 0)
        exit(1);

    if (num == 0 || num == 1) {
        printf("%d", 1);
    }
    else {
        role_t first_actor_role;
        first_actor_role.prompts = prompts_first_actor;
        first_actor_role.nprompts = 5;
        if (actor_system_create(&actor, &first_actor_role) != 0)
            exit(1);

        send_message(actor, (message_t){.message_type = MSG_GETDATA,
                                        .nbytes = sizeof(factorial_type), .data = &num});

        actor_system_join(actor);
    }
}
