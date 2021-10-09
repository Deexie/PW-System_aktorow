#include <pthread.h>
#include <semaphore.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>


#include "cacti.h"
#include "queue.h"

#define DATA_SIZE 32

typedef struct state_ptr_type
{
    void *state;
} state_ptr_t;

typedef struct actor_properties
{
    bool is_dead;
    role_t role;
    queue_t message_queue;
    state_ptr_t *state;
    pthread_mutex_t mutex;
} actor_properties_t;

typedef struct system
{
    size_t dead_count;
    size_t actor_count;
    size_t actors_capacity;
    actor_properties_t *actors;
    pthread_mutex_t system_state_mutex;

    queue_t actor_queue;
    pthread_mutex_t actor_queue_mutex;
    
    pthread_cond_t cond;
    pthread_t threads[POOL_SIZE];
    bool all_work_done; // True if all actors died.

    struct sigaction old_action;
    bool interrupted;   // True if SIGINT was send.
    bool all_threads_returned;
    size_t returned_threads; // Counter.

    pthread_rwlock_t realloc_safety;
} actor_system_t;

actor_system_t *actor_system;
_Thread_local actor_id_t thread_actor;


// Function is not thread-safe - system_state_mutex must be locked before entering
// the function and unlocked after.
int create_actor(actor_id_t id, role_t *const role);

int create_thread_pool();

// New SIGINT action.
void block_system(int sig);

// Changes SIGINT action to [block_system].
int set_signal_operation();

// Threads execution.
void *worker (void *data);

// Pops message form [actor]'s queue and executes it.
void work_with_actor(actor_id_t actor);

// MSG_GODIE execution. [realloc_safety] and actor's mutex must be locked while entering
// the function.
void go_die(actor_id_t actor);

// MSG_SPAWN execution.
void spawn(message_t message, actor_id_t actor);

// Checks if actor with given id exists.
bool actor_exists(actor_id_t actor);

// Changes SIGINT action to default.
int reset_signal_operation();

// Destroys [created_threads_count] threads from threads' pool.
void destroy_thread_pool(size_t created_threads_count);

void destroy_actors();

void destroy_actor_system();


int create_actor(actor_id_t id, role_t *const role) {
    if (actor_system->actor_count == CAST_LIMIT)
        return -2;

    // If furthest part of actor creation failed, current [actor_system->actors] capacity would
    // not be regained.
    ++actor_system->actor_count;
    if (actor_system->actor_count > actor_system->actors_capacity) {
        // [actor_system->actors] cannot be read during capacity change.
        if (pthread_rwlock_wrlock(&actor_system->realloc_safety) != 0)
            exit(1);

        actor_system->actors = realloc(actor_system->actors,
                2 * actor_system->actors_capacity * sizeof(actor_properties_t));
        if (actor_system->actors == NULL)
            exit(1);

        actor_system->actors_capacity *= 2;

        if (pthread_rwlock_unlock(&actor_system->realloc_safety) != 0)
            exit(1);
    }

    actor_system->actors[id].role.nprompts = role->nprompts;
    actor_system->actors[id].role.prompts = role->prompts;
    actor_system->actors[id].is_dead = false;
    actor_system->actors[id].state = malloc(sizeof(state_ptr_t));
    if (actor_system->actors[id].state == NULL)
        goto STATE_ERROR;
    actor_system->actors[id].state->state = NULL;

    if (create_queue(&actor_system->actors[id].message_queue) != 0)
        goto ALLOC_ERROR;

    if (pthread_mutex_init(&actor_system->actors[id].mutex, 0) != 0)
        goto MUTEX_ERROR;

    return 0;

    MUTEX_ERROR:
    delete_queue(&actor_system->actors[id].message_queue);
    ALLOC_ERROR:
    free(actor_system->actors[id].state);
    STATE_ERROR:
    --actor_system->actor_count;
    return -1;
}

int create_thread_pool() {
    for (size_t i = 0; i < POOL_SIZE; ++i) {
        if (pthread_create(&actor_system->threads[i], NULL, worker, NULL) != 0) {
            destroy_thread_pool(i);
            return -1;
        }
    }

    return 0;
}

void block_system(__attribute__((unused))int sig) {
    actor_system->interrupted = true;

    if (actor_system->all_threads_returned) {
        destroy_actor_system();
        raise(SIGINT);
    } else {
        pthread_cond_broadcast(&actor_system->cond);
    }
}

int set_signal_operation() {
    struct sigaction new_action;
    sigset_t block_mask;

    sigemptyset(&block_mask);

    new_action.sa_handler = block_system;
    new_action.sa_mask = block_mask;
    new_action.sa_flags = SA_RESTART;

    if (sigaction(SIGINT, &new_action, &actor_system->old_action) != 0)
        return -1;

    return 0;
}

void *worker (__attribute__((unused))void *data) {
    actor_id_t *current_actor_ptr, current_actor;

    while (actor_system->actor_count == 0 || actor_system->actor_count != actor_system->dead_count) {
        if (pthread_mutex_lock(&actor_system->actor_queue_mutex) != 0)
            exit(1);

        while (!actor_system->interrupted && !actor_system->all_work_done &&
               empty(&actor_system->actor_queue)) {
            if (pthread_cond_wait(&actor_system->cond, &actor_system->actor_queue_mutex) != 0)
                exit(1);
        }

        // If all actors are dead and no more messages stayed at any actor's queue.
        if ((actor_system->all_work_done || actor_system->interrupted) &&
            empty(&actor_system->actor_queue)) {
            if (pthread_mutex_unlock(&actor_system->actor_queue_mutex) != 0)
                exit(1);

            break;
        }

        current_actor_ptr = (actor_id_t *)pop(&actor_system->actor_queue);
        current_actor = *current_actor_ptr;
        free(current_actor_ptr);

        if (pthread_mutex_unlock(&actor_system->actor_queue_mutex) != 0)
            exit(1);

        work_with_actor(current_actor);
    }

    if (pthread_cond_signal(&actor_system->cond) != 0)
        exit(1);

    if (pthread_mutex_lock(&actor_system->system_state_mutex) != 0)
        exit(1);

    actor_system->returned_threads++;
    actor_system->all_threads_returned = actor_system->returned_threads == POOL_SIZE;
    // If all other threads returned and actors' system was interrupted by SIGINT,
    // program has to be killed.
    if (actor_system->all_threads_returned && actor_system->interrupted) {
        if (pthread_mutex_unlock(&actor_system->system_state_mutex) != 0)
            exit(1);
        reset_signal_operation();
        raise(SIGINT);
    }

    if (pthread_mutex_unlock(&actor_system->system_state_mutex) != 0)
        exit(1);

    return NULL;
}

void work_with_actor(actor_id_t actor) {
    thread_actor = actor;

    if (pthread_rwlock_rdlock(&actor_system->realloc_safety) != 0)
        exit(1);

    if (pthread_mutex_lock(&actor_system->actors[actor].mutex) != 0)
        exit(1);

    message_t *current_message = (message_t *)pop(&actor_system->actors[actor].message_queue);
    bool actor_has_more_messages = !empty(&actor_system->actors[actor].message_queue);

    // It needs to be done with actor's mutex locked, because no more messages may be received.
    if (current_message->message_type == MSG_GODIE) {
        go_die(actor);
        free(current_message);
        return;
    }

    if (pthread_mutex_unlock(&actor_system->actors[actor].mutex) != 0)
        exit(1);

    if (current_message->message_type == MSG_SPAWN) {
        if (pthread_rwlock_unlock(&actor_system->realloc_safety) != 0)
            exit(1);

        spawn(*current_message, actor);
    }
    else {
        // Given message type is not defined for this actor.
        if ((size_t)current_message->message_type >= actor_system->actors[actor].role.nprompts) {
            free(current_message);
            return;
        }

        size_t nbytes = current_message->nbytes;
        void *data = current_message->data;

        act_t service = actor_system->actors[actor].role.prompts[current_message->message_type];

        state_ptr_t *state_ptr = actor_system->actors[actor].state;

        if (pthread_rwlock_unlock(&actor_system->realloc_safety) != 0)
            exit(1);

        service(&state_ptr->state, nbytes, data);
    }

    if (actor_has_more_messages) {
        if (pthread_mutex_lock(&actor_system->actor_queue_mutex) != 0)
            exit(1);

        actor_id_t *actor_id = malloc(sizeof(actor_id_t));
        *actor_id = actor;
        if (push(&actor_system->actor_queue, actor_id) != 0)
            exit(1);

        if (pthread_mutex_unlock(&actor_system->actor_queue_mutex) != 0)
            exit(1);
    }

    free(current_message);
}

void go_die(actor_id_t actor) {
    bool was_dead = actor_system->actors[actor].is_dead;

    actor_system->actors[actor].is_dead = true;

    if (pthread_mutex_unlock(&actor_system->actors[actor].mutex) != 0)
        exit(1);

    if (pthread_rwlock_unlock(&actor_system->realloc_safety) != 0)
        exit(1);

    if (was_dead)
        return;

    if (pthread_mutex_lock(&actor_system->system_state_mutex) != 0)
        exit(1);

    ++actor_system->dead_count;
    actor_system->all_work_done = actor_system->dead_count == actor_system->actor_count;

    if (pthread_mutex_unlock(&actor_system->system_state_mutex) != 0)
        exit(1);
}

void spawn(message_t message, actor_id_t actor) {
    if (actor_system->interrupted)
        return;

    if (pthread_mutex_lock(&actor_system->system_state_mutex) != 0)
        exit(1);

    actor_id_t new_actor_id = actor_system->actor_count;

    int err;
    if ((err = create_actor(new_actor_id, message.data)) == -1)
        exit(1);
    // If actor cannot be created due to CAST_LIMIT, nothing happens.
    if (err == -2) {
        if (pthread_mutex_unlock(&actor_system->system_state_mutex) != 0)
            exit(1);

        return;
    }

    if (pthread_mutex_unlock(&actor_system->system_state_mutex) != 0)
        exit(1);

    // New actor should receive hello message.
    if (send_message(new_actor_id, (message_t){.message_type = MSG_HELLO, .data = (void *)actor,
            .nbytes = sizeof(actor_id_t)}) != 0)
        exit(1);
}

bool actor_exists(actor_id_t actor) {
    if (actor < 0)
        return false;
    if (pthread_mutex_lock(&actor_system->system_state_mutex) != 0)
        exit(1);

    bool exists = actor < (actor_id_t)actor_system->actor_count;

    if (pthread_mutex_unlock(&actor_system->system_state_mutex) != 0)
        exit(1);

    return exists;
}

int reset_signal_operation() {
    if (sigaction(SIGINT, &actor_system->old_action, NULL) != 0)
        return -1;

    return 0;
}

void destroy_thread_pool(size_t created_threads_count) {
    actor_system->all_work_done = true;

    pthread_cond_broadcast(&actor_system->cond);

    for (size_t i = 0; i < created_threads_count; ++i) {
        if (pthread_join(actor_system->threads[i], NULL) != 0)
            exit(1);
    }
}

void destroy_actors() {
    for (size_t i = 0; i < actor_system->actor_count; ++i) {
        pthread_mutex_destroy(&actor_system->actors[i].mutex);
        delete_queue(&actor_system->actors[i].message_queue);
        free(actor_system->actors[i].state);
        // Deleting state and role is user's responsibility.
    }

    free(actor_system->actors);
}

void destroy_actor_system() {
    if (pthread_cond_destroy(&actor_system->cond) != 0)
        exit(1);
    if (pthread_mutex_destroy(&actor_system->system_state_mutex) != 0)
        exit(1);
    if (pthread_mutex_destroy(&actor_system->actor_queue_mutex) != 0)
        exit(1);
    if (pthread_rwlock_destroy(&actor_system->realloc_safety) != 0)
        exit(1);

    delete_queue(&actor_system->actor_queue);
    destroy_actors();
    reset_signal_operation();

    free(actor_system);
    actor_system = NULL;
}

actor_id_t actor_id_self() {
    return thread_actor;
}

int actor_system_create(actor_id_t *actor, role_t *const role) {
    if (actor_system != NULL)
        return -1;

    actor_system = malloc(sizeof(actor_system_t));
    if (actor_system == NULL)
        return -1;

    actor_system->actors = malloc(DATA_SIZE * sizeof(actor_properties_t));
    if (actor_system->actors == NULL)
        goto ACTORS_ERROR;

    actor_system->dead_count = 0;
    actor_system->actor_count = 0;
    actor_system->actors_capacity = DATA_SIZE;
    actor_system->all_work_done = false;
    actor_system->interrupted = false;
    actor_system->all_threads_returned = false;
    actor_system->returned_threads = 0;

    if (create_queue(&actor_system->actor_queue) != 0)
        goto ACTOR_QUEUE_ERROR;

    if (pthread_mutex_init(&actor_system->actor_queue_mutex, 0) != 0)
        goto MUTEX_ERROR;

    if (pthread_mutex_init(&actor_system->system_state_mutex, 0) != 0)
        goto STATE_MUTEX_ERROR;

    if (pthread_cond_init(&actor_system->cond, 0) != 0)
        goto COND_ERROR;

    if (create_thread_pool() != 0)
        goto THREADS_ERROR;

    if (set_signal_operation() != 0)
        goto SET_SIGNAL_ERROR;

    if (pthread_rwlock_init(&actor_system->realloc_safety, NULL) != 0)
        goto RW_LOCK_ERROR;

    *actor = 0;
    if (create_actor(0, role) != 0)
        goto NEW_ACTOR_ERROR;

    send_message(*actor, (message_t){.message_type = MSG_HELLO,
            .nbytes = 0, .data = NULL});

    return 0;

    NEW_ACTOR_ERROR:
    if (pthread_rwlock_destroy(&actor_system->realloc_safety) != 0)
        exit(1);
    RW_LOCK_ERROR:
        reset_signal_operation();
    SET_SIGNAL_ERROR:
        destroy_thread_pool(POOL_SIZE);
    THREADS_ERROR:
        if (pthread_cond_destroy(&actor_system->cond) != 0)
            exit(1);
    COND_ERROR:
        if (pthread_mutex_destroy(&actor_system->system_state_mutex) != 0)
            exit(1);
    STATE_MUTEX_ERROR:
        if (pthread_mutex_destroy(&actor_system->actor_queue_mutex) != 0)
            exit(1);
    MUTEX_ERROR:
        delete_queue(&actor_system->actor_queue);
    ACTOR_QUEUE_ERROR:
        free(actor_system->actors);
        actor_system->actors = NULL;
    ACTORS_ERROR:
        free(actor_system);
        actor_system = NULL;
        return -1;
}

void actor_system_join(actor_id_t actor) {
    if (actor_system == NULL || !actor_exists(actor))
        return;

    pthread_t threads[POOL_SIZE];
    memcpy(threads, actor_system->threads, POOL_SIZE * sizeof(pthread_t));
    for (int i = 0; i < POOL_SIZE; ++i) {
        if (pthread_join(threads[i], NULL) != 0)
            exit(1);
    }

    if (!actor_system->interrupted)
        destroy_actor_system();
}

int send_message(actor_id_t actor, message_t message) {
    if (actor_system == NULL || !actor_exists(actor))
        return -2;

    // Create message.
    message_t *new_mess = malloc(sizeof(message_t));
    if (new_mess == NULL)
        exit(1);

    new_mess->message_type = message.message_type;
    new_mess->data = message.data;
    new_mess->nbytes = message.nbytes;

    // Accessing actors cannot be done during reallocation.
    if (pthread_rwlock_rdlock(&actor_system->realloc_safety) != 0)
        exit(1);

    // Check if given actor is dead or its message queue is full.
    if (pthread_mutex_lock(&actor_system->actors[actor].mutex) != 0)
        exit(1);

    size_t q_size = get_size(&actor_system->actors[actor].message_queue);

    bool dead = actor_system->interrupted || actor_system->actors[actor].is_dead;
    if (dead || q_size == ACTOR_QUEUE_LIMIT) {
        if (pthread_mutex_unlock(&actor_system->actors[actor].mutex) != 0)
            exit(1);
        if (pthread_rwlock_unlock(&actor_system->realloc_safety) != 0)
            exit(1);
        free(new_mess);
        if (dead)
            return -1;
        else
            return -3;
    }

    if (push(&actor_system->actors[actor].message_queue, new_mess) != 0) {
        if (pthread_mutex_unlock(&actor_system->actors[actor].mutex) != 0)
            exit(1);
        if (pthread_rwlock_unlock(&actor_system->realloc_safety) != 0)
            exit(1);
        free(new_mess);
        return -1;
    }

    if (pthread_mutex_unlock(&actor_system->actors[actor].mutex) != 0)
        exit(1);

    if (pthread_rwlock_unlock(&actor_system->realloc_safety) != 0)
        exit(1);

    // If queue was empty, actor information needs to be pushed into actors queue.
    if (q_size == 0) {
        if (pthread_mutex_lock(&actor_system->actor_queue_mutex) != 0)
            exit(1);

        actor_id_t *actor_id = malloc(sizeof(actor_id_t));
        *actor_id = actor;
        if (push(&actor_system->actor_queue, actor_id) != 0)
            exit(1);

        if (pthread_mutex_unlock(&actor_system->actor_queue_mutex) != 0)
            exit(1);

        if (pthread_cond_signal(&actor_system->cond) != 0)
            exit(1);
    }

    return 0;
}
