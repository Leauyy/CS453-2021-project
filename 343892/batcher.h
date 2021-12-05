//
// Created by leauy on 26.11.21.
//
#include "lock.h"
#include <tm.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>

//TODO change to atomic

struct threads{
    pthread_mutex_t lock;
};


struct batch{
    atomic_size_t counter;
    atomic_size_t remaining;
    atomic_size_t size;
    atomic_size_t last;
    atomic_flag flag;
    struct lock_t lock;
    struct threads* threads;
};

struct dualMem {
    atomic_int isAValid; //false = A is valid; true = B is valid;
    atomic_int wasWritten;
    //USE Linked List because too lazy to free array after each del ... :)
    struct dualMem* NEXT;
    struct dualMem* PREV;
    //access set
    atomic_size_t* accessed;
    atomic_int totalAccesses;
    void* copyA;
    void* copyB;
};

struct batch* init(size_t threadCount);

size_t get_epoch(struct batch *self);

void enter(struct batch *self);

bool leave(struct batch *self, struct dualMem* dualMem, size_t size, size_t align);

bool read_word(struct dualMem* dualMem, size_t index, size_t source, void* target, size_t align, size_t transactionId);

bool write_word(struct batch* self ,struct dualMem* dualMem, size_t index, void const* source, size_t offset, size_t align, size_t transactionId);

bool read(struct batch *self, const void* source, size_t size, void* target);

bool write(struct batch *self, const void* source, size_t size, void* target);

shared_t alloc(struct batch *self, size_t size, void* target);

//bool free(struct batch *self, void* target);

bool commit(struct dualMem* dualMem, size_t size, size_t align, tx_t tx);
void cleanup_read(struct batch* self, struct dualMem* dualMem,void* target, const void* source, size_t size, size_t reachedIndex, size_t align);
void cleanup_write(struct batch* self, struct dualMem* dualMem,void* target, const void* source, size_t size, size_t reachedIndex, size_t align);
void cleanup(struct batch* self, struct dualMem* dualMem, size_t size, size_t align);

