//
// Created by leauy on 26.11.21.
//
#include "lock.h"
#include <tm.h>
#include <pthread.h>
#include <stdatomic.h>
//#include <stdio.h>
#include <string.h>
#include <stdlib.h>
//#include <signal.h>

//TODO change to atomic

// #define printdebug(x)
//#define printdebug(x) printf x


struct batch{
    atomic_size_t counter;
    atomic_size_t remaining;
    atomic_size_t size;
    atomic_size_t last;
    atomic_flag flag;
    struct lock_t lock;
    size_t* finishedTransactions;
    atomic_size_t finishedCounter;
};


struct dualMem {
    struct lock_t* word_lock;
    atomic_size_t hasCommits;
    atomic_size_t remove;
    size_t size;
    size_t align;
    //USE Linked List because too lazy to free array after each del ... :)
    struct dualMem* NEXT;
    struct dualMem* PREV;
    //access set
    atomic_size_t* accessed;
    atomic_size_t* totalAccesses;
    atomic_size_t* wasWritten;
    atomic_size_t* writeEpoch;
    atomic_size_t belongsTo;
    atomic_bool* spinLock;

    void* writeCopy;
    void* validCopy;
};

struct region{
    void* start;
    size_t size;
    size_t align;
    struct dualMem* allocs;
    struct batch* batcher;
    struct dualMem dualMem;
    struct dualMem** memoryRegions;

    atomic_size_t nextROSlot;
    atomic_size_t nextRWSlot;
    atomic_short nextSegment;
    atomic_bool printer;
};

struct batch* init(size_t threadCount);

size_t get_epoch(struct batch *self);

void enter(struct batch *self);

bool leave(struct batch *self);

bool read_word(struct dualMem* dualMem, size_t index, void* target, size_t offset, size_t transactionId);

bool write_word(struct dualMem* dualMem, size_t index, void const* source, size_t offset,  size_t transactionId);

bool read(struct batch *self, const void* source, size_t size, void* target);

bool write(struct batch *self, const void* source, size_t size, void* target);

shared_t alloc(struct batch *self, size_t size, void* target);

bool commit(struct batch *self, struct dualMem** dualMem);
void cleanup_read(struct region* region, struct batch* self, struct dualMem** dualMem);
void cleanup_write(struct region* region, struct batch* self, struct dualMem** dualMem);
void cleanup(struct region* region, struct batch* self, struct dualMem* dualMem);

void spin_lock(atomic_bool* lock);
void spin_unlock(atomic_bool* lock);
