/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers

//TODO CHECK IF POSIX MUTEXES ARE SYSCALLS

// Internal headers
#include <tm.h>
#include "macros.h"
#include "batcher.h"



/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/



shared_t tm_create(size_t size, size_t align) {
    struct region* region = (struct region*) malloc(sizeof(struct region));

    region->size        = size;
    region->align       = align;
    region->batcher = init(1024);

    size_t words = size/align;

    //allocate first segment STM
    struct dualMem* dualMem= (struct dualMem*) malloc(sizeof(struct dualMem));
    dualMem->validCopy = (void*) malloc(size);
    dualMem->writeCopy = (void*) malloc(size);

    dualMem->accessed = (atomic_size_t*) malloc(words * sizeof(atomic_size_t));
    dualMem->wasWritten = (atomic_size_t*) malloc(words * sizeof(atomic_size_t));
    dualMem->totalAccesses = (atomic_size_t*) malloc(words * sizeof(atomic_size_t));

    //create pointer array to segments
    size_t maxRegions = 1<<16;
    region->memoryRegions = (struct dualMem**) malloc(maxRegions*sizeof(struct dualMem*));

    //dualMem->spinLock = (atomic_bool*) malloc(sizeof(atomic_bool)*words);

    atomic_store(&region->nextSegment,2);
    uintptr_t thisPointer = 1;
    thisPointer = thisPointer<<48;
    region->memoryRegions[1] = dualMem;

    memset(dualMem->validCopy, 0, size);
    memset(dualMem->writeCopy, 0, size);
    memset(dualMem->accessed, 0, size);
    memset(dualMem->wasWritten, 0, size);
    memset(dualMem->totalAccesses, 0, size);
    //memset(dualMem->spinLock, 0, words*sizeof(atomic_bool));
    atomic_store(&dualMem->remove, 0);
    dualMem->word_lock = (struct lock_t*) malloc(sizeof(struct lock_t)*words);

    for (size_t i=0ul; i < words; i++){
        lock_init(dualMem->word_lock+i);
    }


    dualMem->size = size;
    dualMem->align = align;

    region->start = (void*) thisPointer;


    //A is valid
    //dualMem->isAValid = true;
    region->dualMem = *dualMem;
    atomic_store(&region->nextROSlot, 2);
    atomic_store(&region->nextRWSlot, 1);

    dualMem->PREV = NULL;
    dualMem->NEXT = NULL;
    region->allocs = dualMem;

    //TODO check memory alloc

    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t unused(shared)) {
    struct region* reg = (struct region*) shared;
    struct dualMem* dm = reg->allocs;

    while (dm != NULL) {
        struct dualMem* thisMem = dm;
        dm = dm->NEXT;

        size_t words = thisMem->size/thisMem->align;
        free(thisMem->validCopy);
        free(thisMem->writeCopy);
        free(thisMem->accessed);
        free(thisMem->wasWritten);
        free(thisMem->totalAccesses);
        for (size_t i=0ul; i < words; i++){
            lock_cleanup(thisMem->word_lock+i);
        }
        free(thisMem->word_lock);

        //free(thisMem->spinLock);
        free(thisMem);
    }
    free(reg->memoryRegions);
    free(reg->batcher->finishedTransactions);
    free(reg->batcher);
    free(reg);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    struct region* reg = (struct region*) shared;
    return reg->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    return ((struct region*) shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) {
    return ((struct region*) shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) {
    struct region* reg = (struct region*) shared;
    enter(reg->batcher);

    if (is_ro) {
        return atomic_fetch_add(&reg->nextROSlot, 2);
    } else {
        return atomic_fetch_add(&reg->nextRWSlot, 2);
    }
    return invalid_tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t tx) {
    // TODO: tm_end(shared_t, tx_t)

    struct region* reg = (struct region*) shared;
    if (leave(reg->batcher)) {
        size_t index = atomic_fetch_add(&reg->batcher->finishedCounter, 1);
        reg->batcher->finishedTransactions[index] = tx;
//        if (global_printer) {
//            printf("Comitting on epoch %lu\n", get_epoch(reg->batcher));
//        }
        commit(reg->batcher, &reg->allocs);
        cleanup(reg ,reg->batcher, reg->allocs);
    } else {
        size_t index = atomic_fetch_add(&reg->batcher->finishedCounter, 1);
        reg->batcher->finishedTransactions[index] = tx;
        lock_release(&(reg->batcher->lock));
    }
    return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    struct region* reg = (struct region*) shared;

    size_t segment = ((size_t)source)>>48;
    size_t offset = ((size_t)source) & ((1ul<<48)-1);
    struct dualMem* dualMem = (struct dualMem*) reg->memoryRegions[segment];


    size_t align = reg->align;
    size_t numberOfWords = size/align;
    bool res;

    for (size_t i=0; i< numberOfWords; i++) {
        size_t idx = i*align;
        res = read_word(dualMem, idx, target, offset, tx);
        if (!res) {
            //printf("Read abort on Segment %lu Word Index %lu Epoch %lu Transaction %lu\n", segment, offset/dualMem->align, get_epoch(reg->batcher), tx);
            cleanup_read(reg ,reg->batcher, &reg->allocs);
            lock_wake_up(&(reg->batcher->lock));
            return false;
        }
    }
//    if (global_printer) {
//        size_t *content = target;
//        //printf("READ %lu words on epoch %lu || Segment: %lu Word Offset: %lu Word Align %lu Word Index %lu || Content: %lu || Transaction %lu\n", numberOfWords, get_epoch(reg->batcher), segment, offset, dualMem->align, offset/dualMem->align, *content, tx);
//        //printf("READ: Content: %lu Transaction %lu || Epoch %lu\n", *content, tx, get_epoch(reg->batcher));
//    }
    return true;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    struct region *reg = (struct region *) shared;
    size_t align = reg->align;
    size_t numberOfWords = size / align;

    //Get offset and segment. Then obtain memory region
    size_t segment = ((uintptr_t) target) >> 48;
    size_t offset = ((uintptr_t) target) & ((1ul << 48) - 1);
    struct dualMem *dualMem = (struct dualMem *) reg->memoryRegions[segment];


    bool res;
    for (size_t i = 0; i < numberOfWords; i++) {
        res = write_word(dualMem, i, source, offset, tx);
        if (!res) {
            //pass all alocs to cleanup from start
            //printf("Write abort\n");
            cleanup_write(reg, reg->batcher, &reg->allocs);
            lock_wake_up(&(reg->batcher->lock));
            return false;
        }
    }

//    size_t *content = source;
//
//    if (*content ==800) {
//        atomic_store(&global_printer, true);
//    }
//    size_t *actual = (dualMem->writeCopy + offset);
//    if (global_printer) {
//        printf("WRITE %lu words on epoch %lu || Segment: %lu Word Offset: %lu Word Align %lu Word Index %lu || Content: %lu Actual: %lu || Transaction %lu\n",
//               numberOfWords, get_epoch(reg->batcher), segment, offset, dualMem->align, offset / dualMem->align,
//               *content,
//               *actual, tx);
//        printf("WRITE: Content: %lu Transaction %lu || Epoch %lu\n", *content, tx, get_epoch(reg->batcher));
//    }
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t shared, tx_t unused(tx), size_t size, void** target) {
    struct region* reg = (struct region*) shared;
    size_t align = reg->align;
    if (size%align != 0){
        return abort_alloc;
    }

    size_t words = size/align;

    struct dualMem* dualMem= (struct dualMem*) malloc(sizeof(struct dualMem));
    //use one malloc
    dualMem->validCopy = (void*) malloc(size);
    dualMem->writeCopy = (void*) malloc(size);


    dualMem->accessed = (atomic_size_t*) malloc(words * sizeof(atomic_size_t));
    dualMem->wasWritten = (atomic_size_t*) malloc(words * sizeof(atomic_size_t));
    dualMem->totalAccesses = (atomic_size_t*) malloc(words * sizeof(atomic_size_t));
    //dualMem->spinLock = (atomic_bool*) malloc(sizeof(atomic_bool)*words);
    dualMem->word_lock = (struct lock_t*) malloc(sizeof(struct lock_t)*words);

    size_t thisPointer = atomic_fetch_add(&reg->nextSegment,1);
    reg->memoryRegions[thisPointer] = dualMem;
    thisPointer = thisPointer<<48;

    dualMem->align = align;
    dualMem->size = size;

    memset(dualMem->accessed, 0, size);
    memset(dualMem->wasWritten, 0, size);
    memset(dualMem->validCopy, 0, size);
    memset(dualMem->writeCopy, 0, size);
    memset(dualMem->totalAccesses, 0, size);
    //memset(dualMem->spinLock, 0, words*sizeof(atomic_bool));
    atomic_store(&dualMem->remove, 0);
    for (size_t i=0ul; i < words; i++){
        lock_init(dualMem->word_lock+i);
    }

    dualMem->PREV = NULL;
    dualMem->NEXT = reg->allocs;

    if (dualMem->NEXT) dualMem->NEXT->PREV = dualMem;
    reg->allocs = dualMem;

    *target = (void*) thisPointer;
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t shared, tx_t tx, void* target) {
    struct region* reg = (struct region*) shared;
    size_t segment = ((size_t)target)>>48;
    struct dualMem* dualMem = (struct dualMem*) reg->memoryRegions[segment];
    atomic_store(&dualMem->remove, tx);
    return true;
}
