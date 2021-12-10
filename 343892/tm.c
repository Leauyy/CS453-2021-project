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

bool print = false;

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
};


/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/



shared_t tm_create(size_t size, size_t align) {
    if (print) {
        printf("Creating memory region \n");
    }
    // TODO: tm_create(size_t, size_t)
    struct region* region = (struct region*) malloc(sizeof(struct region));

    region->size        = size;
    region->align       = align;
    region->batcher = init(1024);

    //size_t words = size/align;

    //allocate first segment STM
    struct dualMem* dualMem= (struct dualMem*) malloc(sizeof(struct dualMem));
    if (posix_memalign((void**) &(dualMem->accessed), sizeof(atomic_size_t), size) != 0) {
        free(region);
        return invalid_shared;
    }
    if (posix_memalign((void**) &(dualMem->wasWritten), sizeof(atomic_size_t), size) != 0) {
        free(region);
        return invalid_shared;
    }
    if (posix_memalign((void**) &(dualMem->totalAccesses), sizeof(atomic_size_t), size) != 0) {
        free(region);
        return invalid_shared;
    }

    //create pointer array to segments
    size_t maxRegions = 1<<16;
    if (posix_memalign((void**) &(region->memoryRegions),sizeof(struct dualMem*), maxRegions*sizeof(maxRegions))){
        free(region);
        return invalid_shared;
    }

    atomic_store(&region->nextSegment,2);
    size_t thisPointer = 1;
    thisPointer = thisPointer<<49;
    region->memoryRegions[1] = dualMem;


    //printf("Accessed size = %lu || Number of words %lu, ||Size %lu \n",sizeof(atomic_size_t)*words, words, size);

    memset(dualMem->accessed, 0, size);
    memset(dualMem->wasWritten, 0, size);
    memset(dualMem->totalAccesses, 0, size);
    atomic_store(&dualMem->remove, 0);

    dualMem->size = size;
    dualMem->align = align;

    //printf("Init value of accessed: %d", *(&(dualMem->accessed) + 1*sizeof(atomic_size_t)));

    if (print) {
        printf("Allocating memory region \n");
    }
    if (posix_memalign(&(dualMem->validCopy), align, size) != 0) {
        free(region);
        return invalid_shared;
    }
    if (posix_memalign(&(dualMem->writeCopy), align, size) != 0) {
        free(region);
        return invalid_shared;
    }

    region->start = (void*) thisPointer;


    memset(dualMem->validCopy, 0, size);
    memset(dualMem->writeCopy, 0, size);

    //A is valid
    //dualMem->isAValid = true;
    region->dualMem = *dualMem;
    atomic_store(&region->nextROSlot, 2);
    atomic_store(&region->nextRWSlot, 1);

    dualMem->PREV = NULL;
    dualMem->NEXT = NULL;
    region->allocs = dualMem;

    //TODO check memory alloc
    //void* memory = (void*) ((uintptr_t)dualMem + sizeof(struct dualMem));

    printf("Finished creationg mem region \n");
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t unused(shared)) {
    // TODO: tm_destroy(shared_t)
    // TODO: clear threads
    struct region* reg = (struct region*) shared;
    if (print) {
        printf("Destroy called..., total transactions = %lu\n", (reg->nextROSlot+reg->nextRWSlot)/2);
    }
    struct dualMem* dm = reg->allocs;

    while (dm != NULL) {
        free(dm->validCopy);
        free(dm->writeCopy);
        free(dm->accessed);
        free(dm->wordLock);
        free(dm);
        dm = dm->NEXT;
    }
    free(reg->memoryRegions);
    free(reg);
    free(reg->batcher);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    // TODO: tm_start(shared_t)
    struct region* reg = (struct region*) shared;

    return reg->start;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    if (print) {
        printf("Size called...\n");
    }
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
    // TODO: tm_begin(shared_t)
    if (print) {
        printf("Begin the transaction...\n");
    }
    struct region* reg = (struct region*) shared;
    //mark RO
    enter(reg->batcher);

    if (print) {
        printf("Thread returned...\n");
    }

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
    if (print) {
        printf("End the transaction %lu... Before epoch %ld\n", tx, get_epoch(reg->batcher));
    }
    if (leave(reg->batcher)) {

        size_t index = atomic_fetch_add(&reg->batcher->finishedCounter, 1);
        reg->batcher->finishedTransactions[index] = tx;
        commit(reg->batcher, &reg->allocs, reg->size, reg->align);
        cleanup(reg->batcher, reg->allocs, reg->size, reg->align);
    } else {
        // Sleep
        size_t index = atomic_fetch_add(&reg->batcher->finishedCounter, 1);
        reg->batcher->finishedTransactions[index] = tx;
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
    // TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
    if (print) {
        printf("Reading transaction..., %ld\n", tx);
    }

    struct region* reg = (struct region*) shared;

    //size_t offset;
    size_t segment = ((size_t)source)>>49;
    size_t one = 1;
    size_t offset = ((size_t)source) & ((one<<49)-1);
    if (segment == 9) {
        //printf("Target = %llu, Segment # = %llu, Offset = %llu \n", source, segment, offset);
        if (offset > 3000) {
            raise(SIGTRAP);
        }
    }
    struct dualMem* dualMem = (struct dualMem*) reg->memoryRegions[segment];


    size_t align = reg->align;
    size_t numberOfWords = size/align;
    bool res;

    for (size_t i=0; i< numberOfWords; i++) {
        size_t idx = i*align;
        res = read_word(dualMem, idx, offset, target, align, tx);
        if (!res) {
            cleanup_read(reg->batcher, &reg->allocs, target, source,size,idx-align,align);
            //printf("Failed read transaction ... \n");
            return false;
        }
    }
    //printf("Successfully read transaction ... \n");
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
    // TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)
    if (print) {
        printf("Writing for the transaction..., %ld\n", tx);
    }
    struct region* reg = (struct region*) shared;
    size_t align = reg->align;
    size_t numberOfWords = size/align;

    //Get offset and segment. Then obtain memory region
    size_t segment = ((uintptr_t)target)>>49;
    size_t offset = ((uintptr_t)target) & 0xffffffffffff;
    struct dualMem* dualMem = (struct dualMem*) reg->memoryRegions[segment];


    bool res;
    for (size_t i=0; i < numberOfWords; i++) {
        //printf("Index = %d\n", idx);
        //printf("Writing to segment %llu, with offset %llu \n", segment, offset);
        res = write_word(reg->batcher, dualMem, i, source, offset, align, tx);
        if (!res) {
            if (print) {
                printf("Failed finishing writing a word from the transaction on thread = %ld, loop = %ld...\n", tx, i);
            }
            //pass all alocs to cleanup from start
            cleanup_write(reg->batcher, &reg->allocs, target, source, reg->size, i, align);
            return false;
        }
    }
    //printf("Successfully wrote the transaction...\n");
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
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
    if (print) {
        printf("Allocate memory for a transaction...\n");
    }
    struct region* reg = (struct region*) shared;
    size_t align = reg->align;
    if (size%align != 0){
        return abort_alloc;
    }

    size_t words = size/align;

    struct dualMem* dualMem= (struct dualMem*) malloc(sizeof(struct dualMem));
    if (posix_memalign((void**)&(dualMem->accessed), sizeof(atomic_size_t), size) != 0) {
        return abort_alloc;
    }
    if (posix_memalign((void**) &(dualMem->wasWritten), sizeof(atomic_size_t), size) != 0) {
        return abort_alloc;
    }
    if (posix_memalign((void**) &(dualMem->totalAccesses), sizeof(atomic_size_t), size) != 0) {
        free(region);
        return invalid_shared;
    }

    size_t thisPointer = atomic_fetch_add(&reg->nextSegment,1);
    reg->memoryRegions[thisPointer] = dualMem;
    thisPointer = thisPointer<<49;

    memset(dualMem->accessed, 0, size);
    memset(dualMem->wasWritten, 0, size);
    dualMem->align = align;
    dualMem->size = size;

    if (print) {
        printf("Allocating memory region \n");
    }
    if (posix_memalign(&(dualMem->validCopy), align, size) != 0) {
        return abort_alloc;
    }
    if (posix_memalign(&(dualMem->writeCopy), align, size) != 0) {
        return abort_alloc;
    }

    dualMem->wordLock = (pthread_mutex_t*) malloc(words * sizeof(pthread_mutex_t));
    for (size_t i=0; i< words; i++) {
        if (pthread_mutex_init(&(dualMem->wordLock[i]), NULL) != 0){
            printf("failed to create lock %lu\n", i);
            return abort_alloc;
        }
    }

    memset(dualMem->validCopy, 0, size);
    memset(dualMem->writeCopy, 0, size);
    memset(dualMem->totalAccesses, 0, size);
    atomic_store(&dualMem->remove, 0);

    dualMem->PREV = NULL;
    dualMem->NEXT = reg->allocs;

    //printf("Alloc %llu, total transactions = %llu\n", reg->allocs, (reg->nextROSlot+reg->nextRWSlot)/2);
    if (dualMem->NEXT) dualMem->NEXT->PREV = dualMem;
    reg->allocs = dualMem;

    //printf("Next = %llu, PREV = %llu\n", dualMem->NEXT, dualMem->PREV);

    //TODO check memory alloc
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
    if (print) {
        printf("Free memory for a transaction... On addr %lu\n", target);
    }
    // TODO: tm_free(shared_t, tx_t, void*)
    //struct dualMem* dualMem = (struct dualMem*) ((uintptr_t) dualMem - sizeof(struct dualMem));
    struct region* reg = (struct region*) shared;
    size_t segment = ((size_t)target)>>49;

    struct dualMem* dualMem = (struct dualMem*) reg->memoryRegions[segment];
    atomic_store(&dualMem->remove, tx);

    // Remove from the linked list
//    if (dualMem->PREV) dualMem->PREV->NEXT = dualMem->NEXT;
//    else ((struct region*) shared)->allocs = dualMem->NEXT;
//    if (dualMem->NEXT) dualMem->NEXT->PREV = dualMem->PREV;
//
//    free(dualMem->validCopy);
//    free(dualMem->writeCopy);
//    free(dualMem->accessed);
//    free(dualMem->wordLock);
//    free(dualMem);
    return true;
}
