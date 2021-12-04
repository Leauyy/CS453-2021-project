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
    struct batch batcher;
    struct dualMem dualMem;
    atomic_size_t nextROSlot;
    atomic_size_t nextRWSlot;
};

static const tx_t read_only_tx  = UINTPTR_MAX - 10;
static const tx_t read_write_tx = UINTPTR_MAX - 11;


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

    //    if (posix_memalign(&(region->start), align, size) != 0) {
    //        free(region);
    //        return invalid_shared;
    //    }

    //memset(region->start, 0, size);
    //region->allocs      = NULL;
    region->size        = size;
    region->align       = align;
    region->batcher = *init(128);


    //    if (size%align != 0){
    //        return abort_alloc;
    //    }

    size_t words = size/align;

    //reg.align * words
    struct dualMem* dualMem= (struct dualMem*) malloc(sizeof(struct dualMem));
    //dualMem->wasWritten = (atomic_int*) malloc(words* sizeof(atomic_int));
    //dualMem->accessed = (atomic_size_t *) malloc(words* sizeof(atomic_size_t));
    if (posix_memalign(&(dualMem->accessed), sizeof(atomic_size_t), size) != 0) {
        free(region);
        return invalid_shared;
    }

    printf("Accessed size = %lu || Number of words %lu, ||Size %lu \n",sizeof(atomic_size_t)*words, words, size);

    memset(dualMem->accessed, 0, size);
    //memset(dualMem->wasWritten, 0, words* sizeof(atomic_int));
    atomic_store(&dualMem->wasWritten, 0);

    //printf("Init value of accessed: %d", *(&(dualMem->accessed) + 1*sizeof(atomic_size_t)));

    if (print) {
        printf("Allocating memory region \n");
    }
    if (posix_memalign(&(dualMem->copyA), align, size) != 0) {
        free(region);
        return invalid_shared;
    }
    if (posix_memalign(&(dualMem->copyB), align, size) != 0) {
        free(region);
        return invalid_shared;
    }

    region->start = &(dualMem->copyA);

    //memset(dualMem->accessed, 0, words* sizeof(atomic_int));
    //memset(dualMem->wasWritten, 0, words* sizeof(atomic_int));

    memset(dualMem->copyA, 0, size);
    memset(dualMem->copyB, 0, size);

    //printf("Address of A: %lu || Address of B:%lu\n Size %d\n", dualMem->copyA, dualMem->copyB, size);

    //A is valid
    dualMem->isAValid = true;
    region->dualMem = *dualMem;
    atomic_store(&region->nextROSlot, 2);
    atomic_store(&region->nextRWSlot, 1);

    dualMem->PREV = NULL;
    dualMem->NEXT = region->allocs;
    if (dualMem->NEXT) dualMem->NEXT->PREV = dualMem;
    region->allocs = dualMem;

    //TODO check memory alloc
    //void* memory = (void*) ((uintptr_t)dualMem + sizeof(struct dualMem));

    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t unused(shared)) {
    // TODO: tm_destroy(shared_t)
    // TODO: clear threads
    if (true) {
        printf("Destroy called...\n");
    }
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t shared) {
    // TODO: tm_start(shared_t)
    struct region* reg = (struct region*) shared;

    if (reg->dualMem.isAValid) {
        //printf("Returned copy A start addr\n");
        return reg->dualMem.copyA;
    } else {
        //printf("Returned copy B start addr\n");
        return reg->dualMem.copyB;
    }
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t unused(shared)) {
    if (print) {
        printf("Size called...\n");
    }
    return ((struct region*) shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t unused(shared)) {
    return ((struct region*) shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t shared, bool is_ro) {
    // TODO: tm_begin(shared_t)
    if (true) {
        printf("Begin the transaction...\n");
    }
    struct region* reg = (struct region*) shared;
    //mark RO
    enter(&reg->batcher);

    if (true) {
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
bool tm_end(shared_t shared, tx_t unused(tx)) {
    // TODO: tm_end(shared_t, tx_t)
    if (true) {
        printf("End the transaction...\n");
    }
    struct region* reg = (struct region*) shared;
    if (leave(&reg->batcher, &(reg->dualMem), reg->size, reg->align)) {
        commit(&(reg->dualMem), reg->size, reg->align);
        lock_release(&(reg->batcher.lock));
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
    if (true) {
        printf("Reading transaction..., %d\n", tx);
    }

    struct region* reg = (struct region*) shared;

    size_t offset;
    if (reg->dualMem.isAValid) {
        offset = source - reg->dualMem.copyA;
    } else {
        offset = source - reg->dualMem.copyB;
    }

    size_t align = reg->align;
    size_t numberOfWords = size/align;
    bool res;
    //bool is_ro = (tx < 0);

    for (size_t i=0; i<numberOfWords; i++) {
        size_t idx = i*align;
        res = read_word(&reg->dualMem, idx, offset,target+idx, align, tx);
        if (!res) {
            cleanup_read(&reg->batcher,&reg->dualMem,target,source,size,idx-align,align);
            printf("Failed read transaction ... \n");
            return false;
        }
    }
    printf("Successfully read transaction ... \n");
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
        printf("Writing for the transaction..., %d\n", tx);
    }
    struct region* reg = (struct region*) shared;
    size_t align = reg->align;
    size_t numberOfWords = size/align;
    size_t thread = (size_t) pthread_self();
    //printf("target = %d, thread %d\n", target, thread);

    size_t offset;
    if (reg->dualMem.isAValid) {
        offset = target - reg->dualMem.copyA;
    } else {
        offset = target - reg->dualMem.copyB;
    }

    //printf("Alignment= %d, Size = %d\n", align, size);

    bool res;

    for (size_t i=0; i<numberOfWords;i++) {
        //printf("Index = %d\n", idx);
        res = write_word(&reg->dualMem, i, source, offset, align, tx);
        if (!res) {
            if (true) {
                printf("Failed finishing writing a word from the transaction on thread = %d, loop = %d...\n", tx, i);
            }
            cleanup_write(&reg->batcher,&reg->dualMem,target,source,size,i,align);
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
alloc_t tm_alloc(shared_t shared, tx_t unused(tx), size_t size, void** unused(target)) {
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
    if (true) {
        printf("Allocate memory for a transaction...\n");
    }
    struct region* reg = (struct region*) shared;

    size_t align = reg->align;

    if (size%align != 0){
        return abort_alloc;
    }

    size_t words = size/align;

    //reg.align * words
    struct dualMem* dualMem= (struct dualMem*) malloc(sizeof(struct dualMem));
    //dualMem->wasWritten = (atomic_int*) malloc(words* sizeof(atomic_int));
    dualMem->accessed = (atomic_size_t *) malloc(words* sizeof(atomic_size_t));
    dualMem->copyA = (void*) malloc(size);
    dualMem->copyB = (void*) malloc(size);

    memset(dualMem->accessed, 0, words* sizeof(atomic_int));
    //memset(dualMem->wasWritten, 0, words* sizeof(atomic_int));
    memset(dualMem->copyA, 0, size);
    memset(dualMem->copyB, 0, size);

    reg->dualMem = *dualMem;

    //
    dualMem->PREV = NULL;
    dualMem->NEXT = reg->allocs;
    if (dualMem->NEXT) dualMem->NEXT->PREV = dualMem;
    reg->allocs = dualMem;

    //TODO check memory alloc
    void* memory = (void*) ((uintptr_t)dualMem + sizeof(struct dualMem));
    *target = memory;
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
    if (true) {
        printf("Free memory for a transaction...\n");
    }
    // TODO: tm_free(shared_t, tx_t, void*)
    struct dualMem* dualMem = (struct dualMem*) ((uintptr_t) dualMem - sizeof(struct dualMem));

    // Remove from the linked list
    if (dualMem->PREV) dualMem->PREV->NEXT = dualMem->NEXT;
    else ((struct region*) shared)->allocs = dualMem->NEXT;
    if (dualMem->NEXT) dualMem->NEXT->PREV = dualMem->PREV;

    free(dualMem->copyA);
    free(dualMem->copyB);
    free(dualMem->accessed);
    free(dualMem);
    return true;
}
