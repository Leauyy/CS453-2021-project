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


/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/



shared_t tm_create(size_t size, size_t align) {
    if (true) {
        printf("Creating memory region \n");
    }
    // TODO: tm_create(size_t, size_t)
    struct region* region = (struct region*) malloc(sizeof(struct region));

    region->size        = size;
    region->align       = align;
    region->batcher = *init(128);


    size_t noWords = size/align;
    struct dualMem* dualMem = (struct dualMem*) malloc(sizeof(struct dualMem));
    struct word* words = malloc(sizeof(struct word)*noWords);

    //Pointers to words structs
    void **contMem = (void**) malloc(sizeof(void*));
    if (posix_memalign(contMem, align, size) != 0) {
        free(region);
        return invalid_shared;
    }



    if (true) {
        printf("Allocating memory region| size %lu| Align %llu \n", size, align);
    }


    printf("Memory start %llu\n",contMem);

    size_t alignDiff = align/sizeof(void*);

    for (size_t i=0; i<noWords; i++) {
        struct word* _word = (struct word*) (words + i*sizeof(struct word*));
        _word->wordId = i;
        _word->validCopy = malloc(align);
        _word->writeCopy = malloc(align);
        atomic_store(&_word->wasWritten, 0);
        atomic_store(&_word->accessed, 0);
        *(contMem + i*align) = (void*) _word;

        printf("Void addr value = %llu || Void addr = %llu\n", *(contMem + i*align), contMem+i*alignDiff);
    }

    region->start = contMem;

    dualMem->words = words;

    region->dualMem = *dualMem;
    atomic_store(&region->nextROSlot, 2);
    atomic_store(&region->nextRWSlot, 1);

    dualMem->PREV = NULL;
    dualMem->NEXT = region->allocs;
    if (dualMem->NEXT) dualMem->NEXT->PREV = dualMem;
    region->allocs = dualMem;

    //TODO check memory alloc

    printf("Finished creating mem region \n");
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

    return (void*)reg->start;
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
    if (print) {
        printf("Begin the transaction...\n");
    }
    struct region* reg = (struct region*) shared;
    //mark RO
    enter(&reg->batcher);

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
        printf("End the transaction %lu... Before epoch %d\n", tx, get_epoch(&reg->batcher));
    }
    if (leave(&reg->batcher, &(reg->dualMem), reg->size, reg->align)) {
        commit(&(reg->dualMem), reg->size, reg->align, tx);
        cleanup(&(reg->batcher), &(reg->dualMem), reg->size, reg->align);
    } else {
        commit(&(reg->dualMem), reg->size, reg->align, tx);
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

    //size_t offset;
    size_t offset = 0;

    //printf("Offset = %lu \n", offset);

    size_t align = reg->align;
    size_t numberOfWords = size/align;
    bool res;
    //bool is_ro = (tx < 0);

    for (size_t i=0; i<numberOfWords; i++) {
        size_t idx = i*align;
        struct word* _word = (struct word*) source;

        res = read_word(_word, idx, offset,target+idx, align, tx);
        if (!res) {
            cleanup_read(&reg->batcher,&reg->dualMem,target,source,size,idx-align,align);
            printf("Failed read transaction ... \n");
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
    if (true) {
        printf("Writing for the transaction..., %d\n", tx);
    }

    struct region* reg = (struct region*) shared;
    size_t align = reg->align;
    size_t numberOfWords = size/align;

    size_t offset;
    offset = target - reg->start;

    printf("Target addr %llu\n", target);

    struct word _word = *(struct word*) (target+24);
    printf("Word access = %llu\n || Word ID = %llu \n", atomic_load(&(_word.accessed)), _word.wordId);
    raise(SIGTRAP);
    void* addr;


    bool res;

    for (size_t i=0; i < numberOfWords; i++) {
        //printf("Index = %d\n", idx);
        //struct word* _word = (struct word*) target;
        //printf("Word access = %llu\n || Word ID = %llu \n", atomic_load(&(_word.accessed)), _word.wordId);
        res = write_word(&reg->batcher, &_word, i, source, offset, align, tx);
        if (!res) {
            if (print) {
                printf("Failed finishing writing a word from the transaction on thread = %d, loop = %d...\n", tx, i);
            }
            cleanup_write(&reg->batcher,&reg->dualMem,target,source,reg->size,i,align);
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

//    size_t align = reg->align;
//
//    if (size%align != 0){
//        return abort_alloc;
//    }
//
//    size_t words = size/align;
//
//    //reg.align * words
//    struct dualMem* dualMem= (struct dualMem*) malloc(sizeof(struct dualMem));
//    dualMem->wasWritten = (atomic_size_t*) malloc(size);
//    dualMem->accessed = (atomic_size_t *) malloc(size);
//    dualMem->validCopy = (void*) malloc(size);
//    dualMem->writeCopy = (void*) malloc(size);
//
//    memset(dualMem->accessed, 0, size);
//    memset(dualMem->wasWritten, 0, size);
//    memset(dualMem->validCopy, 0, size);
//    memset(dualMem->writeCopy, 0, size);
//
//    reg->dualMem = *dualMem;
//
//    //
//    dualMem->PREV = NULL;
//    dualMem->NEXT = reg->allocs;
//    if (dualMem->NEXT) dualMem->NEXT->PREV = dualMem;
//    reg->allocs = dualMem;
//
//    //TODO check memory alloc
//    void* memory = (void*) ((uintptr_t)dualMem + sizeof(struct dualMem));
//    *target = memory;
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
    if (print) {
        printf("Free memory for a transaction...\n");
    }
    // TODO: tm_free(shared_t, tx_t, void*)
//    struct dualMem* dualMem = (struct dualMem*) ((uintptr_t) dualMem - sizeof(struct dualMem));
//
//    // Remove from the linked list
//    if (dualMem->PREV) dualMem->PREV->NEXT = dualMem->NEXT;
//    else ((struct region*) shared)->allocs = dualMem->NEXT;
//    if (dualMem->NEXT) dualMem->NEXT->PREV = dualMem->PREV;
//
//    free(dualMem->validCopy);
//    free(dualMem->writeCopy);
//    free(dualMem->accessed);
//    free(dualMem);
    return true;
}
