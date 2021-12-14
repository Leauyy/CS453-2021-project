//
// Created by Liudvikas Lazauskas on 26.11.21.
//
#include "batcher.h"
bool toPrint = false;
bool printThreads = false;
bool printAborts = false;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

static inline void pause()
{
#if (defined(__i386__) || defined(__x86_64__)) && defined(USE_MM_PAUSE)
    _mm_pause();
#else
    sched_yield();
#endif
}

struct batch* init(size_t threadCount) {
    struct batch* batch = (struct batch*) malloc(sizeof(struct batch));
    batch->counter = 0;
    batch->remaining = 0;
    atomic_store(&batch->last, 0);
    lock_init(&(batch->lock));

    batch->finishedTransactions = (size_t*) malloc(threadCount * sizeof(threadCount));
    atomic_store(&batch->finishedCounter, 0);

    return batch;
}

size_t get_epoch(struct batch *self) {
    return atomic_load(&self->counter);
}

void enter(struct batch *self) {
    size_t expected = 0;
    lock_acquire(&(self->lock));
    if (atomic_compare_exchange_strong(&self->remaining, &expected,1)) {
        lock_release(&(self->lock));
    } else {
        //append current thread

        //TODO batcher unique condition
        //Edge case, thread enters after leave
        size_t epoch = get_epoch(self);
        atomic_fetch_add(&self->last, 1);
        lock_wait(&(self->lock));
        lock_release(&(self->lock));
    }


    return;
}

bool leave(struct batch *self) {
    //last one to leave
    size_t expected = 1;
    lock_acquire(&(self->lock));
    size_t total = atomic_load(&self->last);
    if (atomic_compare_exchange_strong(&self->remaining, &expected, total)) {
        atomic_store(&self->last,0);
        atomic_fetch_add(&self->counter, 1);
        return true;
    } else {
        atomic_fetch_sub(&self->remaining, 1);
        lock_release(&(self->lock));
        return false;
    }

}

bool commit(struct batch* self, struct dualMem** dualMem) {
    size_t words;
    uintptr_t varIdx;
    size_t epoch = get_epoch(self)-1;

    //Pass first dual mem in linked list
    struct dualMem* dm = *dualMem;
    int number = 0;

    size_t elements = atomic_load(&self->finishedCounter);
    size_t written_by = 0;
    do {
        words = dm->size/dm->align;
        size_t rem = atomic_load(&dm->remove);
        bool found = false;
        for (size_t i = 0; i <= words; i++) {
            varIdx = i * dm->align;
            spin_lock(dm->spinLock+i);
            for (size_t j = 0; j < elements; j++) {
                if (self->finishedTransactions[j]==rem) {
                    found = true;
                }
                written_by = atomic_load(dm->wasWritten + i);
                if (written_by == self->finishedTransactions[j] && written_by!=0) {
                    printf("Comitting Transaction: %lu || Word index %lu || On Epoch %lu\n", written_by, i, epoch);
                    memcpy(dm->validCopy + varIdx, dm->writeCopy + varIdx, dm->align);
                    break;
                }
            }
            spin_unlock(dm->spinLock+i);
        }
        if (rem>0 && found){
            struct dualMem* thisMem = dm;
            if (dm->PREV) dm->PREV->NEXT = dm->NEXT;
            else *dualMem = dm->NEXT;
            if (dm->NEXT) dm->NEXT->PREV = dm->PREV;
            dm = dm->NEXT;

            free(thisMem->validCopy);
            free(thisMem->writeCopy);
            free(thisMem->accessed);
            free(thisMem->totalAccesses);
            free(thisMem->spinLock);
            free(thisMem);
        } else {
            memset(dm->writeCopy, 0, dm->size);
            dm = dm->NEXT;
        }
        number++;
    } while(dm != NULL);
    return true;
}

bool read_word(struct dualMem* dualMem, size_t index, void* target, size_t offset, size_t transactionId) {

    size_t varIdx = offset/dualMem->align + index;
    lock_acquire(dualMem->word_lock+varIdx);


    uintptr_t writeOffset = index*dualMem->align;
    //accesed word index

    bool result = false;

    bool is_ro = (transactionId % 2) == 0;

    if (is_ro){
        memcpy(target + writeOffset, dualMem->validCopy + offset + writeOffset, dualMem->align);
        result = true;
    } else {
        // Acquire lock for writing to this word
        // has been written in current epoch
        size_t expected = 0;
        if(atomic_compare_exchange_strong(dualMem->accessed + varIdx, &expected, transactionId)) {
            //First to access
            atomic_fetch_add(dualMem->totalAccesses + varIdx, 1);
        }

        size_t acc = atomic_load(dualMem->accessed + varIdx);
        size_t writtenBy = atomic_load(dualMem->wasWritten + varIdx);

        if (writtenBy>0) {
            if (acc == transactionId && writtenBy == transactionId) { //in access set + I don't fully understand memory order so this check is necessary
                //read
                memcpy(target + writeOffset, dualMem->writeCopy + offset + writeOffset, dualMem->align);
                result = true;
            } else {
                result = false;
            }
        } else {
            size_t expected = 0;
            if (!atomic_compare_exchange_strong(dualMem->accessed + varIdx, &expected, transactionId) && acc!=transactionId) {
                atomic_fetch_add(dualMem->totalAccesses + varIdx, 1);
            }
            memcpy(target + writeOffset, dualMem->validCopy + offset + writeOffset, dualMem->align);
            result = true;
        }
        //Release lock for this word
    }
    lock_release(dualMem->word_lock+varIdx);
    return result;
}
bool write_word(struct dualMem* dualMem, size_t index, void const* source, size_t offset,  size_t transactionId) {
    size_t writeOffset = index*dualMem->size;
    //accesed word index
    size_t varIdx = offset/dualMem->align + index;
    lock_acquire(dualMem->word_lock+varIdx);
    bool result = false;


    size_t writtenBy = atomic_load(dualMem->wasWritten + varIdx);
    if (writtenBy>0){
        size_t tId = atomic_load(dualMem->accessed + varIdx);
        if (tId == transactionId) {
            memcpy(dualMem->writeCopy + offset + writeOffset, source + writeOffset, dualMem->align);
            result = true;
        } else {
            result = false;
        }
    } else {

        // If we got an access to this object show that we are first to access
        size_t totalAcc = atomic_load(dualMem->totalAccesses + varIdx);

        if (totalAcc>1) {
            result = false;
        } else {
            size_t expected = 0;

            if (atomic_compare_exchange_strong(dualMem->accessed + varIdx, &expected, transactionId)){
                atomic_fetch_add(dualMem->totalAccesses+varIdx,1);
            } else if (transactionId != atomic_load(dualMem->accessed+varIdx)) {
                result = false;
            }
            memcpy(dualMem->writeCopy+ offset + writeOffset, source + writeOffset, dualMem->align);

            atomic_store(dualMem->wasWritten+varIdx, transactionId);
            result = true;
        }
    }
    lock_release(dualMem->word_lock+varIdx);
    return result;
}

void cleanup_read(struct region* region, struct batch* self, struct dualMem** dualMem){
    if (leave(self)) {
        commit(self, dualMem);
        cleanup(region ,self, *dualMem);
    }
}

void cleanup_write(struct region* region, struct batch* self, struct dualMem** dualMem){
    if (leave(self)) {
        commit(self, dualMem);
        cleanup(region, self, *dualMem);
    }
}

void cleanup(struct region* region, struct batch* self, struct dualMem* dualMem){

    struct dualMem* dm = dualMem;
    do {
        memset(dm->accessed, 0, dm->size);
        memset(dm->wasWritten, 0, dm->size);
        memset(dm->totalAccesses, 0, dm->size);
        dm = dm->NEXT;
    } while(dm != NULL);
    atomic_store(&self->finishedCounter, 0);

    atomic_store(&region->nextRWSlot, 1);
    atomic_store(&region->nextROSlot, 2);


    lock_release(&(self->lock));
    //new condition for new epoch

}

void spin_lock(atomic_bool* lock){
    bool expected = false;
    while (!atomic_compare_exchange_weak_explicit(lock, &expected, true, memory_order_acquire, memory_order_relaxed)){
        expected = false;
        while (atomic_load_explicit(lock, memory_order_relaxed)) pause();
    }
}

void spin_unlock(atomic_bool* lock){
    atomic_store_explicit(lock, false, memory_order_release);
}