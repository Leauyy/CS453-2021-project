//
// Created by Liudvikas Lazauskas on 26.11.21.
//
#include "batcher.h"
bool toPrint = false;
bool printThreads = false;
bool printAborts = false;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

struct batch* init(size_t threadCount) {
    if (toPrint) {
        printf("Initializing Batcher\n");
    }
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
        if (printThreads) {
            printf("Thread is first\n");
        }
        lock_release(&(self->lock));
    } else {
        //append current thread
        size_t epoch = get_epoch(self);
        //wrap mutex

        //TODO batcher unique condition
        //Edge case, thread enters after leave
        size_t last = atomic_fetch_add(&self->last, 1);

        if (printThreads) {
            printf("Thread %d is entering sleep| EPOCH = %lu\n", last, epoch);
        }
        //lock_release(&(self->lock));
        //pthread_mutex_lock(&(self->lock.mutex));
        pthread_cond_wait(&(self->lock.cv), &(self->lock.mutex));
        pthread_mutex_unlock(&(self->lock.mutex));
        if (printThreads) {
            printf("Thread %d is awake| FROM EPOCH = %lu\n", last, epoch);
        }
    }


    return;
}

bool leave(struct batch *self) {
    //last one to leave
    size_t expected = 1;
    lock_acquire(&(self->lock));
    size_t total = atomic_load(&self->last);
    if (printThreads) {
        printf("Thread is leaving... Current sleeping %d\n", total);
    }
    if (atomic_compare_exchange_strong(&self->remaining, &expected, total)) {
        size_t epoch = atomic_fetch_add(&self->counter,1 );
        if (printThreads) {
            printf("Waking up total threads : %d, on epoch %d \n", total, epoch);
        }
        atomic_store(&self->last,0);

        if (printThreads) {
            size_t remains = atomic_load(&self->remaining);
            printf("Thread has finished an epoch \n");
        }
        return true;
    } else {
        size_t remains = atomic_fetch_sub(&self->remaining, 1);
        if (printThreads) {
            printf("Current active threads %d \n", remains);
        }
        lock_release(&(self->lock));
        return false;
    }

}

bool commit(struct batch* self, struct dualMem** dualMem) {
    if (toPrint) {
        printf("Commiting transaction...\n");
    }

    size_t words;
    size_t varIdx;

    //Pass first dual mem in linked list
    struct dualMem* dm = *dualMem;
    int number = 0;

    size_t elements = atomic_load(&self->finishedCounter);

    do {
        //printf("Segment %lu \n", number);
        words = dm->size/dm->align;
        size_t rem = atomic_load(&dm->remove);
        bool found = false;
        for (size_t i = 0; i < words; i++) {
            varIdx = i * dm->align;
            for (size_t j = 0; j < elements; j++) {
                if (self->finishedTransactions[j]==rem) {
                    found = true;
                }
                if (atomic_load(dm->wasWritten + i) == self->finishedTransactions[j]) {
                    memcpy(dm->validCopy + varIdx, dm->writeCopy + varIdx, dm->align);
                    break;
                }
            }
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
            dm = dm->NEXT;
        }
        number++;
    } while(dm != NULL);
    return true;
}

bool read_word(struct dualMem* dualMem, size_t index, size_t offset, void* target, size_t align, size_t transactionId) {
    //if read only
    if(toPrint) {
        printf("Reading word \n");
    }
    size_t writeOffset = index*align;
    //accesed word index
    size_t varIdx = offset/align + index;
    bool is_ro = (transactionId % 2) == 0;

    if (is_ro){
        if(toPrint) {
            printf("Reading word| Is Read Only\n");
        }
        memcpy(target + writeOffset, dualMem->validCopy + offset + writeOffset, align);
        return true;
    } else {
        // Acquire lock for writing to this word
        spin_lock(dualMem->spinLock+varIdx);
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
                memcpy(target + writeOffset, dualMem->writeCopy + offset + writeOffset, align);
                spin_unlock(dualMem->spinLock+varIdx);
                return true;
            } else {
                if (printAborts){
                    printf("ABORT: Read aborted due to NOT RO + AND accessed by other transaction Them : %lu | Us : %lu.... Written by %lu \n", acc, transactionId,writtenBy);
                }
                spin_unlock(dualMem->spinLock+varIdx);
                return false;
            }
        } else {
            size_t expected = 0;
            if (!atomic_compare_exchange_strong(dualMem->accessed + varIdx, &expected, transactionId) && acc!=transactionId) {
                atomic_fetch_add(dualMem->totalAccesses + varIdx, 1);
            }
            memcpy(target + writeOffset, dualMem->validCopy + offset + writeOffset, align);


            spin_unlock(dualMem->spinLock+varIdx);
            return true;
        }
        //Release lock for this word
    }
}
bool write_word(struct batch* self, struct dualMem* dualMem, size_t index, void const* source, size_t offset, size_t align, size_t transactionId) {
    if (toPrint) {
        printf("Writing word for transaction, with index %d\n", index);
    }

    size_t writeOffset = index*align;
    //accesed word index
    size_t varIdx = offset/align + index;
    spin_lock(dualMem->spinLock+varIdx);

    size_t writtenBy = atomic_load(dualMem->wasWritten + varIdx);
    if (writtenBy>0){
        if (toPrint) {
            printf("Word was written in this epoch\n");
        }
        size_t tId = atomic_load(dualMem->accessed + varIdx);
        if (toPrint) {
            printf("Transaction ID of access = %lu, our ID = %lu, index = %lu\n", tId, transactionId, index+offset);
        }
        if (tId == transactionId) {
            if (toPrint) {
                printf("Word was accessed before by this transaction %lu\n", transactionId);
            }
            if (toPrint) {
                printf("Transaction writing %lu\n", transactionId);
            }
            //TODO check if write to correct result
            memcpy(dualMem->writeCopy + offset + writeOffset, source + writeOffset, align);
            spin_unlock(dualMem->spinLock+varIdx);
            return true;
        } else {
            if (printAborts) {
                printf("ABORT: Write aborted due to WORD WAS ACCESSED BY OTHER TRANSACTIOn, US = %d, THEM = %d,\n"
                       " IDX = %llu, Align %llu, Offset %llu, EPOCH = %lu \n", transactionId, tId, varIdx, align, offset,
                       get_epoch(self));
            }
            spin_unlock(dualMem->spinLock+varIdx);
            return false;
        }
    } else {
        if (toPrint) {
            printf("NO word was written in this epoch, %lu\n");
        }
        size_t expected = 0;
        // If we got an access to this object show that we are first to access
        if (atomic_compare_exchange_strong(dualMem->accessed + varIdx, &expected, transactionId)){
            atomic_fetch_add(dualMem->totalAccesses+varIdx,1);
        }
        size_t totalAcc = atomic_load(dualMem->totalAccesses + varIdx);
        size_t acc = atomic_load(dualMem->accessed + varIdx);

        if (totalAcc>1 || acc != transactionId) {
            if (printAborts) {
                printf("ABORT: WRITE word was accessed before by: %lu, us: %lu, total accesses = %lu\n", acc, transactionId, totalAcc);
            }
            spin_unlock(dualMem->spinLock+varIdx);
            return false;
        } else {
            expected = 0;
            if (!atomic_compare_exchange_strong(dualMem->wasWritten + varIdx,&expected, transactionId)){
                printf("Wtf\n");
                return false;
            }
            if (toPrint) {
                printf("First to access word %lu is transaction = %d, epoch %lu\n", dualMem + offset,transactionId, get_epoch(self));
            }

            memcpy(dualMem->writeCopy+ offset + writeOffset, source + writeOffset, align);
            spin_unlock(dualMem->spinLock+varIdx);
            return true;
        }
    }
}

void cleanup_read(struct batch* self, struct dualMem** dualMem){
    if (leave(self)) {
        commit(self, dualMem);
        cleanup(self, *dualMem);
        //TODO commit
    }
}

void cleanup_write(struct batch* self, struct dualMem** dualMem){
    if (leave(self)) {
        //printf("Transaction aborted cleaning up writes...\n");
        commit(self, dualMem);
        cleanup(self, *dualMem);
        //commit
    }
}

void cleanup(struct batch* self, struct dualMem* dualMem){
    //printf("Cleaning up\n");

    struct dualMem* dm = dualMem;
    do {
        //printf("Segment %llu \n", number);
        memset(dm->accessed, 0, dm->size);
        memset(dm->wasWritten, 0, dm->size);
        memset(dm->totalAccesses, 0, dm->size);
        dm = dm->NEXT;
    } while(dm != NULL);
    //printf("Broadcasting awake \n");
    atomic_store(&self->finishedCounter, 0);
    pthread_cond_broadcast(&(self->lock.cv));
    //new condition for new epoch
    lock_release(&(self->lock));
}

void spin_lock(atomic_bool* lock){
    bool expected = false;
    while (!atomic_compare_exchange_weak_explicit(lock, &expected, true, memory_order_acquire, memory_order_relaxed)){
        expected = false;
        while (atomic_load_explicit(lock, memory_order_relaxed));
    }
}

void spin_unlock(atomic_bool* lock){
    atomic_store_explicit(lock, false, memory_order_release);
}