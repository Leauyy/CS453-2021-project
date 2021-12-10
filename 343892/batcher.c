//
// Created by Liudvikas Lazauskas on 26.11.21.
//
//TODO FIX WRITE AND READ OFFSETS FROM TARGET!!!!!
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

bool commit(struct batch* self, struct dualMem** dualMem ,size_t size, size_t align) {
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
            free(thisMem->wordLock);
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

        // has been written in current epoch
        size_t writtenBy = atomic_load(dualMem->wasWritten + varIdx);

        if (writtenBy>0) {
            size_t acc = atomic_load(dualMem->accessed + varIdx);
            if (acc == transactionId) { //in access set
                //read
                memcpy(target + writeOffset, dualMem->writeCopy + offset + writeOffset, align);
                return true;
            } else {
                if (printAborts){
                    printf("ABORT: Read aborted due to NOT RO + AND accessed by other transaction Them : %lu | Us : %lu.... Written by %lu \n", acc, transactionId,writtenBy);
                }
                return false;
            }
        } else {
            size_t expected = 0;
            accessSet
            if(!atomic_compare_exchange_strong(dualMem->accessed + varIdx, &expected, transactionId)) {
                printf("New Abort...\n");
                return false;
            }
            memcpy(target + writeOffset, dualMem->validCopy + offset + writeOffset, align);
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
    //printf("Access index = %lu\n", varIdx);
    if (atomic_load(dualMem->wasWritten + varIdx)>0){
        if (toPrint) {
            printf("Word was written in this epoch\n");
        }
        size_t tId = atomic_load(dualMem->accessed + varIdx);
        if (toPrint) {
            printf("Transaction ID of access = %lu, our ID = %lu, index = %lu\n", tId, transactionId, index+offset);
        }
        if (tId == transactionId) {
            if (toPrint) {
                printf("Word was not accessed before, or accessed by this Thread\n");
            }
            if (toPrint) {
                printf("Transaction writing %lu\n", transactionId);
            }
            //TODO check if write to correct result
            memcpy(dualMem->writeCopy + offset + writeOffset, source + writeOffset, align);
            return true;
        } else {
            //size_t tId = atomic_load(dualMem->accessed + offset + index);
            if (printAborts) {
                printf("ABORT: Write aborted due to WORD WAS ACCESSED BY OTHER TRANSACTIOn, US = %d, THEM = %d,\n"
                       " IDX = %llu, Align %llu, Offset %llu, EPOCH = %lu \n", transactionId, tId, varIdx, align, offset,
                       get_epoch(self));
            }
            //pthread_mutex_unlock(&dualMem->wordLock[varIdx]);
            return false;
        }
    } else {
        if (toPrint) {
            printf("NO word was written in this epoch\n");
        }
        size_t expected = 0;
        size_t totalAcc = atomic_load(dualMem->totalAccesses + varIdx);
        atomic_compare_exchange_strong(dualMem->accessed + varIdx, &expected, transactionId);
        size_t acc = atomic_load(dualMem->accessed + varIdx);

        if (acc != transactionId) {
            if (printAborts) {
                printf("ABORT: WRITE word was accessed before by: %lu, us: %lu, epoch = %lu\n", acc, transactionId,
                       get_epoch(self));
            }
            return false;
        } else {
            atomic_store(dualMem->wasWritten + varIdx, transactionId);
            if (toPrint) {
                printf("First to access word is transaction = %d\n", transactionId);
            }

            memcpy(dualMem->writeCopy+ offset + writeOffset, source + writeOffset, align);
            return true;
        }
    }
    //pthread_mutex_unlock(&dualMem->wordLock[varIdx]);
    return true;
}

void cleanup_read(struct batch* self, struct dualMem** dualMem,void* target, const void* source, size_t size, size_t reachedIndex, size_t align){
    if (leave(self)) {
        commit(self, dualMem, size, align);
        cleanup(self, *dualMem, size, align);
        //TODO commit
    }
}

void cleanup_write(struct batch* self, struct dualMem** dualMem,void* target, const void* source, size_t size, size_t reachedIndex, size_t align){
    if (leave(self)) {
        //printf("Transaction aborted cleaning up writes...\n");
        commit(self, dualMem, size, align);
        cleanup(self, *dualMem, size, align);
        //commit
    }
}

void cleanup(struct batch* self, struct dualMem* dualMem, size_t size, size_t align){
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