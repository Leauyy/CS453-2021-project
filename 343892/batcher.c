//
// Created by Liudvikas Lazauskas on 26.11.21.
//

#include "batcher.h"
bool toPrint = false;
bool printThreads = true;
bool printAborts = true;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

struct batch* init(size_t threadCount) {
    if (toPrint) {
        printf("Initializing Batcher\n");
    }
    struct batch* batch = (struct batch*) malloc(sizeof(struct batch));
    batch->counter = 0;
    batch->remaining = 0;
    batch->threads = (struct threads*) malloc(sizeof(struct threads)*threadCount);
    batch->size = threadCount;
    atomic_store(&batch->last, 0);

    lock_init(&(batch->lock));

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
        
        //wrap mutex

        //TODO batcher unique condition
        //Edge case, thread enters after leave
        size_t last = atomic_fetch_add(&self->last, 1);

        //warp mutex

        //self->threads[last].thread = pthread_self();

        pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

        //self->threads[last].lock = lock;

        if (printThreads) {
            printf("Thread %d is entering sleep\n", last);
        }
        //lock_release(&(self->lock));
        //pthread_mutex_lock(&(self->lock.mutex));
        pthread_cond_wait(&cond, &(self->lock.mutex));
        pthread_mutex_unlock(&(self->lock.mutex));
        if (printThreads) {
            printf("Thread %d is awake\n", last);
        }
    }


    return;
}

bool leave(struct batch *self, struct dualMem* dualMem, size_t size, size_t align) {
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
        // unlock mutex
        // clear blocked list

        //wrap mutex
        atomic_store(&self->last,0);
        // return true and :


        pthread_cond_broadcast(&cond);
        if (printThreads) {
            size_t remains = atomic_load(&self->remaining);
            printf("Thread has finished an epoch \n");
        }
        //wrap mutex
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

bool commit(struct dualMem* dualMem ,size_t size, size_t align) {
    if (printThreads) {
        printf("Commiting transaction...\n");
    }
    size_t words = size/align;

    if (!dualMem->isAValid) {
        memcpy(dualMem->copyB, dualMem->copyA, size);
    } else {
        memcpy(dualMem->copyA, dualMem->copyB, size);
    }

    //raise(SIGTRAP);

    if (memcmp(dualMem->copyA,dualMem->copyB, size)){
        printf("Could not equal memory regions\n");
    }

    dualMem->isAValid = !dualMem->isAValid;

    //reset accessed
    //memset(dualMem->wasWritten, 0, words* sizeof(atomic_int));
    atomic_store(&dualMem->wasWritten, 0);
    memset(dualMem->accessed, 0, words* sizeof(atomic_size_t));

    if (toPrint) {
        printf("Finished commiting the transaction\n");
    }
    return true;
}

bool read_word(struct dualMem* dualMem, size_t index, size_t offset, void* target, size_t align, size_t transactionId) {
    //if read only
    if(toPrint) {
        printf("Reading word \n");
    }
    //size_t thread = (size_t) pthread_self();
    size_t varIdx = + offset/align + index*sizeof(atomic_size_t);
    bool is_ro = (transactionId % 2) == 0;
    if (is_ro){
        if(toPrint) {
            printf("Reading word| Is Read Only\n");
        }
        if (dualMem->isAValid) {
            memcpy(target, dualMem->copyA + offset, align);
        } else {
            memcpy(target, dualMem->copyB + offset, align);
        }
        return true;
    } else {
        // has been written in current epoch
        if (atomic_load(&dualMem->wasWritten)) {
            if (atomic_load(dualMem->accessed + varIdx) == transactionId) { //in access set
                //read
                if (!dualMem->isAValid) {
                    memcpy(target, dualMem->copyA + offset, align);
                } else {
                    memcpy(target, dualMem->copyB + offset, align);
                }
                return true;
            } else {
                if (printAborts){
                    printf("ABORT: Read aborted due to NOT RO + AND accessed by other thread \n");
                }
                return false;
            }
        } else {
            atomic_store(dualMem->accessed + varIdx, transactionId);
            if (dualMem->isAValid) {
                memcpy(target, dualMem->copyA + offset, align);
            } else {
                memcpy(target, dualMem->copyB + offset, align);
            }
            return true;
        }
    }
}
bool write_word( struct dualMem* dualMem, size_t index, void const* source, size_t offset, size_t align, size_t transactionId) {
    if (toPrint) {
        printf("Writing word for transaction, with index %d\n", index);
    }

    size_t writeOffset = index*align;
    //accesed word index
    size_t varIdx = offset/align + index;

    //printf("Access index = %lu\n", varIdx);
    if (atomic_load(&dualMem->wasWritten)){
        if (toPrint) {
            printf("Word was written in this epoch\n");
        }
        size_t tId = atomic_load(dualMem->accessed + varIdx);
        if (toPrint) {
            printf("Transaction ID of access = %d, our ID = %lu, index = %lu\n", tId, transactionId, index+offset);
        }
        if (tId == transactionId || tId == 0) {
            if (toPrint) {
                printf("Word was not accessed before, or accessed by this Thread\n");
            }
            if (tId == 0){
                atomic_store(dualMem->accessed + varIdx, transactionId);
            }
            printf("Transaction writing %lu\n", transactionId);
            if (!dualMem->isAValid) {
                memcpy(dualMem->copyA + offset + writeOffset, source + writeOffset, align);
            } else {
                memcpy(dualMem->copyB + offset + writeOffset, source + writeOffset, align);
            }
            return true;
        } else {
            //size_t tId = atomic_load(dualMem->accessed + offset + index);
            if (printAborts) {
                printf("ABORT: Write aborted due to WORD WAS ACCESSED BY OTHER TRANSACTIOn, US = %d, THEM = %d, IDX = %llu \n", transactionId, tId, varIdx);
            }
            if (varIdx == 2305843009213693943){
                raise(SIGTRAP);
            }
            return false;
        }
    } else {
        if (true) {
            printf("NO word was written in this epoch\n");
        }
        size_t expected = 0;
        if (transactionId == 256) {
            printf("WTF NX\n");
        }
        if (!atomic_compare_exchange_strong(dualMem->accessed + varIdx, &expected, transactionId)) {
            if (printAborts) {
                size_t them = atomic_load(dualMem->accessed + varIdx);
                printf("ABORT: WRITE word was accessed before by: %d, us: %d \n", them, transactionId);
            }
            return false;
        } else {
            atomic_fetch_or(&dualMem->wasWritten, true);
            if (true) {
                printf("First to access word is transaction = %d\n", transactionId);
            }
            if (!dualMem->isAValid) {
                memcpy(dualMem->copyA + offset, source + writeOffset, align);
            } else {
                memcpy(dualMem->copyB + offset, source + writeOffset, align);
            }

            return true;
        }
    }
    return true;
}

void cleanup_read(struct batch* self, struct dualMem* dualMem,void* target, const void* source, size_t size, size_t reachedIndex, size_t align){
    if (leave(self,dualMem,size,align)) {
        lock_release(&(self->lock));
    }
}

void cleanup_write(struct batch* self, struct dualMem* dualMem,void* target, const void* source, size_t size, size_t reachedIndex, size_t align){
    //
    if (leave(self,dualMem,size,align)) {
        printf("Transaction aborted cleaning up writes...\n");
        size_t words = size / align;
        //memset(dualMem->wasWritten, 0, words * sizeof(atomic_int));
        atomic_store(&dualMem->wasWritten, 0);
        memset(dualMem->accessed, 0, words * sizeof(atomic_size_t));
        lock_release(&(self->lock));
    }
}