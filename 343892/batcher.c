//
// Created by Liudvikas Lazauskas on 26.11.21.
//

#include "batcher.h"
bool toPrint = false;
bool printThreads = false;
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

bool commit(struct dualMem* dualMem ,size_t size, size_t align, tx_t tx) {
    if (printThreads) {
        printf("Commiting transaction...\n");
    }

    size_t words = size/align;
    size_t varIdx;

    for (size_t i= 0; i<words; i++){
        varIdx = i*align;
        struct word* _word = (struct word*) (dualMem->words + varIdx);
        if (atomic_load(&_word->accessed) == tx){
            memcpy(_word->validCopy, _word->writeCopy, align);
        }
    }

    return true;
}

bool read_word(struct word* _word, size_t index, size_t offset, void* target, size_t align, size_t transactionId) {
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
        //TODO fix target address in write
        memcpy(target, _word->validCopy, align);
        return true;
    } else {
        // has been written in current epoch
        size_t writtenBy = atomic_load(&_word->wasWritten);
        if (writtenBy>0) {
            size_t acc = atomic_load(&_word->accessed);
            if (acc == transactionId) { //in access set
                //read
                memcpy(target, _word->writeCopy, align);
                return true;
            } else {
                if (printAborts){
                    printf("ABORT: Read aborted due to NOT RO + AND accessed by other transaction Them : %lu | Us : %lu.... Written by %lu \n", acc, transactionId,writtenBy);
                }
                return false;
            }
        } else {
            size_t expected = 0;
            atomic_compare_exchange_strong(&_word->accessed , &expected, transactionId);
            memcpy(target, _word->validCopy, align);
            return true;
        }
    }
}
bool write_word( struct batch* self, struct word* _word, size_t index, void const* source, size_t offset, size_t align, size_t transactionId) {
    if (true) {
        printf("Writing word for transaction, with index %d\n", index);
    }

    size_t writeOffset = index*align;
    //accesed word index
    size_t varIdx = offset/align + index;

    //printf("Access index = %lu\n", varIdx);
    if (atomic_load(&(_word->wasWritten))>0){
        if (toPrint) {
            printf("Word was written in this epoch\n");
        }
        size_t tId = atomic_load(&(_word->accessed));
        if (toPrint) {
            printf("Transaction ID of access = %lu, our ID = %lu, index = %lu\n", tId, transactionId, index+offset);
        }
        if (tId == transactionId) {
            if (toPrint) {
                printf("Word was not accessed before, or accessed by this Thread\n");
            }
            if (tId == 0){
                atomic_store(&(_word->accessed), transactionId);
            }
            if (toPrint) {
                printf("Transaction writing %lu\n", transactionId);
            }
            //TODO fix source addr
            memcpy(_word->writeCopy, source, align);
            return true;
        } else {
            //size_t tId = atomic_load(dualMem->accessed + offset + index);
            if (printAborts) {
                printf("ABORT: Write aborted due to WORD WAS ACCESSED BY OTHER TRANSACTIOn, US = %d, THEM = %d,\n"
                       " IDX = %llu, Align %llu, Offset %llu, EPOCH = %lu \n", transactionId, tId, varIdx, align, offset,
                       get_epoch(self));
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
        if (transactionId == 256) {
            printf("WTF NX\n");
        }
        // TODO: consult this palce
        size_t acc = atomic_load(&(_word->accessed));
        if (acc != transactionId && acc != 0) {
            if (printAborts) {
                printf("ABORT: WRITE word was accessed before by: %lu, us: %lu, epoch = %lu\n", acc, transactionId,
                       get_epoch(self));
            }
            return false;
        } else {
            size_t expected = 0;
            atomic_compare_exchange_strong(&(_word->accessed), &expected, transactionId);
            atomic_store(&(_word->wasWritten), transactionId);
            if (toPrint) {
                printf("First to access word is transaction = %d\n", transactionId);
            }
            memcpy(_word->writeCopy, source, align);
            return true;
        }
    }
    return true;
}

void cleanup_read(struct batch* self, struct dualMem* dualMem,void* target, const void* source, size_t size, size_t reachedIndex, size_t align){
    if (leave(self,dualMem,size,align)) {
        cleanup(self, dualMem, size, align);
    }
}

void cleanup_write(struct batch* self, struct dualMem* dualMem,void* target, const void* source, size_t size, size_t reachedIndex, size_t align){
    if (reachedIndex>0){
        //memset everything in accessed to 0
        for (size_t i=0; i<=reachedIndex; i++) {
            atomic_store(&(dualMem->words+i*align)->accessed, 0);
        }
    }
    if (leave(self,dualMem,size,align)) {
        //printf("Transaction aborted cleaning up writes...\n");
        cleanup(self, dualMem, size, align);
    }
}

void cleanup(struct batch* self, struct dualMem* dualMem, size_t size, size_t align){
    //printf("Cleaning up\n");

    size_t noWords = size/align;

    for (size_t i=0; i<noWords; i++) {
        atomic_store(&(dualMem->words+i*align)->wasWritten, 0);
        atomic_store(&(dualMem->words+i*align)->accessed, 0);
    }

    //printf("Broadcasting awake \n");
    pthread_cond_broadcast(&(self->lock.cv));
    //new condition for new epoch
    lock_release(&(self->lock));
}