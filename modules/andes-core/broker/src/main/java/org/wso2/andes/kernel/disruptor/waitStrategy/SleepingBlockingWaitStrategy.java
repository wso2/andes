package org.wso2.andes.kernel.disruptor.waitStrategy;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class SleepingBlockingWaitStrategy implements WaitStrategy {
    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition;

    public SleepingBlockingWaitStrategy() {
        this.processorNotifyCondition = this.lock.newCondition();
    }

    public long waitFor(long sequence, Sequence cursorSequence, Sequence dependentSequence, SequenceBarrier barrier) throws AlertException, InterruptedException {
        if(cursorSequence.get() < sequence) {
            this.lock.lock();

            try {
                while(cursorSequence.get() < sequence) {
                    barrier.checkAlert();
                    this.processorNotifyCondition.await();
                }
            } finally {
                this.lock.unlock();
            }
        }

        long availableSequence;
        while((availableSequence = dependentSequence.get()) < sequence) {
            barrier.checkAlert();
            LockSupport.parkNanos(1L);
        }

        return availableSequence;
    }

    public void signalAllWhenBlocking() {
        this.lock.lock();

        try {
            this.processorNotifyCondition.signalAll();
        } finally {
            this.lock.unlock();
        }

    }
}
