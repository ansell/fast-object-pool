package cn.danielw.fop;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Daniel
 */
public class ObjectPool<T> {

    protected final PoolConfig config;
    protected final ObjectFactory<T> factory;
    protected final ObjectPoolPartition<T>[] partitions;
    private final Scavenger scavenger;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public ObjectPool(PoolConfig poolConfig, ObjectFactory<T> objectFactory) {
        this.config = poolConfig;
        this.factory = objectFactory;
        this.partitions = new ObjectPoolPartition[config.getPartitionSize()];
        try {
            for (int i = 0; i < config.getPartitionSize(); i++) {
                partitions[i] = new ObjectPoolPartition<>(this, i, config, objectFactory, createBlockingQueue(poolConfig));
            }
        } catch (InterruptedException e) {
        	Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (config.getScavengeIntervalMilliseconds() > 0) {
            this.scavenger = new Scavenger();
            this.scavenger.start();
        } else {
        	this.scavenger = null;
        }
    }

    protected BlockingQueue<Poolable<T>> createBlockingQueue(PoolConfig poolConfig) {
        return new ArrayBlockingQueue<>(poolConfig.getMaxSize());
    }

    public Poolable<T> borrowObject() {
        return borrowObject(true);
    }

    public Poolable<T> borrowObject(boolean blocking) {
        for (int i = 0; i < 3; i++) { // try at most three times
            Poolable<T> result = getObject(blocking);
            if (factory.validate(result.getObject())) {
                return result;
            } else {
                this.partitions[result.getPartition()].decreaseObject(result);
            }
        }
        throw new RuntimeException("Cannot find a valid object");
    }

    private Poolable<T> getObject(boolean blocking) {
        if (shuttingDown.get()) {
            throw new IllegalStateException("Your pool is shutting down");
        }
        int partition = (int) (Thread.currentThread().getId() % this.config.getPartitionSize());
        ObjectPoolPartition<T> subPool = this.partitions[partition];
        BlockingQueue<Poolable<T>> objectQueue = subPool.getObjectQueue();
		Poolable<T> freeObject = objectQueue.poll();
        if (freeObject == null) {
            // increase objects and return one, it will return null if reach max size
            subPool.increaseObjects(1);
            try {
                if (blocking) {
                    freeObject = objectQueue.take();
                } else {
                    freeObject = objectQueue.poll(config.getMaxWaitMilliseconds(), TimeUnit.MILLISECONDS);
                    if (freeObject == null) {
                        throw new RuntimeException("Cannot get a free object from the pool");
                    }
                }
            } catch (InterruptedException e) {
            	Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        freeObject.setLastAccessTs(System.currentTimeMillis());
        return freeObject;
    }

    public void returnObject(Poolable<T> obj) {
        ObjectPoolPartition<T> subPool = this.partitions[obj.getPartition()];
        BlockingQueue<Poolable<T>> objectQueue = subPool.getObjectQueue();
        try {
			objectQueue.put(obj);
            if (Log.isDebug())
                Log.debug("return object: queue size:", objectQueue.size(),
                    ", partition id:", obj.getPartition());
        } catch (InterruptedException e) {
        	Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public int getSize() {
        int size = 0;
        for (ObjectPoolPartition<T> subPool : partitions) {
            size += subPool.getTotalCount();
        }
        return size;
    }

    public synchronized int shutdown() throws InterruptedException {
        int removed = 0;
    	if(shuttingDown.compareAndSet(false, true)) {
	        if (scavenger != null) {
	            scavenger.interrupt();
	            scavenger.join();
	        }
	        for (ObjectPoolPartition<T> partition : partitions) {
	            removed += partition.shutdown();
	        }
    	}
        return removed;
    }

    private class Scavenger extends Thread {

        @Override
        public void run() {
            int partition = 0;
            while (!ObjectPool.this.shuttingDown.get()) {
                try {
                    Thread.sleep(config.getScavengeIntervalMilliseconds());
                    partition = ++partition % config.getPartitionSize();
                    Log.debug("scavenge sub pool ",  partition);
                    partitions[partition].scavenge();
                } catch (InterruptedException ignored) {
                	Thread.currentThread().interrupt();
                }
            }
        }

    }
}
