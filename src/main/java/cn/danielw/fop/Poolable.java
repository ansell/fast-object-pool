package cn.danielw.fop;

/**
 * @author Daniel
 */
public class Poolable<T> implements AutoCloseable {

    private final T object;
    private ObjectPool<T> pool;
    private final int partition;
    private long lastAccessTs;
    private boolean closed;

    public Poolable(T t, ObjectPool<T> pool, int partition) {
        this.object = t;
        this.pool = pool;
        this.partition = partition;
        this.lastAccessTs = System.currentTimeMillis();
    }

    public T getObject() {
        return object;
    }

    ObjectPool<T> getPool() {
        return pool;
    }

    int getPartition() {
        return partition;
    }

    void returnObject() {
        pool.returnObject(this);
    }

    public long getLastAccessTs() {
        return lastAccessTs;
    }

    void setLastAccessTs(long lastAccessTs) {
        this.lastAccessTs = lastAccessTs;
    }

    void borrow() {
    	closed = false;
    }
    
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            this.returnObject();
        }
    }
}
