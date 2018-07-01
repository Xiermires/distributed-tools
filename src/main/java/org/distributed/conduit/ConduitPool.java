package org.distributed.conduit;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.dev.io.Closeables;
import org.dev.shutdown.ShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConduitPool implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ConduitPool.class);

    private final EvictionWorker evicter;
    private final ConduitFactory<String> factory;
    private final Map<String, TimedConduit> pool = new ConcurrentHashMap<>();

    public ConduitPool(ConduitFactory<String> factory) {
	this.evicter = new EvictionWorker(pool);
	this.factory = factory;
	evicter.start();
	ShutdownManager.closeOnShutdown(this);
    }

    public Conduit getOrCreate(String url) {
	TimedConduit channel = pool.get(url);
	if (channel == null) {
	    channel = new TimedConduit(new PooledConduit(url, factory.newConduit(url)));
	    pool.put(url, channel);
	}
	return channel.conduit;
    }

    void tick(String url) {
	final TimedConduit timedConduit = pool.get(url);
	timedConduit.tick();
    }

    @Override
    public void close() throws IOException {
	evicter.active = false;
	for (TimedConduit value : pool.values()) {
	    try {
		value.conduit.close();
	    } catch (Exception e) {
		log.error("Cannot close.");
	    }
	}
    }
    
    public synchronized void clear() {
	Closeables.closeSilently(this);
	pool.clear();
    }

    private final static class TimedConduit {

	private final Conduit conduit;
	private long touchTime;

	TimedConduit(Conduit conduit) {
	    this.conduit = conduit;
	    this.touchTime = System.currentTimeMillis();
	}
	
	void tick() {
	    touchTime = System.currentTimeMillis();
	}
    }

    static class EvictionWorker extends Thread {

	private static final Logger log = LoggerFactory.getLogger(EvictionWorker.class);

	boolean active = true;
	private final Map<String, TimedConduit> pool;

	public EvictionWorker(Map<String, TimedConduit> pool) {
	    this.pool = pool;
	    this.setName("EvictionWorker");
	}

	@Override
	public void run() {
	    while (active) {
		final long now = System.currentTimeMillis();
		for (Entry<String, TimedConduit> entry : pool.entrySet()) {
		    if (now - entry.getValue().touchTime > 5 * 60 * 1000) {
			try {
			    final TimedConduit timedConduit = pool.remove(entry.getKey());
			    timedConduit.conduit.close();
			} catch (Exception e) {
			    log.error("Cannot close.", e);
			}
		    }
		}
		try {
		    Thread.sleep(5000);
		} catch (InterruptedException e) {
		    log.error("Unexpected interruption.", e);
		    Thread.interrupted(); // clean interrupt state
		}
	    }
	}
    }
}
