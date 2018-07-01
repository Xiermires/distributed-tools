package org.distributed.conduit;

import java.util.concurrent.Future;
import java.util.function.Function;

public class PooledConduit implements Conduit {

    final String url;
    private final Conduit delegate;

    PooledConduit(String url, Conduit delegate) {
	this.url = url;
	this.delegate = delegate;
    }

    @Override
    public void send(Object message) {
	delegate.send(message);
    }

    @Override
    public <T> Future<T> send(Object message, Class<T> type) {
	return delegate.send(message, type);
    }

    @Override
    public <T> Future<T> send(Object message, Function<byte[], T> transformer) {
	return delegate.send(message, transformer);
    }

    @Override
    public void close() {
    }
}
