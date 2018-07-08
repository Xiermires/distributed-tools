package org.distributed.conduit;

import java.io.Closeable;
import java.util.concurrent.Future;
import java.util.function.Function;

public interface Conduit extends Closeable {
    
    @Override
    void close();

    void send(Object message);
    
    <T> Future<T> send(Object message, Class<T> type);

    <T> Future<T> send(Object message, Function<byte[], T> transformer);
}
