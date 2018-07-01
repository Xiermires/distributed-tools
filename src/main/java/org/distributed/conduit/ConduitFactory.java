package org.distributed.conduit;

public interface ConduitFactory<P> {

    Conduit newConduit(P param);
}
