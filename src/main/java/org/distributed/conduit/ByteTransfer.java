package org.distributed.conduit;

public class ByteTransfer {
    
    public final int id;
    public final byte[] payload;

    public ByteTransfer(int id, byte[] payload) {
	this.id = id;
	this.payload = payload;
    }
}