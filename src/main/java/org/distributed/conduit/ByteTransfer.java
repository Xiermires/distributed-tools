package org.distributed.conduit;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public class ByteTransfer {

    public final int id;
    public final byte[] payload;

    private transient String senderHostname;
    private transient int senderPort;
    private transient String receiverHostname;
    private transient int receiverPort;

    public ByteTransfer(int id, byte[] payload) {
	this.id = id;
	this.payload = payload;
    }

    public static ByteTransfer fromByteBuf(ByteBuf buffer) {
	// id + length
	if (buffer.readableBytes() < 8) {
	    return null;
	}

	buffer.markReaderIndex();

	final int id = buffer.readInt();
	final int length = buffer.readInt();

	if (buffer.readableBytes() < length) {
	    buffer.resetReaderIndex();
	    return null;
	}

	final byte[] payload = new byte[length];
	buffer.readBytes(payload);
	return new ByteTransfer(id, payload);
    }

    /**
     * It creates a byte transfer from the given bytes.
     * <p>
     * If bytes represent an non-completed transfer it returns <code>null</code>
     */
    public static ByteTransfer fromByteArray(byte[] bytes) {
	final ByteBuffer buffer = ByteBuffer.wrap(bytes);
	if (bytes.length < 8)
	    return null;

	final int id = buffer.getInt();
	final int size = buffer.getInt();
	if (size <= buffer.remaining()) {
	    final byte[] payload = new byte[size];
	    buffer.get(payload);
	    return new ByteTransfer(id, payload);
	}
	return null;
    }

    public byte[] toByteArray() {
	final ByteBuffer buffer = ByteBuffer.allocate(payload.length + 2 * Integer.BYTES);
	buffer.putInt(id);
	buffer.putInt(payload.length);
	buffer.put(payload);
	return buffer.array();
    }

    public String getSenderHostname() {
	return senderHostname;
    }

    void setSenderHostname(String senderHostname) {
	this.senderHostname = senderHostname;
    }

    public int getSenderPort() {
	return senderPort;
    }

    void setSenderPort(int senderPort) {
	this.senderPort = senderPort;
    }

    public String getReceiverHostname() {
	return receiverHostname;
    }

    void setReceiverHostname(String receiverHostname) {
	this.receiverHostname = receiverHostname;
    }

    public int getReceiverPort() {
	return receiverPort;
    }

    void setReceiverPort(int receiverPort) {
	this.receiverPort = receiverPort;
    }
}