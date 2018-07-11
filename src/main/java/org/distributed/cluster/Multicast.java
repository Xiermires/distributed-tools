package org.distributed.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Multicast {

    private static final int MAX_BYTES = 1024;

    /**
     * Joins a multicast group and consumes any incoming messages.
     * <p>
     * Any messages exceeding {@value #MAX_BYTES} bytes are truncated.
     */
    public static Closeable join(InetSocketAddress address, Consumer<byte[]> consumer) throws IOException {
	final MulticastListener multicastListener = new MulticastListener(address, consumer);
	multicastListener.start();
	return multicastListener;
    }

    /**
     * Creates a multicast publisher at the given address & port.
     */
    public static MulticastPublisher publisher(InetSocketAddress address) throws IOException {
	return new MulticastPublisher(address);
    }

    static class MulticastListener extends Thread implements Closeable {

	private static final Logger log = LoggerFactory.getLogger(MulticastListener.class);

	private final MulticastSocket socket;
	private final Consumer<byte[]> consumer;
	private boolean active = true;

	MulticastListener(InetSocketAddress address, Consumer<byte[]> consumer) throws IOException {
	    this.socket = new MulticastSocket(address.getPort());
	    socket.joinGroup(address.getAddress());
	    this.consumer = consumer;
	}

	@Override
	public void run() {
	    final byte[] buffer = new byte[MAX_BYTES];

	    while (active) {
		final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
		try {
		    socket.receive(packet);
		    consumer.accept(buffer);
		} catch (IOException e) {
		    log.error("Cannot consume packet.");
		}
	    }
	}

	@Override
	public void close() {
	    socket.close();
	    active = false;
	}
    }

    public static class MulticastPublisher implements Closeable {

	private final DatagramSocket socket;
	private final InetSocketAddress address;

	MulticastPublisher(InetSocketAddress address) throws SocketException {
	    this.socket = new DatagramSocket();
	    this.address = address;
	}

	/**
	 * Publish the buffer to the multicast group.
	 * <p>
	 * The buffer is truncated if it exceeds {@value Multicast#MAX_BYTES}.
	 */
	public void publish(byte[] buffer) throws IOException {
	    final int length = Math.min(MAX_BYTES, buffer.length);
	    final DatagramPacket packet = new DatagramPacket(buffer, length, address.getAddress(), address.getPort());
	    socket.send(packet);
	}

	@Override
	public void close() {
	    socket.close();
	}
    }
}
