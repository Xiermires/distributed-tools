package org.distributed.holepunch;

public class Message {

    public static enum MessageType {
	/**
	 * A peer makes itself known to the mediator.
	 */
	HELLO, //
	/**
	 * A peer request an available port for a hostname.
	 */
	REQUEST_PORT, //
	/**
	 * A peer requests to be forgotten by the mediator.
	 */
	FREE_PORT
    };

    private MessageType type;
    private String hostname;
    private int port;

    public static Message hello() {
	final Message message = new Message();
	message.type = MessageType.HELLO;
	return message;
    }

    public static Message requestPort(String hostname) {
	final Message message = new Message();
	message.type = MessageType.REQUEST_PORT;
	message.hostname = hostname;
	return message;
    }

    public static Message freePort(String hostname, int port) {
	final Message message = new Message();
	message.type = MessageType.FREE_PORT;
	message.hostname = hostname;
	message.port = port;
	return message;
    }

    public String getHostname() {
	return hostname;
    }

    public int getPort() {
	return port;
    }

    public MessageType getType() {
	return type;
    }
}
