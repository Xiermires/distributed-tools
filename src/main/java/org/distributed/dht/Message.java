package org.distributed.dht;

import java.math.BigInteger;

public class Message {

    public static enum MessageType {
	/**
	 * Pulls the single remote instance state into the current proxy.
	 */
	SYNC_PULL, //
	/**
	 * Pushes the current proxy state into the single remote instance.
	 */
	SYNC_PUSH, //
	/**
	 * Puts a { key, value } pair into the remote storage.
	 */
	PUT, //
	/**
	 * Gets a value from the remote storage.
	 */
	GET
    };

    private MessageType type;
    private String[] currPrevNext;

    // V get (K)
    // put (K, V)
    private BigInteger key;
    private Object value;

    public static Message sync() {
	final Message message = new Message();
	message.type = MessageType.SYNC_PULL;
	return message;
    }

    public static Message sync(Node node) {
	final Message message = dto(node);
	message.type = MessageType.SYNC_PUSH;
	return message;
    }

    public static Message dto(Node node) {
	final Message message = new Message();
	message.setNode(node);
	return message;
    }

    public static Message put(BigInteger key, Object value) {
	final Message message = new Message();
	message.type = MessageType.PUT;
	message.setKey(key);
	message.setValue(value);
	return message;
    }

    public static Object get(BigInteger key) {
	final Message message = new Message();
	message.type = MessageType.GET;
	message.setKey(key);
	return message;
    }

    public MessageType getType() {
	return type;
    }

    public void setType(MessageType type) {
	this.type = type;
    }

    public BigInteger getKey() {
	return key;
    }

    public void setKey(BigInteger key) {
	this.key = key;
    }

    public Object getValue() {
	return value;
    }

    public void setValue(Object value) {
	this.value = value;
    }

    public String[] getCurrPrevNext() {
	return currPrevNext;
    }

    public void setNode(Node node) {
	currPrevNext = new String[3];
	currPrevNext[0] = node.getHostname() + ":" + node.getPort() + ":" + node.getId();
	if (node.getPrev() != null) {
	    currPrevNext[1] = node.getPrev().getHostname() + //
		    ":" + //
		    node.getPrev().getPort() + //
		    ":" + //
		    node.getPrev().getId();
	}
	if (node.getNext() != null) {
	    currPrevNext[2] = node.getNext().getHostname() + //
		    ":" + //
		    node.getNext().getPort() + //
		    ":" + //
		    node.getNext().getId();
	}
    }
}
