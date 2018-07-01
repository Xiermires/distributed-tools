package org.distributed.dht;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.dev.serialize.impl.KryoSerializer;
import org.distributed.conduit.ByteTransfer;
import org.distributed.conduit.CloseableConduit;
import org.distributed.conduit.CloseableServer;
import org.distributed.conduit.Conduit;
import org.distributed.conduit.ConduitFactory;
import org.distributed.conduit.ConduitPool;
import org.distributed.conduit.NioGroupFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class Node {

    private static final int STANDARD_CACHE_SIZE = 100 * 1024 * 1024;

    private transient CloseableServer server;

    private final String hostname;
    private final int port;
    private final BigInteger id;

    private transient Node prev = null;
    private transient Node next = null;

    private transient NavigableMap<BigInteger, Node> fingers = Collections.synchronizedNavigableMap(new TreeMap<>());
    private transient final Weigher<BigInteger, byte[]> weigher = (key, value) -> value.length;

    @VisibleForTesting
    transient Cache<BigInteger, byte[]> cache = CacheBuilder.newBuilder() //
	    .weigher(weigher) //
	    .maximumWeight(STANDARD_CACHE_SIZE) //
	    .build();

    private transient final ConduitPool pool;

    public Node(String hostname, int port) throws InterruptedException {
	this.hostname = hostname;
	this.port = port;
	this.id = generateId(hostname, port);

	final ConduitFactory<String> factory = (url) -> {
	    try {
		final String[] hostPort = url.split(":");
		return new CloseableConduit(hostPort[0], Integer.valueOf(hostPort[1]), NioGroupFactory.minimal);
	    } catch (Exception e) {
		throw new IllegalArgumentException("Cannot create. Expected format 'hostname:port'", e);
	    }
	};
	this.pool = new ConduitPool(factory);
    }

    @VisibleForTesting
    Node(String hostname, int port, ConduitPool pool) throws InterruptedException {
	this.hostname = hostname;
	this.port = port;
	this.id = generateId(hostname, port);
	this.pool = pool;
    }

    private Node(String hostname, int port, BigInteger id, ConduitPool pool) throws InterruptedException {
	this.hostname = hostname;
	this.port = port;
	this.id = id;
	this.pool = pool;
    }

    public Node start() throws InterruptedException {
	server = new CloseableServer(new Handler(), hostname, port);
	return this;
    }

    public void stop() {
	server.close();
    }

    public void resizeCache(long size) {
	cache = CacheBuilder.newBuilder() //
		.weigher(weigher) //
		.maximumWeight(size) //
		.build();
    }

    private BigInteger generateId(String hostname, int port) {
	@SuppressWarnings("deprecation")
	final Hasher hasher = Hashing.sha1().newHasher();
	hasher.putString(hostname, Charsets.UTF_8);
	hasher.putInt(port);
	return new BigInteger(hasher.hash().toString(), 16);
    }

    @Sharable
    class Handler extends SimpleChannelInboundHandler<ByteTransfer> {

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, ByteTransfer msg) throws Exception {
	    final KryoSerializer kryo = new KryoSerializer();
	    final Message message = kryo.deserialize(msg.payload, Message.class);
	    switch (message.getType()) {
	    case SYNC_PULL:
		ctx.writeAndFlush(new ByteTransfer(msg.id, kryo.serialize(Message.dto(Node.this))));
		break;
	    case SYNC_PUSH:
		updatePrevNext(Node.this, message);
		break;
	    case PUT:
		cache.put(message.getKey(), kryo.serialize(message.getValue()));
		break;
	    case GET:
		ctx.writeAndFlush(new ByteTransfer(msg.id, cache.getIfPresent(message.getKey())));
		break;
	    }
	}
    }

    private static final KryoSerializer serializer = new KryoSerializer();
    private static final Function<byte[], Message> toMsg = bytes -> serializer.deserialize(bytes, Message.class);

    /**
     * Joins a new node into an existing network.
     */
    public void join(Node node) throws InterruptedException, ExecutionException {
	final Node[] prevNext = findEnclosingInterval(node, id);
	add(prevNext[0], prevNext[1]);
    }

    private Node[] findEnclosingInterval(Node first, BigInteger id) throws InterruptedException, ExecutionException {
	Node prev = getClosestStart(first.sync(), id), next = first.getNext();

	// Handle one node scenario
	if (next == null) {
	    return new Node[] { first, first };
	} else {
	    next.sync();
	}

	int lhs = id.compareTo(prev.id);
	int rhs = id.compareTo(next.id);
	if (lhs > 0 && rhs < 0) {
	    // lucky hit
	} else if (lhs < 0) { // search backwards
	    do {
		next = prev;
		prev = prev.sync().getPrev();
		lhs = id.compareTo(prev.id);
	    } while (lhs < 0 && prev.id.compareTo(next.id) < 0);
	} else { // search forwards
	    do {
		prev = next;
		next = next.sync().getNext();
		rhs = id.compareTo(next.id);
	    } while (rhs > 0 && prev.id.compareTo(next.id) < 0);
	}
	return new Node[] { prev, next };
    }

    // TODO recurse
    private static Node getClosestStart(Node node, BigInteger id) {
	final Entry<BigInteger, Node> floor = node.fingers.floorEntry(id);
	final Entry<BigInteger, Node> ceiling = node.fingers.ceilingEntry(id);

	if (floor != null && ceiling != null) {
	    return getClosestBetween(id, node, getClosestBetween(id, floor.getValue(), ceiling.getValue()));
	} else if (floor != null) {
	    return getClosestBetween(id, node, floor.getValue());
	} else if (ceiling != null) {
	    return getClosestBetween(id, node, ceiling.getValue());
	} else {
	    return node;
	}
    }

    private static Node getClosestBetween(BigInteger ref, Node n1, Node n2) {
	final BigInteger n1Diff = ref.subtract(n1.getId()).abs();
	final BigInteger n2Diff = ref.subtract(n2.getId()).abs();
	final int cmp = n1Diff.compareTo(n2Diff);
	if (cmp < 0) {
	    return n1;
	} else {
	    return n2;
	}
    }

    private void add(Node prev, Node next) throws InterruptedException, ExecutionException {
	setNext(next);
	setPrev(prev);
	prev.setNext(this);
	next.setPrev(this);
	prev.sync(prev);
	next.sync(next);
	sync(this);
    }

    /**
     * Sync the remote node into its proxy.
     */
    public Node sync() throws InterruptedException, ExecutionException {
	try (Conduit channel = pool.getOrCreate(hostname + ":" + port)) {
	    final Message answer = channel.send(Message.sync(), toMsg).get();
	    updatePrevNext(this, answer);
	}
	return this;
    }

    /**
     * Sync the remote node with the contents of this proxy.
     */
    public Node sync(Node node) {
	try (Conduit channel = pool.getOrCreate(hostname + ":" + port)) {
	    channel.send(Message.sync(node));
	}
	return this;
    }

    private static void updatePrevNext(Node node, Message message) throws InterruptedException {
	final String[] prev = message.getCurrPrevNext()[1] != null ? message.getCurrPrevNext()[1].split(":") : null;
	final String[] next = message.getCurrPrevNext()[2] != null ? message.getCurrPrevNext()[2].split(":") : null;
	if (prev != null) {
	    node.setPrev(new Node(prev[0], Integer.parseInt(prev[1]), new BigInteger(prev[2]), node.pool));
	}
	if (next != null) {
	    node.setNext(new Node(next[0], Integer.parseInt(next[1]), new BigInteger(next[2]), node.pool));
	}
    }

    /**
     * Puts a { key, value } pair into the storage of the closest node to the key.
     */
    public void put(BigInteger key, Object value) throws InterruptedException, ExecutionException {
	final Node closest = findClosest(key);
	try (Conduit channel = pool.getOrCreate(closest.hostname + ":" + closest.port)) {
	    channel.send(Message.put(key, value));
	}
    }

    /**
     * Resolves the closest node to the key, and checks if its storage contains the value.
     */
    public <T> T get(BigInteger key, Class<T> type) throws InterruptedException, ExecutionException {
	final Node closer = findClosest(key);
	try (Conduit channel = pool.getOrCreate(closer.hostname + ":" + closer.port)) {
	    return channel.send(Message.get(key), type).get();
	}
    }

    private Node findClosest(BigInteger key) throws InterruptedException, ExecutionException {
	final Node[] prevNext = findEnclosingInterval(this, key);
	return getClosestBetween(key, prevNext[0], prevNext[1]);
    }

    public void setFingers(Map<BigInteger, Node> fingers) {
	this.fingers.clear();
	this.fingers.putAll(fingers);
    }

    public Node getPrev() {
	return prev;
    }

    public void setPrev(Node lhs) {
	this.prev = lhs;
    }

    public Node getNext() {
	return next;
    }

    public void setNext(Node rhs) {
	this.next = rhs;
    }

    public String getHostname() {
	return hostname;
    }

    public int getPort() {
	return port;
    }

    public BigInteger getId() {
	return id;
    }

    @Override
    public int hashCode() {
	return 31 + ((id == null) ? 0 : id.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	Node other = (Node) obj;
	if (id == null) {
	    if (other.id != null)
		return false;
	} else if (!id.equals(other.id))
	    return false;
	return true;
    }
}
