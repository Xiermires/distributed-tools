package org.distributed.dht;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.distributed.conduit.ConduitFactory;
import org.distributed.conduit.ConduitPool;
import org.distributed.conduit.NioGroupFactory;
import org.distributed.conduit.TCPConduit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.hash.Hashing;

public class TestNode {

    private static Node first = null;
    private static ConduitPool pool = null;

    @BeforeClass
    public static void pre() throws Exception {
	final ConduitFactory<String> factory = (url) -> {
	    try {
		final String[] hostPort = url.split(":");
		return new TCPConduit(hostPort[0], Integer.valueOf(hostPort[1]), NioGroupFactory.minimal);
	    } catch (Exception e) {
		throw new IllegalArgumentException("Cannot create. Expected format 'hostname:port'", e);
	    }
	};
	pool = new ConduitPool(factory);
	first = new Node("127.0.0.1", 19999, pool);
	first.start();
    }

    @AfterClass
    public static void post() throws Exception {
	first.stop();
    }

    @Test
    public void testJoinNode() throws InterruptedException, ExecutionException {
	final Random rand = new Random();
	final List<Node> nodes = new ArrayList<>();
	try {
	    for (int i = 0; i < 10; i++) {
		final Node node = new Node("127.0.0.1", 10000 + rand.nextInt(20000), pool).start();
		node.join(first);
		nodes.add(node);
	    }

	    first.sync();
	    Node next = first;
	    boolean cycle = false;
	    int count = 0;
	    do {
		final int cmp = next.getId().compareTo(next.getNext().getId());
		if (cmp > 0 && cycle) {
		    fail("Invalid sequence. next.id < curr.id at least twice.");
		} else if (cmp > 0) {
		    cycle = true; // found cycle
		}
		next = next.getNext();
		count++;
	    } while (!first.equals(next.sync()));
	    assertThat(count, is(11));
	} finally {
	    for (Node node : nodes) {
		node.stop();
	    }
	}
    }

    @Test
    public void testPutGet() throws InterruptedException, ExecutionException {
	final List<Node> nodes = new ArrayList<>();
	try {
	    for (int i = 0; i < 5; i++) {
		final Node node = new Node("127.0.0.1", 20000 + i, pool).start();
		node.join(first);
		nodes.add(node);
	    }

	    // put the value into some node storage
	    final BigInteger key = Keys.of("this is a key");
	    first.put(key, "this is a value");
	    assertThat(first.get(key, String.class), is("this is a value"));
	    assertThat(first.cache.getIfPresent(key), is(nullValue()));
	} finally {
	    for (Node node : nodes) {
		node.stop();
	    }
	}
    }

    @Test
    public void testPerformanceUsingFingers() throws InterruptedException, ExecutionException {
	final List<Node> nodes = new ArrayList<>();
	try {
	    for (int i = 0; i < 25; i++) {
		final Node node = new Node("127.0.0.1", 20000 + i, pool).start();
		node.join(first);
		nodes.add(node);
	    }

	    // Update the fingers of the first node only
	    Topology.updateFingers(first);

	    for (int i = 0; i < 1000; i++) {
		@SuppressWarnings("deprecation")
		final BigInteger key = new BigInteger(Hashing.sha1().hashInt(i).toString(), 16);
		first.put(key, 1000 - i);
	    }

	    for (int i = 0; i < 1000; i++) {
		@SuppressWarnings("deprecation")
		final BigInteger key = new BigInteger(Hashing.sha1().hashInt(i).toString(), 16);
		assertThat(first.get(key, Integer.class), is(1000 - i));
	    }
	} finally {
	    for (Node node : nodes) {
		node.stop();
	    }
	}
    }

    @Test
    public void testPerformance() throws InterruptedException, ExecutionException {
	final List<Node> nodes = new ArrayList<>();
	try {
	    for (int i = 0; i < 25; i++) {
		final Node node = new Node("127.0.0.1", 20000 + i, pool).start();
		node.join(first);
		nodes.add(node);
	    }

	    for (int i = 0; i < 1000; i++) {
		@SuppressWarnings("deprecation")
		final BigInteger key = new BigInteger(Hashing.sha1().hashInt(i).toString(), 16);
		first.put(key, 1000 - i);
	    }

	    for (int i = 0; i < 1000; i++) {
		@SuppressWarnings("deprecation")
		final BigInteger key = new BigInteger(Hashing.sha1().hashInt(i).toString(), 16);
		assertThat(first.get(key, Integer.class), is(1000 - i));
	    }
	} finally {
	    for (Node node : nodes) {
		node.stop();
	    }
	}
    }
}
