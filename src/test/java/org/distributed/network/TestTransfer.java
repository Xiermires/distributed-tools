package org.distributed.network;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.distributed.conduit.CloseableConduit;
import org.distributed.conduit.CloseableServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTransfer {

    private static CloseableServer server = null;

    @BeforeClass
    public static void pre() throws InterruptedException {
	server = new CloseableServer(new Echo(), "127.0.0.1", 19999);
    }

    @AfterClass
    public static void post() {
	server.close();
    }

    @Test
    public void testTransfer() throws Exception {
	final List<String> expected = new ArrayList<>();
	try (CloseableConduit channel = new CloseableConduit("127.0.0.1", 19999)) {
	    expected.add(channel.send("One", String.class).get());
	    expected.add(channel.send("Two", String.class).get());
	    expected.add(channel.send("Three", String.class).get());
	}
	assertThat(expected, contains("One", "Two", "Three"));
    }
}
