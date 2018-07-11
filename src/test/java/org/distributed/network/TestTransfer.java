package org.distributed.network;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.distributed.conduit.Conduit;
import org.distributed.conduit.TCPConduit;
import org.distributed.conduit.TCPServer;
import org.distributed.conduit.UDPConduit;
import org.distributed.conduit.UDPServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTransfer {

    private static TCPServer tcpServer = null;
    private static UDPServer udpServer = null;

    @BeforeClass
    public static void pre() throws InterruptedException {
	tcpServer = new TCPServer(new TCPEcho(), "127.0.0.1", 19999);
	udpServer = new UDPServer(new UDPEcho(), "127.0.0.1", 20000);
    }

    @AfterClass
    public static void post() {
	tcpServer.close();
	udpServer.close();
    }

    @Test
    public void testTcpTransfer() throws Exception {
	final List<String> expected = new ArrayList<>();
	try (Conduit conduit = new TCPConduit("127.0.0.1", 19999)) {
	    expected.add(conduit.send("One", String.class).get());
	    expected.add(conduit.send("Two", String.class).get());
	    expected.add(conduit.send("Three", String.class).get());
	}
	assertThat(expected, contains("One", "Two", "Three"));
    }

    @Test
    public void testUdpTransfer() throws Exception {
	final List<String> expected = new ArrayList<>();
	try (Conduit conduit = new UDPConduit("127.0.0.1", 20000, true)) {
	    expected.add(conduit.send("One", String.class).get());
	    expected.add(conduit.send("Two", String.class).get());
	    expected.add(conduit.send("Three", String.class).get());
	}
	assertThat(expected, contains("One", "Two", "Three"));
    }
}
