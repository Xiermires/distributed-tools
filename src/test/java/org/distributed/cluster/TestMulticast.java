package org.distributed.cluster;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.dev.shutdown.ShutdownManager;
import org.distributed.cluster.Multicast.MulticastPublisher;
import org.junit.Test;

import com.google.common.primitives.Ints;

public class TestMulticast {

    @Test
    public void testMulticast() throws IOException, InterruptedException {
	final AtomicInteger inc = new AtomicInteger(0);

	for (int i = 0; i < 5; i++) {
	    ShutdownManager.closeOnShutdown(Multicast.join(new InetSocketAddress("233.3.3.3", 5000),
		    c -> inc.incrementAndGet()));
	}
	final InetSocketAddress address = new InetSocketAddress("233.3.3.3", 5000);
	final MulticastPublisher publisher = ShutdownManager.closeOnShutdown(Multicast.publisher(address));
	publisher.publish(Ints.toByteArray(1));
	Thread.sleep(10);
	assertThat(inc.get(), is(5));
    }
}
