package org.distributed.holepunch;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.concurrent.ExecutionException;

import org.distributed.conduit.UDPConduit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMediator {

    private Mediator mediator = null;

    @Before
    public void pre() throws InterruptedException {
	mediator = new Mediator("127.0.0.1", 20000);
    }

    @After
    public void post() {
	mediator.close();
    }

    @Test
    public void testMediator() throws InterruptedException, ExecutionException {
	UDPConduit c1 = null;
	UDPConduit c2 = null;
	try {
	    c1 = new UDPConduit("127.0.0.1", 20000, true);
	    c2 = new UDPConduit("127.0.0.1", 20000, true);
	    c1.send(Message.hello());
	    c2.send(Message.hello());

	    final Integer p1 = c1.send(Message.requestPort("127.0.0.1"), Integer.class).get();
	    final Integer p2 = c2.send(Message.requestPort("127.0.0.1"), Integer.class).get();

	    assertThat(p1, is(not(nullValue())));
	    assertThat(p2, is(not(nullValue())));
	    assertThat(p1, is(not(p2)));
	} finally {
	    c1.close();
	    c2.close();
	}
    }
}
