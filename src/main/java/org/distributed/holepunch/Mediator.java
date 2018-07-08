package org.distributed.holepunch;

import org.distributed.conduit.UDPServer;

public class Mediator extends UDPServer {
    
    public Mediator(String hostname, int port) throws InterruptedException {
	super(new ShareConnection(), hostname, port);
    }
}
