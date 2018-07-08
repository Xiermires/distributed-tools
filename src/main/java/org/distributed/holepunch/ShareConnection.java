package org.distributed.holepunch;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.List;

import org.dev.serialize.impl.KryoSerializer;
import org.distributed.conduit.ByteTransfer;
import org.distributed.conduit.UDPConduit;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

public class ShareConnection extends SimpleChannelInboundHandler<ByteTransfer> {

    private final KryoSerializer serializer = new KryoSerializer();
    private final ListMultimap<String, Integer> lookup = Multimaps
	    .synchronizedListMultimap(LinkedListMultimap.create());

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteTransfer msg) throws Exception {
	final Message message = serializer.deserialize(msg.payload, Message.class);
	switch (message.getType()) {
	case HELLO:
	    lookup.put(msg.getSenderHostname(), msg.getSenderPort());
	    break;
	case REQUEST_PORT:
	    final List<Integer> ports = lookup.get(message.getHostname());
	    final Integer port;
	    synchronized (lookup) {
		if (!ports.isEmpty()) {
		    port = ports.remove(0);
		} else {
		    port = null;
		}
	    }
	    try (UDPConduit conduit = new UDPConduit(msg.getSenderHostname(), msg.getSenderPort(), false)) {
		conduit.send(new ByteTransfer(msg.id, serializer.serialize(port)));
	    }
	    break;
	case FREE_PORT:
	    lookup.put(message.getHostname(), message.getPort());
	    break;
	}
    }
}
