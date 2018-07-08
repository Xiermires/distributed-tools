package org.distributed.network;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.distributed.conduit.ByteTransfer;
import org.distributed.conduit.UDPConduit;

public class UDPEcho extends SimpleChannelInboundHandler<ByteTransfer> {

    private final long sleep;

    public UDPEcho() {
	this.sleep = -1;
    }

    public UDPEcho(long sleep) {
	this.sleep = sleep;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, final ByteTransfer msg) throws Exception {
	if (sleep > 0) {
	    Thread.sleep(sleep);
	}
	try (UDPConduit conduit = new UDPConduit(msg.getSenderHostname(), msg.getSenderPort(), false)) {
	    conduit.send(msg);
	}
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
	cause.printStackTrace();
	ctx.close();
    }

}
