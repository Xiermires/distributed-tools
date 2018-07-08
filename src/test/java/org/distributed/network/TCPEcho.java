package org.distributed.network;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.distributed.conduit.ByteTransfer;

@Sharable
public class TCPEcho extends SimpleChannelInboundHandler<ByteTransfer> {

    private final long sleep;

    public TCPEcho() {
	this.sleep = -1;
    }

    public TCPEcho(long sleep) {
	this.sleep = sleep;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, final ByteTransfer msg) throws Exception {
	if (sleep > 0) {
	    Thread.sleep(sleep);
	}
	ctx.writeAndFlush(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
	cause.printStackTrace();
	ctx.close();
    }
}
