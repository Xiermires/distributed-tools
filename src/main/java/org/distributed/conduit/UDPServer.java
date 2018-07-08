package org.distributed.conduit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDPServer extends AbstractConduit<ByteTransfer, DatagramChannel> {

    private static final Logger log = LoggerFactory.getLogger(TCPServer.class);

    private final SimpleChannelInboundHandler<ByteTransfer> handler;

    private ChannelFuture channelFuture;
    private EventLoopGroup group;

    public UDPServer( //
	    SimpleChannelInboundHandler<ByteTransfer> handler, //
	    String hostname, //
	    int port) throws InterruptedException {
	this(handler, hostname, port, NioGroupFactory.defau1t);
    }

    public UDPServer( //
	    SimpleChannelInboundHandler<ByteTransfer> handler, //
	    String hostname, //
	    int port, //
	    NioGroupFactory factory) throws InterruptedException {

	super(hostname, port, Protocol.UDP);
	this.handler = handler;

	group = factory.createChildrenGroup();

	final Bootstrap server = new Bootstrap();
	server.group(group) //
		.channel(NioDatagramChannel.class) //
		.handler(this);

	this.channelFuture = server.bind(hostname, port).sync();
    }

    @Override
    public void close() {
	try {
	    group.shutdownGracefully();
	    channelFuture.channel().closeFuture().sync();
	} catch (InterruptedException e) {
	    log.warn("Thread interrupted while trying to close server.", e);
	    Thread.interrupted(); // clean interrupt status
	}
    }

    @Override
    protected SimpleChannelInboundHandler<ByteTransfer> getHandler() {
	return handler;
    }
}
