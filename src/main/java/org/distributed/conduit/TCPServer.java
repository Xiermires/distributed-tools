package org.distributed.conduit;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TCPServer extends AbstractConduit<ByteTransfer, SocketChannel> {

    private static final Logger log = LoggerFactory.getLogger(TCPServer.class);

    private final SimpleChannelInboundHandler<ByteTransfer> handler;

    private ChannelFuture channelFuture;
    private EventLoopGroup parentGroup;
    private EventLoopGroup childrenGroup;

    public TCPServer( //
	    SimpleChannelInboundHandler<ByteTransfer> handler, //
	    String hostname, //
	    int port) throws InterruptedException {
	this(handler, hostname, port, NioGroupFactory.defau1t);
    }

    public TCPServer( //
	    SimpleChannelInboundHandler<ByteTransfer> handler, //
	    String hostname, //
	    int port, //
	    NioGroupFactory factory) throws InterruptedException {

	super(hostname, port, Protocol.TCP);
	this.handler = handler;

	parentGroup = factory.createParentGroup();
	childrenGroup = factory.createChildrenGroup();

	final ServerBootstrap server = new ServerBootstrap();
	server.group(parentGroup, childrenGroup) //
		.channel(NioServerSocketChannel.class) //
		.handler(new LoggingHandler(LogLevel.INFO)) //
		.childHandler(this);

	this.channelFuture = server.bind(hostname, port).sync();
    }

    @Override
    public void close() {
	try {
	    parentGroup.shutdownGracefully();
	    childrenGroup.shutdownGracefully();
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
