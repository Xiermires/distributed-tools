package org.distributed.conduit;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;

import java.io.Closeable;

public abstract class AbstractConduit extends ChannelInitializer<SocketChannel> implements Closeable {

    protected final String hostname;
    protected final int port;

    protected AbstractConduit(String hostname, int port) {
	this.hostname = hostname;
	this.port = port;
    }

    protected abstract SimpleChannelInboundHandler<ByteTransfer> getHandler();

    @Override
    protected void initChannel(SocketChannel ch) {
	final ChannelPipeline pipeline = ch.pipeline();

	pipeline.addLast(new ByteTransferDecoder());
	pipeline.addLast(new ByteTransferEncoder());
	pipeline.addLast(getHandler());
    }

    public String getHostname() {
	return hostname;
    }

    public int getPort() {
	return port;
    }
}
