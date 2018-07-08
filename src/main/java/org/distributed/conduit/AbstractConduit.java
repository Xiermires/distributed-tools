package org.distributed.conduit;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.Closeable;

public abstract class AbstractConduit<T, C extends Channel> extends ChannelInitializer<C> implements Closeable {

    public static enum Protocol {
	TCP, UDP
    };

    protected final String hostname;
    protected final int port;
    protected final Protocol protocol;

    protected AbstractConduit(String hostname, int port, Protocol protocol) {
	this.hostname = hostname;
	this.port = port;
	this.protocol = protocol;
    }

    protected abstract SimpleChannelInboundHandler<T> getHandler();

    @Override
    protected void initChannel(C ch) {
	if (protocol == Protocol.TCP) {
	    initTCPChannel(ch);
	} else {
	    initUDPChannel(ch);
	}
    }

    private void initUDPChannel(C ch) {
	final ChannelPipeline pipeline = ch.pipeline();
	pipeline.addLast(new ByteTransferDecoder());
	pipeline.addLast(new ByteTransferEncoder());
	pipeline.addLast(new ByteTransferToDatagram());
	pipeline.addLast(new DatagramToByteTransfer());
	final SimpleChannelInboundHandler<T> handler = getHandler();
	if (handler != null) {
	    pipeline.addLast(handler);
	}
    }

    private void initTCPChannel(C ch) {
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
