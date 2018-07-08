package org.distributed.conduit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Function;

import org.dev.future.TransformableFuture;
import org.dev.serialize.Serializer;
import org.dev.serialize.impl.KryoSerializer;

public class TCPConduit extends AbstractConduit<ByteTransfer, SocketChannel> implements Conduit {

    private final Channel channel;
    private final EventLoopGroup group;

    // the default message serializer
    private Serializer serializer = new KryoSerializer();

    // ongoing messages
    protected final Map<Integer, CompletableFuture<byte[]>> ongoing = new ConcurrentHashMap<>();

    // counter to identify messages
    private int id = 0;
    
    public TCPConduit(String host, int port) throws InterruptedException {
	this(host, port, NioGroupFactory.defau1t);
    }

    public TCPConduit(String host, int port, NioGroupFactory factory) throws InterruptedException {
	super(host, port, Protocol.TCP);
	group = factory.createParentGroup();

	final Bootstrap bootstrap = new Bootstrap();
	bootstrap.group(group) //
		.channel(NioSocketChannel.class) //
		.handler(this);

	channel = bootstrap.connect(hostname, port).sync().channel();
    }

    @Override
    protected SimpleChannelInboundHandler<ByteTransfer> getHandler() {
	return new Answer();
    }
    
    @Override
    public void send(Object message) {
	if (message instanceof ByteTransfer) {
	    channel.writeAndFlush(message);
	} else {
	    final ByteTransfer transfer = new ByteTransfer(-1, serializer.serialize(message));
	    channel.writeAndFlush(transfer);
	}
    }

    @Override
    public <E> Future<E> send(Object message, Class<E> type) {
	final int currentId = id++;
	final CompletableFuture<byte[]> answer = new CompletableFuture<>();
	ongoing.put(currentId, answer);
	final ByteTransfer transfer = new ByteTransfer(currentId, serializer.serialize(message));
	channel.writeAndFlush(transfer);
	return new TransformableFuture<>(answer, f -> serializer.deserialize(f, type));
    }

    @Override
    public <E> Future<E> send(Object message, Function<byte[], E> transformer) {
	final int currentId = id++;
	final CompletableFuture<byte[]> answer = new CompletableFuture<>();
	ongoing.put(currentId, answer);
	final ByteTransfer transfer = new ByteTransfer(currentId, serializer.serialize(message));
	channel.writeAndFlush(transfer);
	return new TransformableFuture<byte[], E>(answer, transformer);
    }

    @Override
    public void close() {
	group.shutdownGracefully();
	channel.close();
    }

    class Answer extends SimpleChannelInboundHandler<ByteTransfer> {

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, final ByteTransfer msg) throws Exception {
	    final CompletableFuture<byte[]> answer = ongoing.get(msg.id);
	    if (answer != null) {
		answer.complete(msg.payload);
		ongoing.remove(msg.id);
	    }
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
	    for (CompletableFuture<byte[]> pending : ongoing.values()) {
		pending.completeExceptionally(new IllegalStateException("Broken transfer.", cause));
	    }
	    close();
	}
    }
}