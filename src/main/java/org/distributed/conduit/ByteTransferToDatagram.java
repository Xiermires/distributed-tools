package org.distributed.conduit;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.net.DatagramPacket;
import java.util.List;

public class ByteTransferToDatagram extends MessageToMessageDecoder<ByteTransfer> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteTransfer msg, List<Object> out) throws Exception {
	final byte[] bytes = msg.toByteArray();
	ctx.writeAndFlush(new DatagramPacket(bytes, bytes.length));
    }
}
