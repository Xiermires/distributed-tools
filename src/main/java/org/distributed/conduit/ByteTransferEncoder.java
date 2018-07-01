package org.distributed.conduit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class ByteTransferEncoder extends MessageToByteEncoder<ByteTransfer> {

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteTransfer msg, ByteBuf out) throws Exception {
	out.writeInt(msg.id);
	out.writeInt(msg.payload.length);
	out.writeBytes(msg.payload);
    }
}