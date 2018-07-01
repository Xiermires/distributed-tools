package org.distributed.conduit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class ByteTransferDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

	// id + length
	if (in.readableBytes() < 8) {
	    return;
	}

	in.markReaderIndex();

	final int id = in.readInt();
	final int length = in.readInt();

	if (in.readableBytes() < length) {
	    in.resetReaderIndex();
	    return;
	}

	final byte[] payload = new byte[length];
	in.readBytes(payload);
	out.add(decodeTransfer(id, payload));
    }

    protected ByteTransfer decodeTransfer(int id, byte[] payload) {
	return new ByteTransfer(id, payload);
    }
}
