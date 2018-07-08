package org.distributed.conduit;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class DatagramToByteTransfer extends MessageToMessageDecoder<DatagramPacket> {

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) throws Exception {
	final ByteTransfer transfer = ByteTransfer.fromByteBuf(msg.content());
	if (transfer != null) {
	    transfer.setSenderHostname(msg.sender().getAddress().getHostAddress());
	    transfer.setSenderPort(msg.sender().getPort());
	    transfer.setReceiverHostname(msg.recipient().getAddress().getHostAddress());
	    transfer.setReceiverPort(msg.recipient().getPort());
	    out.add(transfer);
	}
    }
}
