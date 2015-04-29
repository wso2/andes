package org.dna.mqtt.moquette.parser.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

class PublishEncoder extends DemuxEncoder<PublishMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, PublishMessage message, ByteBuf out) {
        if (message.getQos() == AbstractMessage.QOSType.RESERVED) {
            throw new IllegalArgumentException("Found a message with RESERVED Qos");
        }
        if (message.getTopicName() == null || message.getTopicName().isEmpty()) {
            throw new IllegalArgumentException("Found a message with empty or null topic name");
        }
        
        ByteBuf variableHeaderBuff = ctx.alloc().buffer(2);
        ByteBuf encodeBuffer = null;
        try {
            variableHeaderBuff.writeBytes(Utils.encodeString(message.getTopicName()));
            if (message.getQos() == AbstractMessage.QOSType.LEAST_ONE || 
                message.getQos() == AbstractMessage.QOSType.EXACTLY_ONCE ) {
                if (message.getMessageID() == null) {
                    throw new IllegalArgumentException("Found a message with QOS 1 or 2 and not MessageID setted");
                }
                variableHeaderBuff.writeShort(message.getMessageID());
            }
            variableHeaderBuff.writeBytes(message.getPayload());
            int variableHeaderSize = variableHeaderBuff.readableBytes();

            byte flags = Utils.encodeFlags(message);

            encodeBuffer = ctx.alloc().buffer(2 + variableHeaderSize);
            encodeBuffer.writeByte(AbstractMessage.PUBLISH << 4 | flags);
            encodeBuffer.writeBytes(Utils.encodeRemainingLength(variableHeaderSize));
            encodeBuffer.writeBytes(variableHeaderBuff);
            out.writeBytes(encodeBuffer);
        } finally {
            variableHeaderBuff.release();
            if(encodeBuffer != null) {
                encodeBuffer.release();
            }
        }
    }
    
}
