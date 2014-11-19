package org.dna.mqtt.moquette.parser.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

import java.util.List;

class PublishDecoder extends DemuxDecoder {

    private static Log log = LogFactory.getLog(PublishDecoder.class);

    @Override
    void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // log.info("decode invoked with buffer {}", in);
        if (log.isDebugEnabled()) {
            log.debug("decode invoked with buffer : " + in);
        }
        in.resetReaderIndex();
        int startPos = in.readerIndex();

        //Common decoding part
        PublishMessage message = new PublishMessage();
        if (!decodeCommonHeader(message, in)) {
            if (log.isDebugEnabled()) {
                log.info("decode ask for more data after " + in);
            }
            in.resetReaderIndex();
            return;
        }
        int remainingLength = message.getRemainingLength();
        
        //Topic name
        String topic = Utils.decodeString(in);
        if (topic == null) {
            in.resetReaderIndex();
            return;
        }
        message.setTopicName(topic);
        
        if (message.getQos() == AbstractMessage.QOSType.LEAST_ONE || 
                message.getQos() == AbstractMessage.QOSType.EXACTLY_ONCE) {
            message.setMessageID(in.readUnsignedShort());
        }
        int stopPos = in.readerIndex();
        
        //read the payload
        int payloadSize = remainingLength - (stopPos - startPos - 2) + (Utils.numBytesToEncode(remainingLength) - 1);
        if (in.readableBytes() < payloadSize) {
            in.resetReaderIndex();
            return;
        }
//        byte[] b = new byte[payloadSize];
        ByteBuf bb = Unpooled.buffer(payloadSize);
        in.readBytes(bb);
        message.setPayload(bb.nioBuffer());
        
        out.add(message);
    }
    
}
