package org.dna.mqtt.moquette.parser.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.dna.mqtt.moquette.proto.messages.DisconnectMessage;

import java.util.List;

class DisconnectDecoder extends DemuxDecoder {

    @Override
    void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        //Common decoding part
        in.resetReaderIndex();
        DisconnectMessage message = new DisconnectMessage();
        if (!decodeCommonHeader(message, in)) {
            in.resetReaderIndex();
            return;
        }
        out.add(message);
    }
    
}
