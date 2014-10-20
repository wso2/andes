package org.dna.mqtt.moquette.parser.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MQTTDecoder extends ByteToMessageDecoder {
    
    private Map<Byte, DemuxDecoder> m_decoderMap = new HashMap<Byte, DemuxDecoder>();
    
    public MQTTDecoder() {
       m_decoderMap.put(AbstractMessage.CONNECT, new ConnectDecoder());
       m_decoderMap.put(AbstractMessage.CONNACK, new ConnAckDecoder());
       m_decoderMap.put(AbstractMessage.PUBLISH, new PublishDecoder());
       m_decoderMap.put(AbstractMessage.PUBACK, new PubAckDecoder());
       m_decoderMap.put(AbstractMessage.SUBSCRIBE, new SubscribeDecoder());
       m_decoderMap.put(AbstractMessage.SUBACK, new SubAckDecoder());
       m_decoderMap.put(AbstractMessage.UNSUBSCRIBE, new UnsubscribeDecoder());
       m_decoderMap.put(AbstractMessage.DISCONNECT, new DisconnectDecoder());
       m_decoderMap.put(AbstractMessage.PINGREQ, new PingReqDecoder());
       m_decoderMap.put(AbstractMessage.PINGRESP, new PingRespDecoder());
       m_decoderMap.put(AbstractMessage.UNSUBACK, new UnsubAckDecoder());
       m_decoderMap.put(AbstractMessage.PUBCOMP, new PubCompDecoder());
       m_decoderMap.put(AbstractMessage.PUBREC, new PubRecDecoder());
       m_decoderMap.put(AbstractMessage.PUBREL, new PubRelDecoder());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        if (!Utils.checkHeaderAvailability(in)) {
            in.resetReaderIndex();
            return;
        }
        in.resetReaderIndex();
        
        byte messageType = Utils.readMessageType(in);
        
        DemuxDecoder decoder = m_decoderMap.get(messageType);
        if (decoder == null) {
            throw new CorruptedFrameException("Can't find any suitable decoder for message type: " + messageType);
        }
        decoder.decode(ctx, in, out);
    }
}
