package org.dna.mqtt.moquette.parser.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.MessageToByteEncoder;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

import java.util.HashMap;
import java.util.Map;

public class MQTTEncoder extends MessageToByteEncoder<AbstractMessage> {
    
    private Map<Byte, DemuxEncoder> m_encoderMap = new HashMap<Byte, DemuxEncoder>();
    
    public MQTTEncoder() {
       m_encoderMap.put(AbstractMessage.CONNECT, new ConnectEncoder());
       m_encoderMap.put(AbstractMessage.CONNACK, new ConnAckEncoder());
       m_encoderMap.put(AbstractMessage.PUBLISH, new PublishEncoder());
       m_encoderMap.put(AbstractMessage.PUBACK, new PubAckEncoder());
       m_encoderMap.put(AbstractMessage.SUBSCRIBE, new SubscribeEncoder());
       m_encoderMap.put(AbstractMessage.SUBACK, new SubAckEncoder());
       m_encoderMap.put(AbstractMessage.UNSUBSCRIBE, new UnsubscribeEncoder());
       m_encoderMap.put(AbstractMessage.DISCONNECT, new DisconnectEncoder());
       m_encoderMap.put(AbstractMessage.PINGREQ, new PingReqEncoder());
       m_encoderMap.put(AbstractMessage.PINGRESP, new PingRespEncoder());
       m_encoderMap.put(AbstractMessage.UNSUBACK, new UnsubAckEncoder());
       m_encoderMap.put(AbstractMessage.PUBCOMP, new PubCompEncoder());
       m_encoderMap.put(AbstractMessage.PUBREC, new PubRecEncoder());
       m_encoderMap.put(AbstractMessage.PUBREL, new PubRelEncoder());
    }
    
    @Override
    protected void encode(ChannelHandlerContext chc, AbstractMessage msg, ByteBuf bb) throws Exception {
        DemuxEncoder encoder = m_encoderMap.get(msg.getMessageType());
        if (encoder == null) {
            throw new CorruptedFrameException("Can't find any suitable decoder for message type: " + msg.getMessageType());
        }
        encoder.encode(chc, msg, bb);
    }
}
