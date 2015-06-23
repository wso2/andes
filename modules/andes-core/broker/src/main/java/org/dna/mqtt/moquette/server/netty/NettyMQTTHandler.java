package org.dna.mqtt.moquette.server.netty;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dna.mqtt.moquette.messaging.spi.IMessaging;
import org.dna.mqtt.moquette.proto.Utils;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.PingRespMessage;
import org.dna.mqtt.moquette.server.Constants;
import org.dna.mqtt.wso2.MQTTPingRequest;

import java.util.HashMap;
import java.util.Map;

import static org.dna.mqtt.moquette.proto.messages.AbstractMessage.*;

/**
 * @author andrea
 */
@Sharable
public class NettyMQTTHandler extends ChannelInboundHandlerAdapter {

    private static Log log = LogFactory.getLog(NettyMQTTHandler.class);
    private IMessaging m_messaging;
    private final Map<ChannelHandlerContext, NettyChannel> mqttChannelMapper = new HashMap<ChannelHandlerContext, NettyChannel>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        AbstractMessage msg = (AbstractMessage) message;
        if (log.isDebugEnabled()) {
            log.debug("Received a message of type " + Utils.msgType2String(msg.getMessageType()));
        }
        try {
            switch (msg.getMessageType()) {
                case CONNECT:
                case SUBSCRIBE:
                case UNSUBSCRIBE:
                case PUBLISH:
                case PUBREC:
                case PUBCOMP:
                case PUBREL:
                case DISCONNECT:
                case PUBACK:
                    NettyChannel channel = null;
                    synchronized (mqttChannelMapper) {
                        if (!mqttChannelMapper.containsKey(ctx)) {
                            mqttChannelMapper.put(ctx, new NettyChannel(ctx));
                        }
                        channel = mqttChannelMapper.get(ctx);
                    }

                    m_messaging.handleProtocolMessage(channel, msg);
                    break;
                case PINGREQ:
                    PingRespMessage pingResp = new PingRespMessage();
                    ctx.writeAndFlush(pingResp);

                    synchronized (mqttChannelMapper) {
                        channel = mqttChannelMapper.get(ctx);
                        if (null == channel) {
                            mqttChannelMapper.put(ctx, new NettyChannel(ctx));
                        }
                    }

                    MQTTPingRequest mqttPingRequest = new MQTTPingRequest();
                    mqttPingRequest.setChannelId(channel.getAttribute(Constants.ATTR_CLIENTID).toString());
                    m_messaging.handleProtocolMessage(channel, mqttPingRequest);
                    break;
            }
        } catch (Exception ex) {
            log.error("Bad error in processing the message", ex);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        NettyChannel channel = mqttChannelMapper.get(ctx);
        if(null != channel) {
            String clientID = (String) channel.getAttribute(Constants.ATTR_CLIENTID);
            m_messaging.lostConnection(clientID);
            ctx.close(/*false*/);
        }
        synchronized (mqttChannelMapper) {
            mqttChannelMapper.remove(ctx);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause){
        // We log the error and close the connection at an event where and exception is caught
        log.error(cause.getMessage(), cause);
        ctx.close();
    }

    public void setMessaging(IMessaging messaging) {
        m_messaging = messaging;
    }
}
