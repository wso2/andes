package org.dna.mqtt.moquette.server.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.dna.mqtt.moquette.server.Constants;
import org.dna.mqtt.moquette.server.ServerChannel;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author andrea
 */
public class NettyChannel implements ServerChannel {
    
    private ChannelHandlerContext m_channel;
    
    private Map<Object, AttributeKey<Object>> m_attributesKeys = new HashMap<Object, AttributeKey<Object>>();
    
    private static final AttributeKey<Object> ATTR_KEY_KEEPALIVE = new AttributeKey<Object>(Constants.KEEP_ALIVE);
    private static final AttributeKey<Object> ATTR_KEY_CLEANSESSION = new AttributeKey<Object>(Constants.CLEAN_SESSION);
    private static final AttributeKey<Object> ATTR_KEY_CLIENTID = new AttributeKey<Object>(Constants.ATTR_CLIENTID);

    NettyChannel(ChannelHandlerContext ctx) {
        m_channel = ctx;
        m_attributesKeys.put(Constants.KEEP_ALIVE, ATTR_KEY_KEEPALIVE);
        m_attributesKeys.put(Constants.CLEAN_SESSION, ATTR_KEY_CLEANSESSION);
        m_attributesKeys.put(Constants.ATTR_CLIENTID, ATTR_KEY_CLIENTID);
    }

    public Object getAttribute(Object key) {
        Attribute<Object> attr = m_channel.attr(mapKey(key));
        return attr.get();
    }

    public void setAttribute(Object key, Object value) {
        Attribute<Object> attr = m_channel.attr(mapKey(key));
        attr.set(value);
    }
    
    private synchronized AttributeKey<Object> mapKey(Object key) {
        if (!m_attributesKeys.containsKey(key)) {
            throw new IllegalArgumentException("mapKey can't find a matching AttributeKey for " + key);
        }
        return m_attributesKeys.get(key);
    }

    public void setIdleTime(int idleTime) {
        if (m_channel.pipeline().names().contains("idleStateHandler")) {
            m_channel.pipeline().remove("idleStateHandler");
        }
        if (m_channel.pipeline().names().contains("idleEventHandler")) {
            m_channel.pipeline().remove("idleEventHandler");
        }
        m_channel.pipeline().addFirst("idleStateHandler", new IdleStateHandler(0, 0, idleTime));
        m_channel.pipeline().addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
    }

    public void close(boolean immediately) {
        m_channel.close();
    }

    public void write(Object value) {
        m_channel.writeAndFlush(value);
    }
    
}
