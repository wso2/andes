package org.dna.mqtt.moquette.server.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.dna.mqtt.commons.Constants;
import org.dna.mqtt.moquette.messaging.spi.IMessaging;
import org.dna.mqtt.moquette.parser.netty.MQTTDecoder;
import org.dna.mqtt.moquette.parser.netty.MQTTEncoder;
import org.dna.mqtt.moquette.server.ServerAcceptor;
import org.dna.mqtt.moquette.server.netty.metrics.BytesMetrics;
import org.dna.mqtt.moquette.server.netty.metrics.BytesMetricsCollector;
import org.dna.mqtt.moquette.server.netty.metrics.BytesMetricsHandler;
import org.dna.mqtt.moquette.server.netty.metrics.MessageMetrics;
import org.dna.mqtt.moquette.server.netty.metrics.MessageMetricsCollector;
import org.dna.mqtt.moquette.server.netty.metrics.MessageMetricsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.transport.ConnectionSettings;
import org.wso2.andes.transport.network.security.ssl.SSLUtil;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Properties;
import org.wso2.andes.configuration.modules.JKSStore;

/**
 * @author andrea
 */
public class NettyAcceptor implements ServerAcceptor {

    static class WebSocketFrameToByteBufDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {

        @Override
        protected void decode(ChannelHandlerContext chc, BinaryWebSocketFrame frame, List<Object> out) throws Exception {
            //convert the frame to a ByteBuf
            ByteBuf bb = frame.content();
            //System.out.println("WebSocketFrameToByteBufDecoder decode - " + ByteBufUtil.hexDump(bb));
            bb.retain();
            out.add(bb);
        }
    }

    static class ByteBufToWebSocketFrameEncoder extends MessageToMessageEncoder<ByteBuf> {

        @Override
        protected void encode(ChannelHandlerContext chc, ByteBuf bb, List<Object> out) throws Exception {
            //convert the ByteBuf to a WebSocketFrame
            BinaryWebSocketFrame result = new BinaryWebSocketFrame();
            //System.out.println("ByteBufToWebSocketFrameEncoder encode - " + ByteBufUtil.hexDump(bb));
            result.content().writeBytes(bb);
            out.add(result);
        }
    }

    abstract class PipelineInitializer {

        abstract void init(ChannelPipeline pipeline) throws Exception;
    }

    private static final Logger log = LoggerFactory.getLogger(NettyAcceptor.class);

    EventLoopGroup m_bossGroup;
    EventLoopGroup m_workerGroup;
    BytesMetricsCollector m_bytesMetricsCollector = new BytesMetricsCollector();
    MessageMetricsCollector m_metricsCollector = new MessageMetricsCollector();

    @Override
    public void initialize(IMessaging messaging, Properties props) throws IOException {
        m_bossGroup = new NioEventLoopGroup();
        m_workerGroup = new NioEventLoopGroup();

        /**
         * We leave the websockets commented for now since we do not support end to end integration with it
         */
        //initializeWebSocketTransport(messaging, props);
        //TODO need to re look into using getProperty here
        String sslTcpPortProp = props.get(Constants.SSL_PORT_PROPERTY_NAME).toString();
        String wssPortProp = props.getProperty(Constants.WSS_PORT_PROPERTY_NAME);
        Boolean sslPortEnabled = Boolean.parseBoolean(props.get(Constants.SSL_CONNECTION_ENABLED).toString());
        Boolean defaultPortEnabled = Boolean.parseBoolean(props.get(Constants.DEFAULT_CONNECTION_ENABLED).toString());

        // non-secure port will be enabled/disabled as per configuration.
        if(defaultPortEnabled) {
            initializePlainTCPTransport(messaging, props);
        } else {
            log.warn("MQTT port has disabled as per configuration.");
        }


        /* if (sslTcpPortProp != null || wssPortProp != null) {
            SslHandler sslHandler = initSSLHandler(props);
            if (sslHandler == null) {
                log.error("Can't initialize SSLHandler layer! Exiting, check your configuration of jks");
                return;
            }
            initializeSSLTCPTransport(messaging, props, sslHandler);
            initializeWSSTransport(messaging, props, sslHandler);
        }*/

        if (sslTcpPortProp != null && sslPortEnabled) {
            SslHandler sslHandler = initSSLHandler(props);
            if (sslHandler == null) {
                log.error("Can't initialize SSLHandler layer! Exiting, check your configuration of jks");
                return;
            }
            initializeSSLTCPTransport(messaging, props, sslHandler);
            /**
             * We ommit web sockets for now
             */
            //initializeWSSTransport(messaging, props, sslHandler);
        } else {
            log.warn("MQTT SSL port not readable or has been disabled as per configuration.");
        }
    }

    private void initFactory(String host, int port, final PipelineInitializer pipeliner) {
        ServerBootstrap b = new ServerBootstrap();
        b.group(m_bossGroup, m_workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        try {
                            pipeliner.init(pipeline);
                        } catch (Throwable th) {
                            log.error("Severe error during pipeline creation", th);
                            throw th;
                        }
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 128)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        try {
            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(host, port);
            log.info("Server binded host: {}, port: {}", host, port);
            f.sync();
        } catch (InterruptedException ex) {
            log.error(null, ex);
        }
    }

    private void initializePlainTCPTransport(IMessaging messaging, Properties props) throws IOException {
        final NettyMQTTHandler handler = new NettyMQTTHandler();
        handler.setMessaging(messaging);
        String host = props.getProperty(Constants.HOST_PROPERTY_NAME);
        int port = Integer.parseInt(props.getProperty(Constants.PORT_PROPERTY_NAME));
        initFactory(host, port, new PipelineInitializer() {
            @Override
            void init(ChannelPipeline pipeline) {
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, Constants.DEFAULT_CONNECT_TIMEOUT));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
                //pipeline.addLast("logger", new LoggingHandler("Netty", LogLevel.ERROR));
                pipeline.addFirst("bytemetrics", new BytesMetricsHandler(m_bytesMetricsCollector));
                pipeline.addLast("decoder", new MQTTDecoder());
                pipeline.addLast("encoder", new MQTTEncoder());
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("handler", handler);
            }
        });
    }

    private void initializeWebSocketTransport(IMessaging messaging, Properties props) throws IOException {
        String webSocketPortProp = props.getProperty(Constants.WEB_SOCKET_PORT_PROPERTY_NAME);
        if (webSocketPortProp == null) {
            //Do nothing no WebSocket configured
            log.info("WebSocket is disabled");
            return;
        }
        int port = Integer.parseInt(webSocketPortProp);

        final NettyMQTTHandler handler = new NettyMQTTHandler();
        handler.setMessaging(messaging);

        String host = props.getProperty(Constants.HOST_PROPERTY_NAME);
        initFactory(host, port, new PipelineInitializer() {
            @Override
            void init(ChannelPipeline pipeline) {
                pipeline.addLast("httpEncoder", new HttpResponseEncoder());
                pipeline.addLast("httpDecoder", new HttpRequestDecoder());
                pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
                pipeline.addLast("webSocketHandler", new WebSocketServerProtocolHandler("/mqtt"/*"/mqtt"*/, "mqttv3.1, mqttv3.1.1"));
                //pipeline.addLast("webSocketHandler", new WebSocketServerProtocolHandler(null, "mqtt"));
                pipeline.addLast("ws2bytebufDecoder", new WebSocketFrameToByteBufDecoder());
                pipeline.addLast("bytebuf2wsEncoder", new ByteBufToWebSocketFrameEncoder());
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, Constants.DEFAULT_CONNECT_TIMEOUT));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
                pipeline.addFirst("bytemetrics", new BytesMetricsHandler(m_bytesMetricsCollector));
                pipeline.addLast("decoder", new MQTTDecoder());
                pipeline.addLast("encoder", new MQTTEncoder());
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("handler", handler);
            }
        });
    }

    private void initializeSSLTCPTransport(IMessaging messaging, Properties props, final SslHandler sslHandler)
            throws IOException {
        String sslPortProp = props.getProperty(Constants.SSL_PORT_PROPERTY_NAME);
        //TODO need to re visit
        sslPortProp = props.get(Constants.SSL_PORT_PROPERTY_NAME).toString();
        if (sslPortProp == null) {
            //Do nothing no SSL configured
            log.info("SSL is disabled");
            return;
        }

        int sslPort = Integer.parseInt(sslPortProp);
        log.info("Starting SSL on port {}", sslPort);

        final NettyMQTTHandler handler = new NettyMQTTHandler();
        handler.setMessaging(messaging);
        String host = props.getProperty(Constants.HOST_PROPERTY_NAME);
        initFactory(host, sslPort, new PipelineInitializer() {
            @Override
            void init(ChannelPipeline pipeline) throws Exception {
                pipeline.addLast("ssl", sslHandler);
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, Constants.DEFAULT_CONNECT_TIMEOUT));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
                //pipeline.addLast("logger", new LoggingHandler("Netty", LogLevel.ERROR));
                pipeline.addFirst("bytemetrics", new BytesMetricsHandler(m_bytesMetricsCollector));
                pipeline.addLast("decoder", new MQTTDecoder());
                pipeline.addLast("encoder", new MQTTEncoder());
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("handler", handler);
            }
        });
    }

    private void initializeWSSTransport(IMessaging messaging, Properties props, final SslHandler sslHandler)
            throws IOException {
        String sslPortProp = props.getProperty(Constants.WSS_PORT_PROPERTY_NAME);
        if (sslPortProp == null) {
            //Do nothing no SSL configured
            log.info("SSL is disabled");
            return;
        }
        int sslPort = Integer.parseInt(sslPortProp);
        final NettyMQTTHandler handler = new NettyMQTTHandler();
        handler.setMessaging(messaging);
        String host = props.getProperty(Constants.HOST_PROPERTY_NAME);
        initFactory(host, sslPort, new PipelineInitializer() {
            @Override
            void init(ChannelPipeline pipeline) throws Exception {
                pipeline.addLast("ssl", sslHandler);
                pipeline.addLast("httpEncoder", new HttpResponseEncoder());
                pipeline.addLast("httpDecoder", new HttpRequestDecoder());
                pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
                pipeline.addLast("webSocketHandler", new WebSocketServerProtocolHandler("/mqtt", "mqttv3.1, mqttv3.1.1"));
                pipeline.addLast("ws2bytebufDecoder", new WebSocketFrameToByteBufDecoder());
                pipeline.addLast("bytebuf2wsEncoder", new ByteBufToWebSocketFrameEncoder());
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(0, 0, Constants.DEFAULT_CONNECT_TIMEOUT));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", new MoquetteIdleTimoutHandler());
                pipeline.addFirst("bytemetrics", new BytesMetricsHandler(m_bytesMetricsCollector));
                pipeline.addLast("decoder", new MQTTDecoder());
                pipeline.addLast("encoder", new MQTTEncoder());
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("handler", handler);
            }
        });
    }

    public void close() {
        if (m_workerGroup == null) {
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        if (m_bossGroup == null) {
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        m_workerGroup.shutdownGracefully();
        m_bossGroup.shutdownGracefully();

        MessageMetrics metrics = m_metricsCollector.computeMetrics();
        log.info("Msg read: {}, msg wrote: {}", metrics.messagesRead(), metrics.messagesWrote());

        BytesMetrics bytesMetrics = m_bytesMetricsCollector.computeMetrics();
        log.info(String.format("Bytes read: %d, bytes wrote: %d", bytesMetrics.readBytes(), bytesMetrics.wroteBytes()));
    }

    /**
     * Constructs connection settings required for ssl connectivity
     *
     * @param props holds information necessary to establish ssl connectivity
     * @return ConnectionSettings
     */
    private ConnectionSettings constructConnectionSettings(Properties props) {
        ConnectionSettings connectionSettings = new ConnectionSettings();
        String certificateType = "SunX509";

        String sslTrustStoreLocation = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_TRUSTSTORE)).getStoreLocation();
        String sslTrustStorePassword = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_TRUSTSTORE)).getPassword();
        String sslKeyStoreLocation = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_KEYSTORE)).getStoreLocation();
        String sslKeyStorePassword = ((JKSStore) AndesConfigurationManager.readValue
                (AndesConfiguration.TRANSPORTS_MQTT_SSL_CONNECTION_KEYSTORE)).getPassword();


        connectionSettings.setTrustStorePath(sslTrustStoreLocation);
        connectionSettings.setTrustStorePassword(sslTrustStorePassword);
        connectionSettings.setTrustStoreCertType(certificateType);

        connectionSettings.setKeyStorePath(sslKeyStoreLocation);
        connectionSettings.setKeyStorePassword(sslKeyStorePassword);
        connectionSettings.setKeyStoreCertType(certificateType);

        return connectionSettings;
    }


    /**
     * Initialization of SSLHandler
     * @param props the configuration details
     * @return
     */
    private SslHandler initSSLHandler(Properties props) {

        try {
            ConnectionSettings sslConnectionSettings = constructConnectionSettings(props);
            SSLContext serverContext = SSLUtil.createSSLContext(sslConnectionSettings);
            SSLEngine engine = serverContext.createSSLEngine();
            engine.setUseClientMode(false);
            return new SslHandler(engine);
        } catch (NoSuchAlgorithmException | UnrecoverableKeyException | CertificateException | KeyStoreException
                | KeyManagementException | IOException ex) {
            log.error("Can't start SSL layer!", ex);
            return null;
        } catch (Exception e) {
            //Here SSLUtils throws a generic type Exception, hence we need to catch it
            //This is bad anyhow
            log.error("Error while establishing ssl server context ", e.getMessage());
            return null;
        }
    }

}
