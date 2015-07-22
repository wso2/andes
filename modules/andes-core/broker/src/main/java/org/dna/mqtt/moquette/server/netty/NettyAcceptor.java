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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Properties;

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

    private static final Logger LOG = LoggerFactory.getLogger(NettyAcceptor.class);

    EventLoopGroup m_bossGroup;
    EventLoopGroup m_workerGroup;
    BytesMetricsCollector m_bytesMetricsCollector = new BytesMetricsCollector();
    MessageMetricsCollector m_metricsCollector = new MessageMetricsCollector();

    @Override
    public void initialize(IMessaging messaging, Properties props) throws IOException {
        m_bossGroup = new NioEventLoopGroup();
        m_workerGroup = new NioEventLoopGroup();

        initializePlainTCPTransport(messaging, props);
        /**
         * We leave the websockets commented for now since we do not support end to end integration with it
         */
        //initializeWebSocketTransport(messaging, props);
        //TODO need to re look into using getProperty here
        String sslTcpPortProp = props.get(Constants.SSL_PORT_PROPERTY_NAME).toString();
        String wssPortProp = props.getProperty(Constants.WSS_PORT_PROPERTY_NAME);
        /* if (sslTcpPortProp != null || wssPortProp != null) {
            SslHandler sslHandler = initSSLHandler(props);
            if (sslHandler == null) {
                LOG.error("Can't initialize SSLHandler layer! Exiting, check your configuration of jks");
                return;
            }
            initializeSSLTCPTransport(messaging, props, sslHandler);
            initializeWSSTransport(messaging, props, sslHandler);
        }*/

        if (sslTcpPortProp != null) {
            SslHandler sslHandler = initSSLHandler(props);
            if (sslHandler == null) {
                LOG.error("Can't initialize SSLHandler layer! Exiting, check your configuration of jks");
                return;
            }
            initializeSSLTCPTransport(messaging, props, sslHandler);
            /**
             * We ommit web sockets for now
             */
            //initializeWSSTransport(messaging, props, sslHandler);
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
                            LOG.error("Severe error during pipeline creation", th);
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
            LOG.info("Server binded host: {}, port: {}", host, port);
            f.sync();
        } catch (InterruptedException ex) {
            LOG.error(null, ex);
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
            LOG.info("WebSocket is disabled");
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
            LOG.info("SSL is disabled");
            return;
        }

        int sslPort = Integer.parseInt(sslPortProp);
        LOG.info("Starting SSL on port {}", sslPort);

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
            LOG.info("SSL is disabled");
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
        LOG.info("Msg read: {}, msg wrote: {}", metrics.messagesRead(), metrics.messagesWrote());

        BytesMetrics bytesMetrics = m_bytesMetricsCollector.computeMetrics();
        LOG.info(String.format("Bytes read: %d, bytes wrote: %d", bytesMetrics.readBytes(), bytesMetrics.wroteBytes()));
    }


    private SslHandler initSSLHandler(Properties props) {
        String jksPath = props.getProperty(Constants.JKS_PATH_PROPERTY_NAME);
        //TODO remove the below hard coded
        jksPath = "/Users/pamod/Desktop/dev/MQTTUpgrade/wso2mb-3.0.0-SNAPSHOT/repository/resources/security/wso2carbon.jks";
        LOG.info("Starting SSL using keystore at {}", jksPath);
        if (jksPath == null || jksPath.isEmpty()) {
            //key_store_password or key_manager_password are empty
            LOG.warn("You have configured the SSL port but not the jks_path, SSL not started");
            return null;
        }

        //if we have the port also the jks then keyStorePassword and keyManagerPassword
        //has to be defined
        String keyStorePassword = props.getProperty(Constants.KEY_STORE_PASSWORD_PROPERTY_NAME);
        String keyManagerPassword = props.getProperty(Constants.KEY_MANAGER_PASSWORD_PROPERTY_NAME);
        //TODO remove the below
        keyManagerPassword = "wso2carbon";
        keyStorePassword = "wso2carbon";
        if (keyStorePassword == null || keyStorePassword.isEmpty()) {
            //key_store_password or key_manager_password are empty
            LOG.warn("You have configured the SSL port but not the key_store_password, SSL not started");
            return null;
        }
        if (keyManagerPassword == null || keyManagerPassword.isEmpty()) {
            //key_manager_password or key_manager_password are empty
            LOG.warn("You have configured the SSL port but not the key_manager_password, SSL not started");
            return null;
        }

        try {
            InputStream jksInputStream = jksDatastore(jksPath);
            SSLContext serverContext = SSLContext.getInstance("TLS");
            final KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(jksInputStream, keyStorePassword.toCharArray());
            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keyManagerPassword.toCharArray());
            serverContext.init(kmf.getKeyManagers(), null, null);

            SSLEngine engine = serverContext.createSSLEngine();
            engine.setUseClientMode(false);
            return new SslHandler(engine);
        } catch (NoSuchAlgorithmException | UnrecoverableKeyException | CertificateException | KeyStoreException
                | KeyManagementException | IOException ex) {
            LOG.error("Can't start SSL layer!", ex);
            return null;
        }
    }

    private InputStream jksDatastore(String jksPath) throws FileNotFoundException {
        URL jksUrl = getClass().getClassLoader().getResource(jksPath);
        if (jksUrl != null) {
            LOG.info("Starting with jks at {}, jks normal {}", jksUrl.toExternalForm(), jksUrl);
            return getClass().getClassLoader().getResourceAsStream(jksPath);
        }
        LOG.info("jks not found in bundled resources, try on the filesystem");
        File jksFile = new File(jksPath);
        if (jksFile.exists()) {
            LOG.info("Using {} ", jksFile.getAbsolutePath());
            return new FileInputStream(jksFile);
        }
        LOG.warn("File {} doesn't exists", jksFile.getAbsolutePath());
        return null;
    }


}
