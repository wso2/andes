/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.andes.server.state;

import org.apache.log4j.Logger;
import org.wso2.andes.AMQException;
import org.wso2.andes.framing.*;
import org.wso2.andes.protocol.AMQConstant;
import org.wso2.andes.protocol.AMQMethodEvent;
import org.wso2.andes.protocol.AMQMethodListener;
import org.wso2.andes.server.protocol.AMQProtocolSession;
import org.wso2.andes.server.security.SecurityManager;
import org.wso2.andes.server.virtualhost.VirtualHostRegistry;

import java.util.concurrent.CopyOnWriteArraySet;

/**
 * The state manager is responsible for managing the state of the protocol session. <p/> For each AMQProtocolHandler
 * there is a separate state manager.
 */
public class AMQStateManager implements AMQMethodListener
{
    private static final Logger _logger = Logger.getLogger(AMQStateManager.class);

    private final VirtualHostRegistry _virtualHostRegistry;
    private final AMQProtocolSession _protocolSession;
    /** The current state */
    private AMQState _currentState;

    /**
     * Maps from an AMQState instance to a Map from Class to StateTransitionHandler. The class must be a subclass of
     * AMQFrame.
     */
/*    private final EnumMap<AMQState, Map<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>>> _state2HandlersMap =
        new EnumMap<AMQState, Map<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>>>(
            AMQState.class);
  */


    private CopyOnWriteArraySet<StateListener> _stateListeners = new CopyOnWriteArraySet<StateListener>();

    public AMQStateManager(VirtualHostRegistry virtualHostRegistry, AMQProtocolSession protocolSession)
    {

        _virtualHostRegistry = virtualHostRegistry;
        _protocolSession = protocolSession;
        _currentState = AMQState.CONNECTION_NOT_STARTED;

    }

    /*
    protected void registerListeners()
    {
        Map<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>> frame2handlerMap;

        frame2handlerMap = new HashMap<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>>();
        _state2HandlersMap.put(AMQState.CONNECTION_NOT_STARTED, frame2handlerMap);

        frame2handlerMap = new HashMap<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>>();
        _state2HandlersMap.put(AMQState.CONNECTION_NOT_AUTH, frame2handlerMap);

        frame2handlerMap = new HashMap<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>>();
        _state2HandlersMap.put(AMQState.CONNECTION_NOT_TUNED, frame2handlerMap);

        frame2handlerMap = new HashMap<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>>();
        frame2handlerMap.put(ConnectionOpenBody.class, ConnectionOpenMethodHandler.getInstance());
        _state2HandlersMap.put(AMQState.CONNECTION_NOT_OPENED, frame2handlerMap);

        //
        // ConnectionOpen handlers
        //
        frame2handlerMap = new HashMap<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>>();
        ChannelOpenHandler.getInstance();
        ChannelCloseHandler.getInstance();
        ChannelCloseOkHandler.getInstance();
        ConnectionCloseMethodHandler.getInstance();
        ConnectionCloseOkMethodHandler.getInstance();
        ConnectionTuneOkMethodHandler.getInstance();
        ConnectionSecureOkMethodHandler.getInstance();
        ConnectionStartOkMethodHandler.getInstance();
        ExchangeDeclareHandler.getInstance();
        ExchangeDeleteHandler.getInstance();
        ExchangeBoundHandler.getInstance();
        BasicAckMethodHandler.getInstance();
        BasicRecoverMethodHandler.getInstance();
        BasicConsumeMethodHandler.getInstance();
        BasicGetMethodHandler.getInstance();
        BasicCancelMethodHandler.getInstance();
        BasicPublishMethodHandler.getInstance();
        BasicQosHandler.getInstance();
        QueueBindHandler.getInstance();
        QueueDeclareHandler.getInstance();
        QueueDeleteHandler.getInstance();
        QueuePurgeHandler.getInstance();
        ChannelFlowHandler.getInstance();
        TxSelectHandler.getInstance();
        TxCommitHandler.getInstance();
        TxRollbackHandler.getInstance();
        BasicRejectMethodHandler.getInstance();

        _state2HandlersMap.put(AMQState.CONNECTION_OPEN, frame2handlerMap);

        frame2handlerMap = new HashMap<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>>();

        _state2HandlersMap.put(AMQState.CONNECTION_CLOSING, frame2handlerMap);

    } */

    public AMQState getCurrentState()
    {
        return _currentState;
    }

    public void changeState(AMQState newState) throws AMQException
    {
        _logger.debug("State changing to " + newState + " from old state " + _currentState);
        final AMQState oldState = _currentState;
        _currentState = newState;

        for (StateListener l : _stateListeners)
        {
            l.stateChanged(oldState, newState);
        }
    }

    public void error(Exception e)
    {
        _logger.error("State manager received error notification[Current State:" + _currentState + "]: " + e, e);
        for (StateListener l : _stateListeners)
        {
            l.error(e);
        }
    }

    public <B extends AMQMethodBody> boolean methodReceived(AMQMethodEvent<B> evt) throws AMQException
    {
        MethodDispatcher dispatcher = _protocolSession.getMethodDispatcher();

        final int channelId = evt.getChannelId();
        B body = evt.getMethod();

        if(channelId != 0 && _protocolSession.getChannel(channelId)== null)
        {

            if(! ((body instanceof ChannelOpenBody)
                  || (body instanceof ChannelCloseOkBody)
                  || (body instanceof ChannelCloseBody)))
            {
                throw body.getConnectionException(AMQConstant.CHANNEL_ERROR, "channel is closed won't process:" + body);
            }

        }

        return body.execute(dispatcher, channelId);

    }

    private <B extends AMQMethodBody> void checkChannel(AMQMethodEvent<B> evt, AMQProtocolSession protocolSession)
        throws AMQException
    {
        if ((evt.getChannelId() != 0) && !(evt.getMethod() instanceof ChannelOpenBody)
                && (protocolSession.getChannel(evt.getChannelId()) == null)
                && !protocolSession.channelAwaitingClosure(evt.getChannelId()))
        {
            throw evt.getMethod().getChannelNotFoundException(evt.getChannelId());
        }
    }

/*
    protected <B extends AMQMethodBody> StateAwareMethodListener<B> findStateTransitionHandler(AMQState currentState,
        B frame)
    // throws IllegalStateTransitionException
    {
        final Map<Class<? extends AMQMethodBody>, StateAwareMethodListener<? extends AMQMethodBody>> classToHandlerMap =
            _state2HandlersMap.get(currentState);

        final StateAwareMethodListener<B> handler =
            (classToHandlerMap == null) ? null : (StateAwareMethodListener<B>) classToHandlerMap.get(frame.getClass());

        if (handler == null)
        {
            _logger.debug("No state transition handler defined for receiving frame " + frame);

            return null;
        }
        else
        {
            return handler;
        }
    }
*/

    public void addStateListener(StateListener listener)
    {
        _logger.debug("Adding state listener");
        _stateListeners.add(listener);
    }

    public void removeStateListener(StateListener listener)
    {
        _stateListeners.remove(listener);
    }

    public VirtualHostRegistry getVirtualHostRegistry()
    {
        return _virtualHostRegistry;
    }

    public AMQProtocolSession getProtocolSession()
    {
        SecurityManager.setThreadSubject(_protocolSession.getAuthorizedSubject());
        return _protocolSession;
    }
}
