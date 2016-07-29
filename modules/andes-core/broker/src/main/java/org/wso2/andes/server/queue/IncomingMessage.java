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
package org.wso2.andes.server.queue;

import org.wso2.andes.framing.abstraction.MessagePublishInfo;
import org.wso2.andes.framing.abstraction.ContentChunk;
import org.wso2.andes.framing.ContentHeaderBody;
import org.wso2.andes.framing.AMQShortString;
import org.wso2.andes.framing.BasicContentHeaderProperties;
import org.wso2.andes.server.store.StoredMessage;
import org.wso2.andes.server.registry.ApplicationRegistry;
import org.wso2.andes.server.exchange.Exchange;
import org.wso2.andes.server.message.InboundMessage;
import org.wso2.andes.server.message.AMQMessageHeader;
import org.wso2.andes.server.message.EnqueableMessage;
import org.wso2.andes.server.message.MessageContentSource;
import org.wso2.andes.server.message.MessageMetaData;
import org.wso2.andes.AMQException;

import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

public class IncomingMessage implements Filterable, InboundMessage, EnqueableMessage, MessageContentSource
{
    private static final boolean SYNCED_CLOCKS =
            ApplicationRegistry.getInstance().getConfiguration().getSynchedClocks();

    private final MessagePublishInfo _messagePublishInfo;
    private ContentHeaderBody _contentHeaderBody;

    /**
     * Publisher's session id to validate whether subscriber and publisher has the same session
     */
    private long publisherSessionID;

    /**
     * Keeps a track of how many bytes we have received in body frames
     */
    private long _bodyLengthReceived = 0;

    /**
     * This is stored during routing, to know the queues to which this message should immediately be
     * delivered. It is <b>cleared after delivery has been attempted</b>. Any persistent record of destinations is done
     * by the message handle.
     */
    private ArrayList<? extends BaseQueue> _destinationQueues;

    private long _expiration;
    private long _arrivalTime;

    private Exchange _exchange;


    private int _receivedChunkCount = 0;
    private List<ContentChunk> _contentChunks = new ArrayList<ContentChunk>();

    // we keep both the original meta data object and the store reference to it just in case the
    // store would otherwise flow it to disk

    private MessageMetaData _messageMetaData;

    private StoredMessage<MessageMetaData> _storedMessageHandle;

    private StoredMessage<MessageMetaData> _storedCassandraMessageHandle;

    public IncomingMessage(
            final MessagePublishInfo info
    )
    {
        _messagePublishInfo = info;
    }

    public void setContentHeaderBody(final ContentHeaderBody contentHeaderBody) throws AMQException
    {
        _contentHeaderBody = contentHeaderBody;
    }

    /**
     * Set the time to live value for the incoming messages
     */
    public void setExpiration()
    {
        _expiration = ((BasicContentHeaderProperties) _contentHeaderBody.getProperties()).getExpiration();

    }

    public long getPublisherSessionID() {
        return publisherSessionID;
    }

    public void setPublisherSessionID(long uniquePublisherSessionID) {
        this.publisherSessionID = uniquePublisherSessionID;
    }

    public void setArrivalTime() {
         _arrivalTime = ((BasicContentHeaderProperties) _contentHeaderBody.getProperties()).getTimestamp();
    }

    public MessageMetaData headersReceived()
    {
        _messageMetaData = new MessageMetaData(_messagePublishInfo, _contentHeaderBody, 0);
        return _messageMetaData;
    }


    public ArrayList<? extends BaseQueue> getDestinationQueues()
    {
        return _destinationQueues;
    }

    /**
     * Add content to message cache
     * @param contentChunk content chunk to ba added to message
     * @return Chunk count
     * @throws AMQException
     */
    public int addContentBodyFrame(final ContentChunk contentChunk)
            throws AMQException
    {
        _bodyLengthReceived += contentChunk.getSize();
        _contentChunks.add(contentChunk);
        return _receivedChunkCount++;
    }

    public long getBodyLengthReceived() {
        return _bodyLengthReceived;
    }

    public boolean allContentReceived()
    {
        return (_bodyLengthReceived == getContentHeader().bodySize);
    }

    public AMQShortString getExchange()
    {
        return _messagePublishInfo.getExchange();
    }

    public String getRoutingKey()
    {
        return _messagePublishInfo.getRoutingKey() == null ? null : _messagePublishInfo.getRoutingKey().toString();
    }

    public String getBinding()
    {
        return _messagePublishInfo.getRoutingKey() == null ? null : _messagePublishInfo.getRoutingKey().toString();
    }


    public boolean isMandatory()
    {
        return _messagePublishInfo.isMandatory();
    }


    public boolean isImmediate()
    {
        return _messagePublishInfo.isImmediate();
    }

    public ContentHeaderBody getContentHeader()
    {
        return _contentHeaderBody;
    }


    public AMQMessageHeader getMessageHeader()
    {
        return _messageMetaData.getMessageHeader();
    }

    public boolean isPersistent()
    {
        return getContentHeader().getProperties() instanceof BasicContentHeaderProperties &&
             ((BasicContentHeaderProperties) getContentHeader().getProperties()).getDeliveryMode() ==
                                                             BasicContentHeaderProperties.PERSISTENT;
    }

    public boolean isRedelivered()
    {
        return false;
    }


    public long getSize()
    {
        return getContentHeader().bodySize;
    }

    public Long getMessageNumber()
    {
        return _storedMessageHandle.getMessageNumber();
    }

    public void setExchange(final Exchange e)
    {
        _exchange = e;
    }

    public void route()
    {
        enqueue(_exchange.route(this));

    }

    public void enqueue(final ArrayList<? extends BaseQueue> queues)
    {
        _destinationQueues = queues;
    }

    public MessagePublishInfo getMessagePublishInfo()
    {
        return _messagePublishInfo;
    }

    public long getExpiration()
    {
        return _expiration;
    }

    public long getArrivalTime()
    {
        return _arrivalTime;
    }

    public int getReceivedChunkCount()
    {
        return _receivedChunkCount;
    }


    public int getBodyCount() throws AMQException
    {
        return _contentChunks.size();
    }

    public ContentChunk getContentChunk(int index) throws IllegalArgumentException, AMQException
    {
        return _contentChunks.get(index);
    }


    public int getContent(ByteBuffer buf, int offset)
    {
        int pos = 0;
        int written = 0;
        for(ContentChunk cb : _contentChunks)
        {
            ByteBuffer data = cb.getData().buf();
            if(offset+written >= pos && offset < pos + data.limit())
            {
                ByteBuffer src = data.duplicate();
                src.position(offset+written - pos);
                src = src.slice();

                if(buf.remaining() < src.limit())
                {
                    src.limit(buf.remaining());
                }
                int count = src.limit();
                buf.put(src);
                written += count;
                if(buf.remaining() == 0)
                {
                    break;
                }
            }
            pos+=data.limit();
        }
        return written;

    }

    public void setStoredMessage(StoredMessage<MessageMetaData> storedMessageHandle)
    {
        _storedMessageHandle = storedMessageHandle;
        _storedMessageHandle.setExchange(this.getExchange().toString());
    }

    public StoredMessage<MessageMetaData> getStoredMessage()
    {
        return _storedMessageHandle;
    }

    public StoredMessage<MessageMetaData> get_storedCassandraMessageHandle() {
        return _storedCassandraMessageHandle;
    }

    public void set_storedCassandraMessageHandle(StoredMessage<MessageMetaData> _storedCassandraMessageHandle) {
        this._storedCassandraMessageHandle = _storedCassandraMessageHandle;
    }
}
