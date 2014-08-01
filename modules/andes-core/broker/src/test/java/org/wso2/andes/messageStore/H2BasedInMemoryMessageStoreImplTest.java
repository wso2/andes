package org.wso2.andes.messageStore;

import junit.framework.TestCase;
import org.wso2.andes.kernel.AndesMessagePart;
import org.wso2.andes.kernel.MessageStore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class H2BasedInMemoryMessageStoreImplTest extends TestCase {

    MessageStore messageStore;
    Connection connection;

    public void setUp() throws Exception {
        super.setUp();
        messageStore = new H2BasedInMemoryMessageStoreImpl();
        Class.forName("org.h2.Driver");
        connection = DriverManager.getConnection("jdbc:h2:mem:msg_store;DB_CLOSE_DELAY=-1");
        ((H2BasedInMemoryMessageStoreImpl) messageStore).connection = connection;
        ((H2BasedInMemoryMessageStoreImpl) messageStore).createTables();
    }

    public void tearDown() throws Exception {
        messageStore.close();
    }

    public void testStoreRetrieveMessagePart() throws Exception {

        byte[] content = "test message".getBytes();
        byte[] content2 = "second part".getBytes();

        // store messages
        List<AndesMessagePart> list = new ArrayList<AndesMessagePart>(10);
        for (int i = 0; i < 10; i++) {
            AndesMessagePart p = new AndesMessagePart();
            p.setMessageID(i);
            p.setData(content);
            p.setDataLength(content.length);
            p.setOffSet(0);
            list.add(p);

            // offset
            p = new AndesMessagePart();
            p.setMessageID(i);
            p.setData(content2);
            p.setDataLength(content2.length);
            p.setOffSet(content.length);
            list.add(p);
        }
        messageStore.storeMessagePart(list);

        for (int i = 0; i < 10; i++) {
            AndesMessagePart p = messageStore.getContent(i, 0);
            assert p != null;
            assertEquals(i, p.getMessageID());
            assertEquals(true, Arrays.equals(content, p.getData()));
            assertEquals(content.length, p.getDataLength());
            assertEquals(0, p.getOffSet());

            int offset = p.getDataLength(); // data length as offset
            p = messageStore.getContent(i, offset);
            assert p != null;
            assertEquals(i, p.getMessageID());
            assertEquals(true, Arrays.equals(content2, p.getData()));
            assertEquals(content.length, p.getDataLength());
            assertEquals(offset, p.getOffSet());
        }
    }

    public void testDeleteMessageParts() throws Exception {

        List<Long> list = new ArrayList<Long>(10);


        for (int i = 0; i < 10; i++) {
            list.add((long) i);
        }
        messageStore.deleteMessageParts(list);

        String sqlStr = "SELECT * " +
                " FROM " + JDBCConstants.MESSAGES_TABLE +
                " WHERE " + JDBCConstants.MESSAGE_ID + "=?";
        PreparedStatement preparedStatement = connection.prepareStatement(sqlStr);
        for (int i = 0; i < 10; i++) {
            preparedStatement.setLong(1, i);
            preparedStatement.addBatch();
        }

        ResultSet resultSet = preparedStatement.executeQuery();
        assertEquals(false, resultSet.next());
    }

    public void testAckReceived() throws Exception {

    }

    public void testAddMetaData() throws Exception {

    }

    public void testGetMessageCountForQueue() throws Exception {

    }

    public void testGetMetaData() throws Exception {

    }

    public void testGetMetaDataList() throws Exception {

    }

    public void testGetNextNMessageMetadataFromQueue() throws Exception {

    }
}