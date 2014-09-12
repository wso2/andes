package org.wso2.andes.messageStore;

import junit.framework.TestCase;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Before;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameAlreadyBoundException;
import java.sql.Connection;
import java.sql.DriverManager;

public class H2BasedAndesContextStoreTest extends TestCase {

    private static Connection connection;
    private H2BasedAndesContextStore contextStore;

    @BeforeClass
    public void BeforeClass() throws Exception{
        try {
            // Create initial context
            System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
                    "org.apache.naming.java.javaURLContextFactory");
            System.setProperty(Context.URL_PKG_PREFIXES,
                    "org.apache.naming");

            InitialContext ic = new InitialContext();
            ic.createSubcontext("jdbc");
            JdbcDataSource ds = new JdbcDataSource();
            ds.setURL("jdbc:h2:mem:msg_store;DB_CLOSE_ON_EXIT=FALSE");
            ic.bind("jdbc/InMemoryMessageStoreDB", ds);

            Class.forName("org.h2.Driver");
            connection = DriverManager.getConnection("jdbc:h2:mem:msg_store;DB_CLOSE_ON_EXIT=FALSE");
        } catch (NameAlreadyBoundException ignored) {
        }
    }

    @Before
    public void setup() throws Exception{
        createTables();
        contextStore = new H2BasedAndesContextStore(true); // in memory mode mode
        contextStore.init(null);
    }

    @AfterClass
    public void afterClass() {
        createResult();
    }



    private void createTables() {

    }


}