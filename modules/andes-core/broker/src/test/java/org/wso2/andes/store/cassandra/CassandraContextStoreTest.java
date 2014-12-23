///*
// * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
// *
// *     WSO2 Inc. licenses this file to you under the Apache License,
// *     Version 2.0 (the "License"); you may not use this file except
// *     in compliance with the License.
// *     You may obtain a copy of the License at
// *
// *       http://www.apache.org/licenses/LICENSE-2.0
// *
// *    Unless required by applicable law or agreed to in writing,
// *    software distributed under the License is distributed on an
// *    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// *    KIND, either express or implied.  See the License for the
// *    specific language governing permissions and limitations
// *    under the License.
// */
//
//package org.wso2.andes.store.cassandra;
//import org.h2.jdbcx.JdbcDataSource;
//import org.junit.BeforeClass;
//import JDBCConstants;
//
//import javax.naming.Context;
//import javax.naming.InitialContext;
//import javax.naming.NameAlreadyBoundException;
//import java.sql.DriverManager;
//
//public class CassandraContextStoreTest {
//
//    @BeforeClass
//    public static void BeforeClass() throws Exception {
//        try {
//            // Create initial context
//            String jdbcUrl = "jdbc:h2:mem:msg_store;DB_CLOSE_ON_EXIT=FALSE";
//            System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
//                    "org.apache.naming.java.javaURLContextFactory");
//            System.setProperty(Context.URL_PKG_PREFIXES,
//                    "org.apache.naming");
//
//            InitialContext ic = new InitialContext();
//            ic.createSubcontext("jdbc");
//            JdbcDataSource ds = new JdbcDataSource();
//            ds.setURL(jdbcUrl);
//            ic.bind(JDBCConstants.H2_MEM_JNDI_LOOKUP_NAME, ds);
//
//            Class.forName("org.h2.Driver");
//            connection = DriverManager.getConnection(jdbcUrl);
//        } catch (NameAlreadyBoundException ignored) {
//        }
//    }
//}
