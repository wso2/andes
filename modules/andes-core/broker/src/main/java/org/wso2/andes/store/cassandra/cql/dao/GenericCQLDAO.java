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

package org.wso2.andes.store.cassandra.cql.dao;


import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;

/**
 * Data Access Object to access Cassandra using CQL.
 */
public class GenericCQLDAO {
	
	private static Log log = LogFactory.getLog(GenericCQLDAO.class);
	
	public static final String CLUSTER_SESSION = "SessionForCluster";
	
	private static final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private static final Lock readLock = readWriteLock.readLock();
	private static final Lock writeLock = readWriteLock.writeLock();
	
	private static final ConcurrentHashMap<String,Session> sessioCache = new ConcurrentHashMap<String,Session>();
	private static Cluster cluster;
	
	public static void setCluster(Cluster cl){
		try{
			writeLock.lock();
			cluster = cl;
		}finally{
			writeLock.unlock();
		}
		
	}
	
	
	public static boolean isSessionExist(String key) throws CassandraDataAccessException{
		if(key == null){
			throw new CassandraDataAccessException("Key can't be null");
		}
		return sessioCache.containsKey(key);
	}
	
	public static boolean isSessionNotExist(String key) throws CassandraDataAccessException{
		return !isSessionExist(key);
	}
	
	public static void add(String key ,Session session) throws CassandraDataAccessException{
		
		if(key == null || session == null){
			throw new CassandraDataAccessException("Key or Session can't be null");
		}
		try{
			writeLock.lock();
			sessioCache.putIfAbsent(key, session);
		}finally{
			writeLock.unlock();
		}
	}
	
	public static Session getSession(String key) throws CassandraDataAccessException{
		
		if(key == null){
			throw new CassandraDataAccessException("Key can't be null");
		}
		Session session = null;
		try{
			readLock.lock();
			session = sessioCache.get(key);
		}finally{
			readLock.unlock();
		}
		
		if(session == null){
			session = createSession(cluster, key);
		}
		return session;
	}
	
	public static Session createSession(Cluster cluster, String keyspace) throws CassandraDataAccessException{
		
		if(sessioCache.containsKey(keyspace)){
			return sessioCache.get(keyspace);
		}
		
		Session session = null;		
		synchronized(cluster){
			if(sessioCache.containsKey(keyspace)){
				return sessioCache.get(keyspace);
			}
			if(CLUSTER_SESSION.equalsIgnoreCase(keyspace)){
				session = cluster.connect();
			}else{
				session = cluster.connect(keyspace);
			}			
			add(keyspace, session);
		}
		
		
		return session;
	}
	
	public static void clearCache(){
		try{
			writeLock.lock();
			Set<Entry<String, Session>> entries = sessioCache.entrySet();
			Iterator<Entry<String, Session>> iter = entries.iterator();
			while(iter.hasNext()){
				Entry<String, Session> entry = iter.next();
				entry.getValue().shutdown(200, TimeUnit.MILLISECONDS);
			}
			sessioCache.clear();
		}finally{
			writeLock.unlock();
		}
	}


    public static ResultSet execute(String keySpace, String query)
            throws CassandraDataAccessException {
        ResultSet result;
        try {
            result = getSession(keySpace).execute(query);
        } catch (Exception e) {
            throw new CassandraDataAccessException("Error while executing statement", e);
        }
        return result;
    }

    public static ResultSetFuture executeAsync(String keySpace , String query) throws CassandraDataAccessException{
		ResultSetFuture result = getSession(keySpace).executeAsync(query);
		return result;
	}
	
	
	public static void insert(String keySpace, String table, Map<String,Object> keyValueMap) throws CassandraDataAccessException{
		Insert insert = CQLQueryBuilder.buildSingleInsert(keySpace, table, keyValueMap);
		if(insert == null){
			throw new CassandraDataAccessException(" Insert statement can not be null");
		}
		getSession(keySpace).execute(insert);
	}
	
	public static void update(String keySpace, CQLQueryBuilder.CqlUpdate cqlUpdate) throws CassandraDataAccessException{
		Update update = CQLQueryBuilder.buildSingleUpdate(cqlUpdate);
		if(update == null){
			throw new CassandraDataAccessException(" Update statement can not be null");
		}
		Map<String, Object>  counters = cqlUpdate.getCounterColumnValue();
		String sql = update.getQueryString();
		if(counters != null){
			Set<Entry<String, Object>> entries = counters.entrySet();
			Iterator<Entry<String, Object>>  iter = entries.iterator();
			while(iter.hasNext()){
				Entry<String, Object> entry = iter.next();
				sql = sql.replaceFirst("'"+entry.getKey()+"'", entry.getKey()+"+"+entry.getValue());
			}
		}
		
		getSession(keySpace).execute(sql);
	}
	
	public static void delete(String keySpace ,CQLQueryBuilder.CqlDelete cqlDelete) throws CassandraDataAccessException{
		Delete delete = CQLQueryBuilder.buildSingleDelete(cqlDelete);
		if(delete == null){
			throw new CassandraDataAccessException(" Delete statement can not be null");
		}
		getSession(keySpace).execute(delete);
	}
	
	public static ResultSet select(String keySpace ,CQLQueryBuilder.CqlSelect cqlSelect) throws CassandraDataAccessException{
		Select select = CQLQueryBuilder.buildSelect(cqlSelect);
		return getSession(keySpace).execute(select);
	}
	
	public static void batchInsert(String keySpace, String table, List<Map<String,Object>> rows) throws CassandraDataAccessException{
		List<Insert> statementList = new ArrayList<Insert>();
		for(Map<String,Object> keyValue : rows){
			Insert insert = CQLQueryBuilder.buildSingleInsert(keySpace, table, keyValue);
			statementList.add(insert);
		}
		
		batchExecute(keySpace, statementList.toArray(new Statement[statementList.size()]));
	}
	
	
	public static void batchExecute(String keySpace, Statement[] statements) throws CassandraDataAccessException{
		
		if(statements == null || statements.length == 0){
			return;
		}
		Query batch = batch(statements);
		getSession(keySpace).execute(batch);
		
	}

}
