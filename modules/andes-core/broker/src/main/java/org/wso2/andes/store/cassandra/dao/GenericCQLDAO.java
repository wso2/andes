package org.wso2.andes.store.cassandra.dao;


import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.store.util.CassandraDataAccessException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

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
		
	
	public static ResultSet execute(String keySpace ,String query) throws CassandraDataAccessException{
		ResultSet result = getSession(keySpace).execute(query);
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
