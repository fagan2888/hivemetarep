package org.senia.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

public class GetHiveObjects {
	HiveMetaStoreClient msc;
	
	public GetHiveObjects(HiveMetaStoreClient msc) {
		this.msc = msc;
	}

	public List<String> getDatabases() throws MetaException {
		return msc.getAllDatabases();
	}

	public Database getDatabase(String dbName) {
		Database db = null;
		try {
			db = msc.getDatabase(dbName);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return db;
	}

	public List<String> getAllTables(String dbName) throws MetaException {
		return msc.getAllTables(dbName);
	}

	public Table getTable(String dbName, String tableName) {
		Table ltb = null;
		try {
			ltb = msc.getTable(dbName, tableName);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ltb;
	}
	
	public StorageDescriptor getSd(Table tb) {
		return tb.getSd();
	}
	public StorageDescriptor getSd(Partition pt) {
		return pt.getSd();
	}
	public String getLocationUri(Database db) {
		return db.getLocationUri();
	}
	
	public StorageDescriptor replacePath(StorageDescriptor sd, String localClusterName, String remoteClusterName) {
		if (sd.getLocation().contains("hdfs://" + localClusterName)) {
			sd.setLocation(sd.getLocation().replace(localClusterName, remoteClusterName));
		}
		return sd;
	}
	public String replacePath(String locationUri, String localClusterName, String remoteClusterName) {
		if (locationUri.contains("hdfs://" + localClusterName)) {
			locationUri = locationUri.replace(localClusterName, remoteClusterName);
		}
		return locationUri;
	}
	
	public List<String> getPartitionKeys(Table tb) {
		ArrayList<String> partKeyList = new ArrayList<String>();
		List<FieldSchema> partKeys = tb.getPartitionKeys();
		for (FieldSchema partKey : partKeys) {
			partKeyList.add(partKey.getName());
		}
		return partKeyList;
	}
	
	public List<Partition> getPartitionsByName(String dbName, String tableName, List<String> partKeyList) {
		List<Partition> parts = null;
		try {
			parts = msc.getPartitionsByNames(dbName, tableName, partKeyList);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return parts;
	}
}
