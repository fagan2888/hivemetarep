package org.senia.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveMetaDataStore {
	// public instance initialized when loading the class
	public static Object ldatabaseLock = new Object();
	public static Object ltableLock = new Object();
	public static Object partLock = new Object();
	public static Integer partThreadCount = 0;
	public static Integer tableThreadCount = 0;
	public static Integer databaseThreadCount = 0;

	private static final HiveMetaDataStore instance = new HiveMetaDataStore();
	public static List<Database> ldatabases = Collections.synchronizedList(new ArrayList<Database>());
	public static List<Table> ltables = Collections.synchronizedList(new ArrayList<Table>());
	public static List<Partition> lparts = Collections.synchronizedList(new ArrayList<Partition>());

	private HiveMetaDataStore() {

	}

	public static HiveMetaDataStore getInstance() {
		return instance;
	}

}
