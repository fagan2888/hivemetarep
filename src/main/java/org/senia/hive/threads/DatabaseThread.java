package org.senia.hive.threads;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.senia.hive.GetHiveObjects;
import org.senia.hive.HiveMetaDataStore;

public class DatabaseThread extends Thread {
	HiveMetaStoreClient msc;
	HiveConf hconf;
	List<String> databases;
	GetHiveObjects gho;
	boolean syncMeta;
	String local_clusterName;
	String remote_clusterName;
	public DatabaseThread(ThreadGroup tg,String name, List<String> databases, HiveConf hconf, boolean syncMeta, String remote_clusterName, String local_clusterName) {
		super(tg, name);
		this.hconf = hconf;
		try {
			this.msc = new HiveMetaStoreClient(hconf);
		} catch (MetaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.databases = databases;
		gho = new GetHiveObjects(msc);
		this.syncMeta = syncMeta;
		this.local_clusterName = local_clusterName;
		this.remote_clusterName = remote_clusterName;

	}
	public void run() {
		for (String localDbStr : databases) {
			System.out.println("Database: " + localDbStr);
			Database ldb = gho.getDatabase(localDbStr);
			if (syncMeta) {
				ldb.setLocationUri(
						gho.replacePath(gho.getLocationUri(ldb), local_clusterName, remote_clusterName));
			}
			HiveMetaDataStore.ldatabases.add(ldb);
		}
		msc.close();
	}
}
