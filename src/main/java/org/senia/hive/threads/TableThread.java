package org.senia.hive.threads;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.senia.hive.HiveObjectHelper;
import org.senia.hive.HiveMetaDataStore;

public class TableThread extends Thread {
	HiveMetaStoreClient msc;
	HiveConf hconf;
	List<String> tables;
	HiveObjectHelper gho;
	boolean connNotEst = true;
	boolean syncMeta;
	String local_clusterName;
	String remote_clusterName;
	String localDbStr;
	ThreadGroup tg;
	ThreadGroup parttg;

	public TableThread(ThreadGroup parttg, ThreadGroup tg, String name, List<String> tables, HiveConf hconf,
			boolean syncMeta, String remote_clusterName, String local_clusterName) {
		super(tg, name);
		this.tg = tg;
		this.parttg = parttg;
		this.localDbStr = name;
		this.hconf = hconf;
		while (connNotEst) {
			try {
				this.msc = new HiveMetaStoreClient(hconf);
				connNotEst = false;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		this.tables = tables;
		gho = new HiveObjectHelper(msc);
		this.syncMeta = syncMeta;
		this.local_clusterName = local_clusterName;
		this.remote_clusterName = remote_clusterName;

	}

	public void run() {
		for (String ltableString : tables) {
			Table ltb = gho.getTable(localDbStr, ltableString);
			if (syncMeta) {
				ltb.setSd(gho.replacePath(gho.getSd(ltb), local_clusterName, remote_clusterName));
			}
			if (gho.partitionExists(ltb)) {
				PartitionThread pThread = new PartitionThread(parttg, localDbStr, ltableString, hconf, syncMeta,
						local_clusterName, remote_clusterName);
				pThread.start();
			}
			synchronized (HiveMetaDataStore.ltables) {
				HiveMetaDataStore.ltables.add(ltb);
			}

		}
		msc.close();
	}
}
