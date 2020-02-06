package org.senia.hive.threads;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.senia.hive.GetHiveObjects;
import org.senia.hive.HiveMetaDataStore;

public class PartitionThread extends Thread {
	HiveMetaStoreClient msc;
	HiveConf hconf;
	GetHiveObjects gho;
	boolean syncMeta;
	String local_clusterName;
	String remote_clusterName;
	String localDbStr;
	String ltableString;

	public PartitionThread(ThreadGroup tg, String name, String ltableString,
			HiveConf hconf, boolean syncMeta, String remote_clusterName, String local_clusterName) {
		super(tg, name + "_" + ltableString);
		this.localDbStr = name;
		this.ltableString = ltableString;
		try {
			this.msc = new HiveMetaStoreClient(hconf);
		} catch (MetaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		gho = new GetHiveObjects(msc);
		this.syncMeta = syncMeta;
		this.local_clusterName = local_clusterName;
		this.remote_clusterName = remote_clusterName;

	}

	public void run() {
		List<Partition> parts = gho.getPartitions(localDbStr, ltableString);
		for (Partition part : parts) {
			if (syncMeta) {
				part.setSd(gho.replacePath(gho.getSd(part), local_clusterName, remote_clusterName));
			}
			synchronized (HiveMetaDataStore.lparts) {
				HiveMetaDataStore.lparts.add(part);
			}
		}
		msc.close();
	}
}
