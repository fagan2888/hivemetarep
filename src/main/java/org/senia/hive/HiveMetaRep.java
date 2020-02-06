package org.senia.hive;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Index;

import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;



import com.google.gson.Gson;

public class HiveMetaRep extends MetaStoreEventListener {

	//status - status of insert, true = success, false = failure
	public Gson gson;
	public HiveMetaMessage hmm;
	
	public HiveMetaRep(Configuration config) {
		super(config);
		gson = new Gson();
		hmm = new HiveMetaMessage();
		// TODO Auto-generated constructor stub
		
	}

	public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
		hmm.setCreateTable(tableEvent.getTable());
		hmm.setStatus(tableEvent.getStatus());
		hmm.setOpType(HiveMetaMessage.CREATE_TABLE);
	}

	/**
	 * @param tableEvent table event.
	 * @throws MetaException
	 */
	public void onDropTable(DropTableEvent tableEvent) throws MetaException {
		hmm.setDropTable(tableEvent.getTable());
		hmm.setStatus(tableEvent.getStatus());
		hmm.setDeleteData(tableEvent.getDeleteData());
		hmm.setOpType(HiveMetaMessage.DROP_TABLE);
	}

	/**
	 * @param tableEvent alter table event
	 * @throws MetaException
	 */
	public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
		hmm.setAlterTableNew(tableEvent.getNewTable());
		hmm.setAlterTableOld(tableEvent.getOldTable());
		hmm.setStatus(tableEvent.getStatus());
		hmm.setOpType(HiveMetaMessage.ALTER_TABLE);
	}

	/**
	 * @param add partition event
	 * @throws MetaException
	 */

	public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
		Iterator<Partition> addPartList = partitionEvent.getPartitionIterator();
		Table table = partitionEvent.getTable();
		boolean status = partitionEvent.getStatus();
	}

	/**
	 * @param partitionEvent partition event
	 * @throws MetaException
	 */
	public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
		Iterator<Partition> dropPartList = partitionEvent.getPartitionIterator();
		boolean deletePartData = partitionEvent.getDeleteData();
		boolean status = partitionEvent.getStatus();
		Table table = partitionEvent.getTable();
	}

	/**
	 * @param partitionEvent partition event
	 * @throws MetaException
	 */
	public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
		Partition newPart = partitionEvent.getNewPartition();
		Partition oldPart = partitionEvent.getOldPartition();
		boolean status = partitionEvent.getStatus();
		Table table = partitionEvent.getTable();
	}

	/**
	 * @param dbEvent database event
	 * @throws MetaException
	 */
	public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
		hmm.setCreateDatabase(dbEvent.getDatabase());
		hmm.setStatus(dbEvent.getStatus());
		hmm.setOpType(HiveMetaMessage.CREATE_DATABASE);
	}

	/**
	 * @param dbEvent database event
	 * @throws MetaException
	 */
	public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
		hmm.setDropDatabase(dbEvent.getDatabase());
		hmm.setStatus(dbEvent.getStatus());
		hmm.setOpType(HiveMetaMessage.DROP_DATABASE);
	}

	/**
	 * @param partSetDoneEvent
	 * @throws MetaException
	 */
	public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
		Map<String, String> partNames = partSetDoneEvent.getPartitionName();
		Table table = partSetDoneEvent.getTable();
		boolean status = partSetDoneEvent.getStatus();

	}
}
