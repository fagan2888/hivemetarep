package org.senia.hive;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;

public class HiveMetaRep extends MetaStoreEventListener {

	public HiveMetaRep(Configuration config) {
		super(config);
		// TODO Auto-generated constructor stub
	}

	public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {

		Table createTable = tableEvent.getTable();
		boolean status = tableEvent.getStatus();

	}

	/**
	 * @param tableEvent table event.
	 * @throws MetaException
	 */
	public void onDropTable(DropTableEvent tableEvent) throws MetaException {

		boolean deleteData = tableEvent.getDeleteData();
		Table dropTable = tableEvent.getTable();
		boolean status = tableEvent.getStatus();

	}

	/**
	 * @param tableEvent alter table event
	 * @throws MetaException
	 */
	public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
		Table newTable = tableEvent.getNewTable();
		Table oldTable = tableEvent.getOldTable();
		boolean status = tableEvent.getStatus();

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
		Database createDb = dbEvent.getDatabase();
		boolean status = dbEvent.getStatus();
	}

	/**
	 * @param dbEvent database event
	 * @throws MetaException
	 */
	public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
		Database dropDb = dbEvent.getDatabase();
		boolean status = dbEvent.getStatus();
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
