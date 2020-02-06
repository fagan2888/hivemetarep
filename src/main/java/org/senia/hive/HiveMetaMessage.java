package org.senia.hive;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveMetaMessage {

	public final static String CREATE_TABLE = "createTable";
	public final static String DROP_TABLE = "dropTable";
	public final static String ALTER_TABLE = "alterTable";
	public final static String CREATE_DATABASE = "createDatabase";
	public final static String DROP_DATABASE = "dropDatabase";
	public final static String ADD_PARTITION = "addPartition";
	public final static String ALTER_PARTITION = "alterPartition";
	public final static String DROP_PARTITION = "dropPartition";
	public final static String LOAD_PARTITION = "loadPartition";
	public final static String ADD_INDEX = "addIndex";
	public final static String DROP_INDEX = "dropIndex";
	public final static String ALTER_INDEX = "alterIndex";
	public final static String INSERT_INDEX = "insertIndex";

	
	public boolean status;
	public boolean deleteData;
	public String opType;

	public Table createTable;
	public Table dropTable;
	public Table alterTableNew;
	public Table alterTableOld;

	public Database createDatabase;
	public Database dropDatabase;

	public Partition partition;
	public Index index;

	public boolean getStatus() {
		return this.status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public boolean getDeleteData() {
		return this.deleteData;
	}

	public void setDeleteData(boolean deleteData) {
		this.deleteData = deleteData;
	}

	public String getOpType() {
		return this.opType;
	}

	public void setOpType(String opType) {
		this.opType = opType;
	}

	public Table getDropTable() {
		return this.dropTable;
	}

	public void setDropTable(Table dropTable) {
		this.dropTable = dropTable;
	}
	public Table getAlterTableNew() {
		return this.alterTableNew;
	}

	public void setAlterTableNew(Table alterTableNew) {
		this.alterTableNew = alterTableNew;
	}
	public Table getAlterTableOld() {
		return this.alterTableOld;
	}

	public void setAlterTableOld(Table alterTableOld) {
		this.alterTableOld = alterTableOld;
	}		

	public Table getCreateTable() {
		return this.createTable;
	}

	public void setCreateTable(Table createTable) {
		this.createTable = createTable;
	}

	public Database getDropDatabase() {
		return this.dropDatabase;
	}

	public void setDropDatabase(Database dropDatabase) {
		this.dropDatabase = dropDatabase;
	}
	
	public Database getCreateDatabase() {
		return this.createDatabase;
	}

	public void setCreateDatabase(Database createDatabase) {
		this.createDatabase = createDatabase;
	}	

	public Partition getPartition() {
		return this.partition;
	}

	public void setPartition(Partition partition) {
		this.partition = partition;
	}

	public Index getIndex() {
		return this.index;
	}

	public void setIndex(Index index) {
		this.index = index;
	}

}
