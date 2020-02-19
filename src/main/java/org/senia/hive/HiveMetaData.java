package org.senia.hive;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveMetaData {

	public List<Database> databases;
	public List<Table> tables;
	public List<Partition> partitions;

	public List<Database> getDatabases() {
		return this.databases;
	}

	public void setDatabases(List<Database> databases) {
		this.databases = databases;
	}

	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	public List<Table> getTables() {
		return this.tables;
	}

	public List<Partition> getPartitions() {
		return this.partitions;
	}

	public void setPartitions(List<Partition> partitions) {
		this.partitions = partitions;
	}
}
