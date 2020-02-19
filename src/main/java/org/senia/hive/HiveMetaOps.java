package org.senia.hive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.senia.hive.threads.DatabaseThread;
import org.senia.hive.threads.TableThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

public class HiveMetaOps extends Thread {
	public static UserGroupInformation ugi;
	private static final Logger LOG = LoggerFactory.getLogger(HiveMetaOps.class);
	List<Database> ldatabases;
	List<Table> ltables;
	List<Partition> lparts;
	ThreadGroup hsmtg;
	ThreadGroup tabletg;
	ThreadGroup dbtg;
	ThreadGroup parttg;
	HiveMetaData hmd;
	List<String> ltableList;
	List<String> localDbs;
	HiveObjectHelper gho;
	HiveMetaStoreClient lmsc;
	Gson gsonOutput;
	String jsonString;
	HiveMetaStoreClient rmsc;
	Writer writer;
	long currentTime;
	long daysMS;
	long deleteTime;
	Path hdfsHiveMetaJsonPath;
	FSDataOutputStream outputStream;
	FileSystem fs;
	Path outputFolderPath;
	long modifyTime = 0;
	FileStatus[] fileStatusList;

	public HiveMetaOps() {

	}

	@Override
	public void run() {
		executeHiveMetaOps();
	}

	public void executeHiveMetaOps() {
		if (UserGroupInformation.isSecurityEnabled()) {
			try {
				if (HiveMetaConfig.setKrb == true) {
					ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(HiveMetaConfig.keytabupn,
							HiveMetaConfig.keytab);
				} else {
					ugi = UserGroupInformation.getCurrentUser();
				}
				LOG.info("UserId for Hadoop: " + ugi.getUserName());
			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
				LOG.error("Exception Getting Credentials Exiting!");
				System.exit(1);
			}
			HiveMetaDataStore.getInstance();
			try {
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						LOG.info("HiveMetaConfig.exportJson: " + HiveMetaConfig.exportJson);
						LOG.info("HiveMetaConfig.syncMeta: " + HiveMetaConfig.syncMeta);
						LOG.info("HiveMetaConfig.importJso: " + HiveMetaConfig.importJson);

						if (HiveMetaConfig.exportJson || HiveMetaConfig.syncMeta) {
							LOG.info("Sync or Export Metastore Objects");
							syncOrExportOp();
						}
						if (HiveMetaConfig.importJson) {
							LOG.info("Import Metastore Objects");
							importOp();
						}
						return null;
					}
				});
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			if (HiveMetaConfig.exportJson || HiveMetaConfig.syncMeta) {
				LOG.info("Sync or Export Metastore Objects");
				syncOrExportOp();
			}
			if (HiveMetaConfig.importJson) {
				LOG.info("Import Metastore Objects");
				importOp();
			}
		}

	}

	public void importOp() {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(HiveMetaConfig.importJsonFile));
			Gson gson = new Gson();
			HiveMetaData hmd = gson.fromJson(bufferedReader, HiveMetaData.class);
			try {
				lmsc = new HiveMetaStoreClient(HiveMetaConfig.localHConf);
			} catch (MetaException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			syncImportOp(hmd, lmsc);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void syncOrExportOp() {

		hmd = new HiveMetaData();
		// List<Database> ldatabases = new ArrayList<Database>();
		ldatabases = Collections.synchronizedList(new ArrayList<Database>());
		// List<Table> ltables = new ArrayList<Table>();
		ltables = Collections.synchronizedList(new ArrayList<Table>());
		/// List<Partition> lparts = new ArrayList<Partition>();
		lparts = Collections.synchronizedList(new ArrayList<Partition>());

		hsmtg = new ThreadGroup("HiveMetaStoreExportThreadGroup");
		tabletg = new ThreadGroup(hsmtg, "TableExportThreadGroup");
		dbtg = new ThreadGroup(hsmtg, "DatabaseExportThreadGroup");
		parttg = new ThreadGroup(hsmtg, "PartitionExportThreadGroup");

		try {
			lmsc = new HiveMetaStoreClient(HiveMetaConfig.localHConf);
			gho = new HiveObjectHelper(lmsc);

			if (HiveMetaConfig.alldbs) {
				localDbs = gho.getDatabases();
				DatabaseThread dbThread = new DatabaseThread(dbtg, "DatabaseExportThread", localDbs,
						HiveMetaConfig.localHConf, HiveMetaConfig.syncMeta, HiveMetaConfig.local_clusterName,
						HiveMetaConfig.remote_clusterName);
				dbThread.start();

				for (String localDbStr : localDbs) {
					ltableList = gho.getAllTables(localDbStr);
					TableThread tThread = new TableThread(parttg, tabletg, localDbStr, ltableList,
							HiveMetaConfig.localHConf, HiveMetaConfig.syncMeta, HiveMetaConfig.local_clusterName,
							HiveMetaConfig.remote_clusterName);
					tThread.start();

				}
				while (true) {
					synchronized (HiveMetaConfig.lock) {
						LOG.info("Total Thread Count: " + hsmtg.activeCount());
						LOG.info("Database Thread Count: " + dbtg.activeCount());
						LOG.info("Table Thread Count: " + tabletg.activeCount());
						LOG.info("Partition Thread Count: " + parttg.activeCount());

						if (hsmtg.activeCount() == 0) {
							hmd.setDatabases(HiveMetaDataStore.ldatabases);
							hmd.setTables(HiveMetaDataStore.ltables);
							hmd.setPartitions(HiveMetaDataStore.lparts);
							break;
						}
						try {
							HiveMetaConfig.lock.wait(5000L);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

				}
				lmsc.close();
			} else if ((!HiveMetaConfig.alldbs) && (HiveMetaConfig.singleDb)) {
				String localDbStr = HiveMetaConfig.db;
				try {
					Database ldb = gho.getDatabase(localDbStr);
					if (HiveMetaConfig.syncMeta) {
						ldb.setLocationUri(gho.replacePath(gho.getLocationUri(ldb), HiveMetaConfig.local_clusterName,
								HiveMetaConfig.remote_clusterName));
					}
					ldatabases.add(ldb);
					ltableList = gho.getAllTables(localDbStr);
					for (String ltableString : ltableList) {
						Table ltb = gho.getTable(localDbStr, ltableString);
						if (HiveMetaConfig.syncMeta) {
							ltb.setSd(gho.replacePath(gho.getSd(ltb), HiveMetaConfig.local_clusterName,
									HiveMetaConfig.remote_clusterName));
						}
						ltables.add(ltb);
						List<String> partList = gho.getPartitionKeys(ltb);
						List<Partition> parts = gho.getPartitionsByName(localDbStr, ltableString, partList);
						for (Partition part : parts) {
							if (HiveMetaConfig.syncMeta) {
								part.setSd(gho.replacePath(gho.getSd(part), HiveMetaConfig.local_clusterName,
										HiveMetaConfig.remote_clusterName));
							}
							lparts.add(part);
						}
					}
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				hmd.setDatabases(ldatabases);
				hmd.setTables(ltables);
				hmd.setPartitions(lparts);
			} else if (!HiveMetaConfig.db.isEmpty() && (!HiveMetaConfig.table.isEmpty())) {
				String localDbStr = HiveMetaConfig.db;
				String ltableString = HiveMetaConfig.table;
				Database ldb = gho.getDatabase(localDbStr);
				if (HiveMetaConfig.syncMeta) {
					ldb.setLocationUri(gho.replacePath(gho.getLocationUri(ldb), HiveMetaConfig.local_clusterName,
							HiveMetaConfig.remote_clusterName));
				}
				ldatabases.add(ldb);
				Table ltb = gho.getTable(localDbStr, ltableString);
				if (HiveMetaConfig.syncMeta) {
					ltb.setSd(gho.replacePath(gho.getSd(ltb), HiveMetaConfig.local_clusterName,
							HiveMetaConfig.remote_clusterName));
				}
				ltables.add(ltb);
				List<String> partList = gho.getPartitionKeys(ltb);
				List<Partition> parts = gho.getPartitionsByName(localDbStr, ltableString, partList);
				for (Partition part : parts) {
					if (HiveMetaConfig.syncMeta) {
						part.setSd(gho.replacePath(gho.getSd(part), HiveMetaConfig.local_clusterName,
								HiveMetaConfig.remote_clusterName));
					}
					lparts.add(part);
				}

				hmd.setDatabases(ldatabases);
				hmd.setTables(ltables);
				hmd.setPartitions(lparts);
			}
		} catch (MetaException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if (HiveMetaConfig.exportJson) {
			LOG.info("Outputing JSON");
			gsonOutput = new Gson();

			jsonString = gsonOutput.toJson(hmd);

			if (HiveMetaConfig.writeToHdfs) {
				hdfsJsonWriter(HiveMetaConfig.outputPath, jsonString);
			} else {
				jsonWriter(HiveMetaConfig.outputPath, jsonString);
			}
			HiveMetaDataStore.clearDatastore();

		} else if (HiveMetaConfig.syncMeta) {
			try {

				HiveMetaConfig.remoteHConf = new HiveConf();
				if (HiveMetaConfig.remoteMs) {
					HiveMetaConfig.remoteHConf.set("hive.metastore.uris", HiveMetaConfig.remote_ms_uri);
					if (HiveMetaConfig.remote_ms_sasl) {
						HiveMetaConfig.remoteHConf.setBoolean("hive.metastore.sasl.enabled", true);
					}
					if (HiveMetaConfig.remoteMsSpn) {
						HiveMetaConfig.remoteHConf.set("hive.metastore.kerberos.principal",
								HiveMetaConfig.remote_ms_spn);
					}
				}
				rmsc = new HiveMetaStoreClient(HiveMetaConfig.remoteHConf);
				syncImportOp(hmd, rmsc);
				HiveMetaDataStore.clearDatastore();

			} catch (MetaException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public void jsonWriter(String outputPath, String outString) {

		try {
			currentTime = System.currentTimeMillis();
			outputPath = outputPath + "/" + currentTime + ".json";
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "utf-8"));
			writer.write(outString);
			writer.close();
		} catch (IOException ex) {
			// report
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
			}
		}
	}

	public void hdfsJsonWriter(String outputPath, String outString) {
		currentTime = System.currentTimeMillis();
		daysMS = HiveMetaConfig.daysToKeep * 86400000L;
		deleteTime = currentTime - daysMS;
		modifyTime = 0;
		try {
			fs = FileSystem.get(HiveMetaConfig.conf);
			outputFolderPath = new Path(outputPath);
			if (!fs.exists(outputFolderPath)) {
				// Create new Directory
				fs.mkdirs(outputFolderPath);
				LOG.info("hdfsOutputPath " + outputPath + " created.");
			} else {
				fileStatusList = fs.listStatus(outputFolderPath);
				for (FileStatus fileStatus : fileStatusList) {
					modifyTime = fileStatus.getModificationTime();
					if (modifyTime <= deleteTime) {
						fs.delete(fileStatus.getPath(), false);
					}
				}
			}

			hdfsHiveMetaJsonPath = new Path(outputPath + "/" + currentTime + ".json");
			outputStream = fs.create(hdfsHiveMetaJsonPath);
			outputStream.writeBytes(outString);
			outputStream.close();
			fs.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void syncImportOp(HiveMetaData hmd, HiveMetaStoreClient msc) {
		ldatabases = hmd.getDatabases();
		ltables = hmd.getTables();
		lparts = hmd.getPartitions();

		if (!ldatabases.isEmpty()) {
			for (Database ldatabase : ldatabases) {
				try {
					msc.createDatabase(ldatabase);
				} catch (AlreadyExistsException e) {
					LOG.info("database: " + ldatabase.getName() + " already exists");
				} catch (InvalidObjectException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		if (!ltables.isEmpty()) {
			for (Table ltable : ltables) {
				try {
					msc.createTable(ltable);
				} catch (AlreadyExistsException e) {
					System.out
							.println("table: " + ltable.getDbName() + "." + ltable.getTableName() + " already exists");
				} catch (InvalidObjectException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		if (!lparts.isEmpty()) {
			for (Partition lpart : lparts) {
				try {
					msc.add_partition(lpart);
				} catch (AlreadyExistsException e) {
					LOG.info("table: " + lpart.getDbName() + "." + lpart.getTableName() + " partition="
							+ lpart.toString() + " already exists");
				} catch (InvalidObjectException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
