package org.senia.hive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.thrift.TException;
import org.senia.hive.threads.DatabaseThread;
import org.senia.hive.threads.TableThread;

import com.google.gson.Gson;

public class HiveMetaLoad {
	public static UserGroupInformation ugi;
	public static HiveConf localHConf;
	public static HiveConf remoteHConf;
	static String keytab = null;
	static String keytabupn = null;
	static boolean setKrb = false;
	private final static Object lock = new Object();

	static boolean alldbs = false;
	static String db = "";
	static boolean singleDb = false;
	static boolean singleTb = false;

	static String table = "";
	static String remote_ms_uri;
	static boolean remoteMs = false;
	static boolean remoteMsSpn = false;
	static boolean remoteClusterName = false;
	static boolean localClusterName = false;
	static boolean exportJson = false;
	static boolean importJson = false;
	static boolean syncMeta = false;
	static String importJsonFile;
	static boolean dbExportComplete = false;
	static boolean partExportComplete = false;
	static boolean tableExportComplete = false;

	static String remote_clusterName;
	static String local_clusterName;

	static String remote_ms_spn;
	static boolean remote_ms_sasl = false;

	static Options options = new Options();

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();

		String[] otherArgs = null;

		try {
			otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			e4.printStackTrace();
		}

		options = new Options();
		options.addOption("export", false, "export as json file");
		options.addOption("import", true, "import json file --import hivemeta.json");
		options.addOption("sync", false, "sync cluster a to b");
		options.addOption("alldbs", false, "replicate all DBs and Tables --alldbs");
		options.addOption("db", true, "DB Name --db (database name)");
		options.addOption("table", true, "Table Name --table (table name)");
		options.addOption("remote_ms_uri", true,
				"(Sync Operation Only) Remote Metastore Hostname --remote_ms_uri (remote uri)");
		options.addOption("remote_ms_spn", true,
				"(Sync Operation Only) Remote Metastore Service Principal --remote_ms_spn (remote host service principal)");
		options.addOption("remote_ms_sasl", false,
				"(Sync Operation Only) Remote Metastore SASL Enabled --remote_mshost_sasl");
		options.addOption("remote_cluster_name", true,
				"(Sync Operation Only) Remote cluster Name --remote_cluster_name (tech)");
		options.addOption("local_cluster_name", true,
				"(Sync OperationOnly ) Local cluster Name --local_cluster_name (unit)");
		options.addOption("krb_keytab", true,
				"Keytab File to Connect to Hive Metastores --krb_keytab $HOME/S00000.keytab");
		options.addOption("krb_upn", true,
				"Kerberos Princpial for Keytab to Connect to Hive Metastores --krb_upn S00000@EXAMP.EXAMPLE.COM");
		options.addOption("help", false, "Display help");
		CommandLineParser parser = new HiveMetaParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, otherArgs);
		} catch (ParseException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		if (cmd.hasOption("remote_ms_sasl")) {
			remote_ms_sasl = true;
		}
		if (cmd.hasOption("remote_ms_uri")) {
			remoteMs = true;
			remote_ms_uri = cmd.getOptionValue("remote_ms_uri");
		}

		if (cmd.hasOption("remote_ms_spn")) {
			if (!(cmd.hasOption("remote_ms_sasl"))) {
				remote_ms_sasl = true;
			}
			remoteMsSpn = true;
			remote_ms_spn = cmd.getOptionValue("remote_ms_spn");
		}
		// Used for SyncOps
		if (cmd.hasOption("remote_cluster_name")) {
			remoteClusterName = true;
			remote_clusterName = cmd.getOptionValue("remote_cluster_name");
		}
		if (cmd.hasOption("local_cluster_name")) {
			localClusterName = true;
			local_clusterName = cmd.getOptionValue("local_cluster_name");
		}

		if (cmd.hasOption("alldbs")) {
			alldbs = true;
		}
		if (cmd.hasOption("db")) {
			singleDb = true;
			db = cmd.getOptionValue("db");
		}
		if (cmd.hasOption("table") && cmd.hasOption("db")) {
			if (!(cmd.hasOption("db"))) {
				singleDb = true;
				db = "default";
			}
			singleTb = true;
			table = cmd.getOptionValue("table");
		}

		if (cmd.hasOption("export")) {
			exportJson = true;
		} else if (cmd.hasOption("sync")) {
			syncMeta = true;
			if (!remoteMs) {
				System.out.println("!!Remote Metastore Not Set!!");
				missingParams();
				System.exit(1);
			}
		} else if (cmd.hasOption("import")) {
			importJson = true;
			importJsonFile = cmd.getOptionValue("import");
		} else {
			System.out.println("Not Sync, Import or Export");
			missingParams();
			System.exit(1);
		}

		if (cmd.hasOption("krb_keytab") && cmd.hasOption("krb_upn")) {
			setKrb = true;
			keytab = cmd.getOptionValue("krb_keytab");
			keytabupn = cmd.getOptionValue("krb_upn");
			File keytabFile = new File(keytab);
			if (keytabFile.exists()) {
				if (!(keytabFile.canRead())) {
					System.out.println("Keytab exists but cannot read it - exiting");
					missingParams();
					System.exit(1);
				}
			} else {
				System.out.println("Keytab doesn't exist  - exiting");
				missingParams();
				System.exit(1);
			}
		}

		if (cmd.hasOption("help")) {
			missingParams();
			System.exit(0);
		}

		if (!alldbs && !singleDb) {
			missingParams();
			System.exit(0);
		}
		LogManager.getRootLogger().setLevel(Level.WARN);


		UserGroupInformation.setConfiguration(conf);

		if (UserGroupInformation.isSecurityEnabled()) {
			try {
				if (setKrb == true) {
					ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(keytabupn, keytab);
				} else {
					ugi = UserGroupInformation.getCurrentUser();
				}
				System.out.println("UserId for Hadoop: " + ugi.getUserName());
			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
				System.out.println("Exception Getting Credentials Exiting!");
				System.exit(1);
			}
			HiveMetaDataStore.getInstance();
			try {
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						if (exportJson || syncMeta) {
							syncOrExportOp();
						}
						if (importJson) {
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
			if (exportJson || syncMeta) {
				syncOrExportOp();
			}
			if (importJson) {
				importOp();
			}
		}

	}

	public static void importOp() {
		try {
			BufferedReader bufferedReader = new BufferedReader(new FileReader(importJsonFile));
			Gson gson = new Gson();
			HiveMetaData hmd = gson.fromJson(bufferedReader, HiveMetaData.class);
			HiveMetaStoreClient lmsc = null;
			try {
				lmsc = new HiveMetaStoreClient(localHConf);
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

	public static void syncOrExportOp() {

		HiveMetaData hmd = new HiveMetaData();
		// List<Database> ldatabases = new ArrayList<Database>();
		List<Database> ldatabases = Collections.synchronizedList(new ArrayList<Database>());
		// List<Table> ltables = new ArrayList<Table>();
		List<Table> ltables = Collections.synchronizedList(new ArrayList<Table>());

		// List<Partition> lparts = new ArrayList<Partition>();
		List<Partition> lparts = Collections.synchronizedList(new ArrayList<Partition>());

		List<String> ltableList;
		List<String> localDbs;
		ThreadGroup hsmtg = new ThreadGroup("HiveMetaStoreExportThreadGroup");
		ThreadGroup tabletg = new ThreadGroup(hsmtg, "TableExportThreadGroup");
		ThreadGroup dbtg = new ThreadGroup(hsmtg, "DatabaseExportThreadGroup");
		ThreadGroup parttg = new ThreadGroup(hsmtg, "PartitionExportThreadGroup");



		HiveMetaStoreClient lmsc;
		try {
			lmsc = new HiveMetaStoreClient(localHConf);
			GetHiveObjects gho = new GetHiveObjects(lmsc);

			if (alldbs) {
				localDbs = gho.getDatabases();
				DatabaseThread dbThread = new DatabaseThread(dbtg, "DatabaseExportThread", localDbs, localHConf,
						syncMeta, local_clusterName, remote_clusterName);
				dbThread.start();

				for (String localDbStr : localDbs) {
					ltableList = gho.getAllTables(localDbStr);
					TableThread tThread = new TableThread(parttg, tabletg, localDbStr, ltableList, localHConf, syncMeta,
							local_clusterName, remote_clusterName);
					tThread.start();

				}
				while (true) {
					synchronized (lock) {
						System.out.println("Total Thread Count: " + hsmtg.activeCount());
						System.out.println("Database Thread Count: " + dbtg.activeCount());
						System.out.println("Table Thread Count: " + tabletg.activeCount());
						System.out.println("Partition Thread Count: " + parttg.activeCount());
						
						if (hsmtg.activeCount() == 0) {
							hmd.setDatabases(HiveMetaDataStore.ldatabases);
							hmd.setTables(HiveMetaDataStore.ltables);
							hmd.setPartitions(HiveMetaDataStore.lparts);
							break;
						}
						try {
							lock.wait(5000L);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

				}
				lmsc.close();
			}
			else if ((!alldbs) && (singleDb)) {
				String localDbStr = db;
				try {
					Database ldb = gho.getDatabase(localDbStr);
					if (syncMeta) {
						ldb.setLocationUri(
								gho.replacePath(gho.getLocationUri(ldb), local_clusterName, remote_clusterName));
					}
					ldatabases.add(ldb);
					ltableList = gho.getAllTables(localDbStr);
					for (String ltableString : ltableList) {
						Table ltb = gho.getTable(localDbStr, ltableString);
						if (syncMeta) {
							ltb.setSd(gho.replacePath(gho.getSd(ltb), local_clusterName, remote_clusterName));
						}
						ltables.add(ltb);
						List<String> partList = gho.getPartitionKeys(ltb);
						List<Partition> parts = gho.getPartitionsByName(localDbStr, ltableString, partList);
						for (Partition part : parts) {
							if (syncMeta) {
								part.setSd(gho.replacePath(gho.getSd(part), local_clusterName, remote_clusterName));
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
			} else if (!db.isEmpty() && (!table.isEmpty())) {
				String localDbStr = db;
				String ltableString = table;
				Database ldb = gho.getDatabase(localDbStr);
				if (syncMeta) {
					ldb.setLocationUri(gho.replacePath(gho.getLocationUri(ldb), local_clusterName, remote_clusterName));
				}
				ldatabases.add(ldb);
				Table ltb = gho.getTable(localDbStr, ltableString);
				if (syncMeta) {
					ltb.setSd(gho.replacePath(gho.getSd(ltb), local_clusterName, remote_clusterName));
				}
				ltables.add(ltb);
				List<String> partList = gho.getPartitionKeys(ltb);
				List<Partition> parts = gho.getPartitionsByName(localDbStr, ltableString, partList);
				for (Partition part : parts) {
					if (syncMeta) {
						part.setSd(gho.replacePath(gho.getSd(part), local_clusterName, remote_clusterName));
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

		if (exportJson) {
			System.out.println("Outputing JSON");
			Gson gsonOutput = new Gson();

			String jsonString = gsonOutput.toJson(hmd);

			jsonWriter("/tmp/hivemeta.json", jsonString);

		} else if (syncMeta) {
			try {

				remoteHConf = new HiveConf();
				if (remoteMs) {
					remoteHConf.set("hive.metastore.uris", remote_ms_uri);
					if (remote_ms_sasl) {
						remoteHConf.setBoolean("hive.metastore.sasl.enabled", true);
					}
					if (remoteMsSpn) {
						remoteHConf.set("hive.metastore.kerberos.principal", remote_ms_spn);
					}
				}
				HiveMetaStoreClient rmsc = new HiveMetaStoreClient(remoteHConf);
				syncImportOp(hmd, rmsc);
			} catch (MetaException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static void jsonWriter(String outputPath, String outString) {

		Writer writer = null;

		try {
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

	public static void syncImportOp(HiveMetaData hmd, HiveMetaStoreClient msc) {
		List<Database> ldatabases = hmd.getDatabases();
		List<Table> ltables = hmd.getTables();
		List<Partition> lparts = hmd.getPartitions();

		if (!ldatabases.isEmpty()) {
			for (Database ldatabase : ldatabases) {
				try {
					msc.createDatabase(ldatabase);
				} catch (AlreadyExistsException e) {
					System.out.println("database: " + ldatabase.getName() + " already exists");
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
					System.out.println("table: " + lpart.getDbName() + "." + lpart.getTableName() + " partition="
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

	private static void missingParams() {
		String header = "HiveMetaLoad";
		String footer = "\nPlease report issues";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
	}

}