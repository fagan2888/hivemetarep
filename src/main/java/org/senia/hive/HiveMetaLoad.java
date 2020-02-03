package org.senia.hive;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.thrift.TException;

import com.google.gson.Gson;

public class HiveMetaLoad {
	public static UserGroupInformation ugi;
	public static HiveConf localHConf;
	public static HiveConf remoteHConf;
	static String keytab = null;
	static String keytabupn = null;
	static boolean setKrb = true;

	static boolean alldbs = false;
	static String db;
	static boolean singleDb = false;
	static boolean singleTb = false;

	static String table;
	static String remote_ms_uri;
	static boolean remoteMs = false;
	static boolean remoteMsSpn = false;
	static boolean remoteClusterName = false;
	static boolean localClusterName = false;
	static boolean dumpJson = false;
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
		options.addOption("alldbs", false, "replicate all DBs and Tables --alldbs");
		options.addOption("db", true, "DB Name --db (database name)");
		options.addOption("table", true, "Table Name --table (table name)");
		options.addOption("remote_ms_uri", true, "Remote Metastore Hostname --remote_ms_uri (remote uri)");
		options.addOption("remote_ms_spn", true,
				"Remote Metastore Service Principal --remote_ms_spn (remote host service principal)");
		options.addOption("remote_ms_sasl", false, "Remote Metastore SASL Enabled --remote_mshost_sasl");
		options.addOption("remote_cluster_name", true, "Remote cluster Name --remote_cluster_name (tech)");
		options.addOption("local_cluster_name", true, "Local cluster Name --local_cluster_name (unit)");
		options.addOption("dump_json", false, "Dump the output to JSON");
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
			remoteMsSpn = true;
			remote_ms_spn = cmd.getOptionValue("remote_ms_spn");
		}
		if (cmd.hasOption("remote_cluster_name")) {
			remoteClusterName = true;
			remote_clusterName = cmd.getOptionValue("remote_cluster_name");
		}
		if (cmd.hasOption("local_cluster_name")) {
			localClusterName = true;
			local_clusterName = cmd.getOptionValue("local_cluster_name");
		}
		if (cmd.hasOption("dump_json")) {
			dumpJson = true;
		}

		if (cmd.hasOption("remote_ms_spn")) {
			remoteMsSpn = true;
			remote_ms_spn = cmd.getOptionValue("remote_ms_spn");
		}
		if (cmd.hasOption("alldbs")) {
			alldbs = true;
		}
		if (cmd.hasOption("db")) {
			singleDb = true;
			db = cmd.getOptionValue("db");
		}
		if (cmd.hasOption("table")) {
			singleTb = true;
			table = cmd.getOptionValue("table");
		}

		if (cmd.hasOption("db")) {
			db = cmd.getOptionValue("db");
		}
		if (cmd.hasOption("table")) {
			table = cmd.getOptionValue("table");
		}

		if (cmd.hasOption("krb_keytab") && cmd.hasOption("krb_upn")) {
			setKrb = true;
			keytab = cmd.getOptionValue("krb_keytab");
			keytabupn = cmd.getOptionValue("krb_upn");
			File keytabFile = new File(keytab);
			if (keytabFile.exists()) {
				if (!(keytabFile.canRead())) {
					System.out.println("Keytab  exists but cannot read it - exiting");
					System.exit(1);
				}
			} else {
				System.out.println("Keytab doesn't exist  - exiting");
				System.exit(1);
			}
		}

		if (cmd.hasOption("help")) {
			System.exit(0);
		}

		localHConf = new HiveConf();
		remoteHConf = localHConf;
		if (remoteMs) {
			remoteHConf.set("hive.metastore.uris", remote_ms_uri);
			if (remote_ms_sasl) {
				remoteHConf.setBoolean("hive.metastore.sasl.enabled", true);
			}
			if (remoteMsSpn) {
				remoteHConf.set("hive.metastore.kerberos.principal", remote_ms_spn);
			}
		}

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
			try {
				ugi.doAs(new PrivilegedExceptionAction<Void>() {
					public Void run() throws Exception {
						syncOp();
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
			syncOp();
		}

	}

	public static void syncOp() {
		List<Database> ldatabases = new ArrayList<Database>();
		List<Table> ltables = new ArrayList<Table>();
		List<Partition> lparts = new ArrayList<Partition>();

		try {
			HiveMetaStoreClient lmsc = new HiveMetaStoreClient(localHConf);

			if (alldbs) {
				List<String> ltableList;
				List<String> localDbs = lmsc.getAllDatabases();
				StorageDescriptor lsd = null;
				for (String localDbStr : localDbs) {
					try {
						Database ldb = lmsc.getDatabase(localDbStr);
						if (ldb.getLocationUri().contains("hdfs://" + local_clusterName)) {
							ldb.setLocationUri(ldb.getLocationUri().replace(local_clusterName, remote_clusterName));
						}
						ldatabases.add(ldb);
						ltableList = lmsc.getAllTables(localDbStr);

						for (String ltableString : ltableList) {
							Table ltb = lmsc.getTable(localDbStr, ltableString);
							lsd = ltb.getSd();
							if (lsd.getLocation().contains("hdfs://" + local_clusterName)) {
								lsd.setLocation(lsd.getLocation().replace(local_clusterName, remote_clusterName));
								ltb.setSd(lsd);
							}
							ltables.add(ltb);
						}
					} catch (NoSuchObjectException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			if ((!alldbs) && (singleDb)) {
				List<String> ltableList;
				Database ldb = lmsc.getDatabase(db);
				StorageDescriptor lsd = null;
				StorageDescriptor psd = null;

				if (ldb.getLocationUri().contains("hdfs://" + local_clusterName)) {
					ldb.setLocationUri(ldb.getLocationUri().replace(local_clusterName, remote_clusterName));
				}
				ldatabases.add(ldb);
				ltableList = lmsc.getAllTables(db);
				if (singleTb) {
					ArrayList<String> partKeyList = new ArrayList<String>();
					Table ltb = lmsc.getTable(db, table);
					lsd = ltb.getSd();
					if (lsd.getLocation().contains("hdfs://" + local_clusterName)) {
						lsd.setLocation(lsd.getLocation().replace(local_clusterName, remote_clusterName));
						ltb.setSd(lsd);
					}
					ltables.add(ltb);
					List<FieldSchema> partKeys = ltb.getPartitionKeys();
					for (FieldSchema partKey : partKeys) {
						partKeyList.add(partKey.getName());
					}
					List<Partition> parts = lmsc.getPartitionsByNames(db, table, partKeyList);
					for (Partition part : parts) {
						psd = part.getSd();
						if (psd.getLocation().contains("hdfs://" + local_clusterName)) {
							psd.setLocation(psd.getLocation().replace(local_clusterName, remote_clusterName));
							part.setSd(psd);
							lparts.add(part);
						}
					}
				} else {
					for (String ltableString : ltableList) {
						ArrayList<String> partKeyList = new ArrayList<String>();
						Table ltb = lmsc.getTable(db, ltableString);
						lsd = ltb.getSd();
						if (lsd.getLocation().contains("hdfs://" + local_clusterName)) {
							lsd.setLocation(lsd.getLocation().replace(local_clusterName, remote_clusterName));
							ltb.setSd(lsd);
						}
						ltables.add(ltb);
						List<FieldSchema> partKeys = ltb.getPartitionKeys();
						for (FieldSchema partKey : partKeys) {
							partKeyList.add(partKey.getName());
						}
						List<Partition> parts = lmsc.getPartitionsByNames(db, ltableString, partKeyList);
						for (Partition part : parts) {
							psd = part.getSd();
							if (psd.getLocation().contains("hdfs://" + local_clusterName)) {
								psd.setLocation(psd.getLocation().replace(local_clusterName, remote_clusterName));
								part.setSd(psd);
								lparts.add(part);
							}
						}
					}
				}

			}
		} catch (NoSuchObjectException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (dumpJson) {
			Gson gsonDb = new Gson();
			Gson gsonTb = new Gson();
			Gson gsonPart = new Gson();

			String tbString = gsonTb.toJson(ltables);
			String dbString = gsonDb.toJson(ldatabases);
			String partString = gsonPart.toJson(lparts);

			jsonWriter("/tmp/db.json", dbString);
			jsonWriter("/tmp/tb.json", tbString);
			jsonWriter("/tmp/parts.json", partString);

		} else {
			try {
				HiveMetaStoreClient rmsc = new HiveMetaStoreClient(remoteHConf);
				if (!ldatabases.isEmpty()) {
					for (Database ldatabase : ldatabases) {
						try {
							rmsc.createDatabase(ldatabase);
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
							rmsc.createTable(ltable);
						} catch (AlreadyExistsException e) {
							System.out.println(
									"table: " + ltable.getDbName() + "." + ltable.getTableName() + " already exists");
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
							rmsc.add_partition(lpart);
						} catch (AlreadyExistsException e) {
							System.out.println("table: " + lpart.getDbName() + "." + lpart.getTableName()
									+ " partition=" + lpart.toString() + " already exists");
						} catch (InvalidObjectException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (TException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
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

}