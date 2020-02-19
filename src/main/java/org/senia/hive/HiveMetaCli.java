package org.senia.hive;

import java.io.File;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

public class HiveMetaCli {

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
		options.addOption("export", false, "export");
		options.addOption("exportPath", true, "export to path --exportPath /tmp");
		options.addOption("useHdfs", false, "useHdfs to export or import");
		options.addOption("import", true, "import from json file --import hivemeta.json");
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
			HiveMetaConfig.remote_ms_sasl = true;
		}
		if (cmd.hasOption("remote_ms_uri")) {
			HiveMetaConfig.remoteMs = true;
			HiveMetaConfig.remote_ms_uri = cmd.getOptionValue("remote_ms_uri");
		}

		if (cmd.hasOption("remote_ms_spn")) {
			if (!(cmd.hasOption("remote_ms_sasl"))) {
				HiveMetaConfig.remote_ms_sasl = true;
			}
			HiveMetaConfig.remoteMsSpn = true;
			HiveMetaConfig.remote_ms_spn = cmd.getOptionValue("remote_ms_spn");
		}
		// Used for SyncOps
		if (cmd.hasOption("remote_cluster_name")) {
			HiveMetaConfig.remoteClusterName = true;
			HiveMetaConfig.remote_clusterName = cmd.getOptionValue("remote_cluster_name");
		}
		if (cmd.hasOption("local_cluster_name")) {
			HiveMetaConfig.localClusterName = true;
			HiveMetaConfig.local_clusterName = cmd.getOptionValue("local_cluster_name");
		}

		if (cmd.hasOption("alldbs")) {
			HiveMetaConfig.alldbs = true;
		}
		if (cmd.hasOption("db")) {
			HiveMetaConfig.singleDb = true;
			HiveMetaConfig.db = cmd.getOptionValue("db");
		}
		if (cmd.hasOption("useHdfs")) {
			HiveMetaConfig.writeToHdfs = true;
		}
		if (cmd.hasOption("table") && cmd.hasOption("db")) {
			if (!(cmd.hasOption("db"))) {
				HiveMetaConfig.singleDb = true;
				HiveMetaConfig.db = "default";
			}
			HiveMetaConfig.singleTb = true;
			HiveMetaConfig.table = cmd.getOptionValue("table");
		}

		if (cmd.hasOption("export")) {
			if (cmd.hasOption("exportPath")) {
				HiveMetaConfig.outputPath = cmd.getOptionValue("exportPath");
			}
			HiveMetaConfig.exportJson = true;
		} else if (cmd.hasOption("sync")) {
			HiveMetaConfig.syncMeta = true;
			if (!HiveMetaConfig.remoteMs) {
				System.out.println("!!Remote Metastore Not Set!!");
				missingParams();
				System.exit(1);
			}
		} else if (cmd.hasOption("import")) {
			HiveMetaConfig.importJson = true;
			HiveMetaConfig.importJsonFile = cmd.getOptionValue("import");
		} else {
			System.out.println("Not Sync, Import or Export");
			missingParams();
			System.exit(1);
		}

		if (cmd.hasOption("krb_keytab") && cmd.hasOption("krb_upn")) {
			HiveMetaConfig.setKrb = true;
			HiveMetaConfig.keytab = cmd.getOptionValue("krb_keytab");
			HiveMetaConfig.keytabupn = cmd.getOptionValue("krb_upn");
			File keytabFile = new File(HiveMetaConfig.keytab);
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

		if (!HiveMetaConfig.alldbs && !HiveMetaConfig.singleDb) {
			missingParams();
			System.exit(0);
		}
		Level level = LogManager.getRootLogger().getLevel();
		if (level == Level.DEBUG) {
			LogManager.getLogger(org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class).setLevel(Level.DEBUG);
		} else if (level == Level.TRACE) {
			LogManager.getLogger(org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class).setLevel(Level.TRACE);
		} else {
			LogManager.getLogger(org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class).setLevel(Level.WARN);
		}

		UserGroupInformation.setConfiguration(conf);
		HiveMetaConfig.conf = conf;
		HiveMetaOps hmo = new HiveMetaOps();
		hmo.executeHiveMetaOps();

	}

	private static void missingParams() {
		String header = "HiveMetaLoad";
		String footer = "\nPlease report issues";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
	}

}