package org.senia.hive;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetaConfig {
	private static final Logger LOG = LoggerFactory.getLogger(HiveMetaConfig.class);

	public static Configuration conf;
	public static HiveConf localHConf;
	public static HiveConf remoteHConf;
	public static String keytab = null;
	public static String keytabupn = null;

	public static final Object lock = new Object();

	public static boolean alldbs = false;
	public static boolean singleDb = false;
	public static boolean singleTb = false;
	public static boolean setKrb = false;
	public static boolean remoteMs = false;
	public static boolean remoteMsSpn = false;
	public static boolean remoteClusterName = false;
	public static boolean localClusterName = false;
	public static boolean exportJson = false;
	public static boolean importJson = false;
	public static boolean syncMeta = false;
	public static boolean dbExportComplete = false;
	public static boolean partExportComplete = false;
	public static boolean tableExportComplete = false;
	public static boolean remote_ms_sasl = false;
	public static boolean daemon = false;
	public static List<Integer> execTimeList;
	public static boolean execAllHours = false;
	public static boolean writeToHdfs = false;
	public static Integer daysToKeep = 7;

	public static String outputPath = "/tmp/";
	public static String db = "";
	public static String table = "";
	public static String remote_ms_uri;
	public static String remote_ms_spn;

	public static String remote_clusterName;
	public static String local_clusterName;

	public static String importJsonFile;

	public HiveMetaConfig() {

	}

	public static void init() {
		LOG.info("HiveMetaConfig Init Complete!");
	}
}
