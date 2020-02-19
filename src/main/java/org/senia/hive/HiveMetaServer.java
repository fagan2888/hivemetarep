package org.senia.hive;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetaServer implements Daemon {
	private static final Logger LOG = LoggerFactory.getLogger(HiveMetaServer.class);
	static Options options = new Options();
	HiveMetaExportRunnable hivemetaHealthThread;
	static String propsFile;
	ScheduledExecutorService executor;

	@Override
	public void init(DaemonContext context) throws DaemonInitException, Exception {
		// TODO Auto-generated method stub

		Level level = LogManager.getRootLogger().getLevel();
		if (level == Level.DEBUG) {
			LogManager.getLogger(org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class).setLevel(Level.DEBUG);
		} else if (level == Level.TRACE) {
			LogManager.getLogger(org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class).setLevel(Level.TRACE);
		} else {
			LogManager.getLogger(org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class).setLevel(Level.WARN);
		}

		HiveMetaConfig.init();
		HiveMetaConfig.daemon = true;
		// LOG.debug("Daemon initialized with arguments {}.",
		// context.getArguments().toString());
		LOG.info("Daemon initialized with arguments:");
		String[] contextArgs = context.getArguments();
		for (int i = 0; i < contextArgs.length; i++) {
			LOG.info("  " + contextArgs[i]);
		}
		Configuration conf = new Configuration();
		String[] otherArgs = null;
		try {
			otherArgs = new GenericOptionsParser(conf, context.getArguments()).getRemainingArgs();
		} catch (IOException e4) {
			// TODO Auto-generated catch block
			LOG.error(e4.getMessage());
		}

		options = new Options();
		options.addOption("propsfile", true, "config file for executing in scheduled batch mode");
		options.addOption("help", false, "Display help");

		CommandLineParser parser = new HiveMetaParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, otherArgs);
		} catch (ParseException e2) {
			// TODO Auto-generated catch block
			LOG.error(e2.getMessage());
		}
		if (cmd.hasOption("propsfile")) {
			propsFile = cmd.getOptionValue("propsfile");
		} else {
			System.out.println("propsfile is not set exiting");
			missingParams();
			System.exit(1);
		}
		if (!(new File(propsFile).exists())) {
			System.out.println("propsfile: " + propsFile + " does not exist");
			missingParams();
			System.exit(1);
		}

		setProps();
		UserGroupInformation.setConfiguration(conf);
		HiveMetaConfig.conf = conf;
	}

	@Override
	public void start() throws Exception {
		LocalDateTime start = LocalDateTime.now();
		// Hour + 1, set Minute and Second to 00
		LocalDateTime end = start.plusHours(1).truncatedTo(ChronoUnit.HOURS);
		LOG.debug("delayStartTime: " + end);

		// Get Duration for delay
		Duration duration = Duration.between(start, end);
		long delayMillis = duration.toMillis();
		HiveMetaExportRunnable hmer = new HiveMetaExportRunnable();

		executor = new ScheduledThreadPoolExecutor(1);
		executor.scheduleAtFixedRate(hmer, delayMillis, 3600000, TimeUnit.MILLISECONDS);
	}

	@Override
	public void stop() throws Exception {
		// TODO Auto-generated method stub
		// hivemetaHealthThread.interrupt();
		executor.shutdown();
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		// hivemetaHealthThread.interrupt();
		executor.shutdownNow();
	}

	private static void missingParams() {
		String header = "HiveMetaExport Engine";
		String footer = "\nPlease report issues at http://github.com/gss2002";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("get", header, options, footer, true);
		System.exit(0);
	}

	public static void setProps() {
		Properties props = new Properties();
		List<Integer> execHoursListInt;
		String[] execHours = null;
		String execHoursStr = "";
		Integer execHourInt;
		try (InputStream input = new FileInputStream(propsFile)) {
			props.load(input);
			HiveMetaConfig.exportJson = true;
			if (props.getProperty("keytab") != null) {
				HiveMetaConfig.keytab = props.getProperty("keytab");
				LOG.debug("HiveMetaConfig.keytab: " + HiveMetaConfig.keytab);

			}
			if (props.getProperty("keytab_upn") != null) {
				HiveMetaConfig.keytabupn = props.getProperty("keytab_upn");
				LOG.debug("HiveMetaConfig.keytabupn: " + HiveMetaConfig.keytabupn);

			}
			if (props.getProperty("kerberos") != null) {
				HiveMetaConfig.setKrb = Boolean.parseBoolean(props.getProperty("kerberos"));
			}
			LOG.debug("HiveMetaConfig.setKrb: " + HiveMetaConfig.setKrb);

			if (props.getProperty("outputFolder") != null) {
				HiveMetaConfig.outputPath = props.getProperty("outputFolder");
			}
			LOG.debug("HiveMetaConfig.outputPath: " + HiveMetaConfig.outputPath);

			if (props.getProperty("hdfsOutput") != null) {
				HiveMetaConfig.writeToHdfs = Boolean.parseBoolean(props.getProperty("hdfsOutput"));
			}
			LOG.debug("HiveMetaConfig.writeToHdfs: " + HiveMetaConfig.writeToHdfs);

			HiveMetaConfig.alldbs = true;

			if (props.getProperty("exec_hours") != null) {
				execHoursListInt = new ArrayList<Integer>();
				execHoursStr = props.getProperty("exec_hours");
				LOG.debug("execHoursStr: " + execHoursStr);

				if (execHoursStr.contains(",")) {
					LOG.debug("execHoursStr contains: ,");
					execHours = execHoursStr.split(",");
					LOG.debug("execHours.length: " + execHours.length);
					for (String execHour : execHours) {

						LOG.debug("execHour: " + execHour);

						execHourInt = Integer.valueOf(execHour);
						if (execHourInt >= 0 && execHourInt <= 23) {
							LOG.debug("execHourIntAdded: " + execHourInt);
							execHoursListInt.add(execHourInt);
						}
					}
				} else {
					execHourInt = Integer.valueOf(execHoursStr);
					if (execHourInt >= 0 && execHourInt <= 23) {
						LOG.debug("single execHourAdded: " + execHourInt);
						execHoursListInt.add(execHourInt);
					}
				}
				if (!(execHoursListInt.isEmpty())) {
					LOG.info("execHoursListInt.toString: " + execHoursListInt.toString());
					HiveMetaConfig.execTimeList = execHoursListInt;
				} else {
					LOG.info("execHoursListInt.isEmpty-executeEveryHour");
					HiveMetaConfig.execAllHours = true;
				}
			} else {
				LOG.info("exec_hours-notset-executeEveryHour");
				HiveMetaConfig.execAllHours = true;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
