package org.senia.hive;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetaExportRunnable implements Runnable {
	HiveMetaOps hmo;
	boolean execNow = false;
	private static final Logger LOG = LoggerFactory.getLogger(HiveMetaExportRunnable.class);

	public void run() {
		// while (runThreads) {
		execNow = false;
		if (HiveMetaConfig.execAllHours) {
			LOG.debug("execAllHours=true");
			execNow = true;
		} else if (!(HiveMetaConfig.execAllHours) && !(HiveMetaConfig.execTimeList.isEmpty())) {
			int currentHour = LocalDateTime.now().getHour();
			LOG.debug("Current Hour: " + currentHour);
			execNow = false;
			for (Integer execTime : HiveMetaConfig.execTimeList) {
				if (execTime == Integer.valueOf(currentHour)) {
					execNow = true;
					break;
				} else {
					execNow = false;
				}
			}
		} else {
			execNow = false;
		}
		if (execNow) {
			LOG.debug("ExecutingNow");
			if (hmo == null) {
				LOG.info("HiveMetaOps-Thread is null");
				hmo = new HiveMetaOps();
			}
			if (!(hmo.isAlive())) {
				LOG.info("HiveMetaOps-Thread: " + hmo.isAlive());
				hmo = new HiveMetaOps();
				LOG.info("Creating HiveMetaOps-Thread");
				hmo.setName("HiveMetaOps-Thread");
				hmo.setDaemon(true);
				hmo.start();
				LOG.info("HiveMetaOps-Thread: " + hmo.isAlive());
			}
		}
	}

}
