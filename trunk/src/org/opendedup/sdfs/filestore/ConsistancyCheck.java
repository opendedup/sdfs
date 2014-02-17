package org.opendedup.sdfs.filestore;

import java.util.HashMap;


import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.CommandLineProgressBar;

public class ConsistancyCheck {
	public static synchronized void runCheck(AbstractHashesMap map,
			AbstractChunkStore store) {
		try {
			store.iterationInit();
			ChunkData data = store.getNextChunck();
			HashMap<Long,Long> mismatch = new HashMap<Long,Long>();
			
			data.recoverd = true;
			long records = 0;
			long recordsRecovered = 0;
			int count = 0;
			System.out
					.println("Running Consistancy Check on DSE, this may take a while");
			SDFSLogger.getLog().warn(
					"Running Consistancy Check on DSE, this may take a while");
			SDFSEvent evt = SDFSEvent.consistancyCheckEvent(
					"Running Consistancy Check on DSE, this may take a while",
					Main.mountEvent);
			CommandLineProgressBar bar = new CommandLineProgressBar(
					"Scanning DSE", store.size() / Main.CHUNK_LENGTH,
					System.out);
			evt.maxCt = store.size() / Main.CHUNK_LENGTH;
			long currentCount = 0;
			while (data != null) {
				count++;
				if (count > 100000) {
					count = 0;
					bar.update(currentCount);
				}
				records++;
				long pos = map.get(data.getHash());
				if (pos <0) {
					map.put(data);
					recordsRecovered++;
				}
				evt.curCt = currentCount;
				try {
				data = store.getNextChunck();
				if (data != null) {
					data.recoverd = true;
					currentCount++;
				}
				}catch(Exception e) {
					SDFSLogger.getLog().warn("Data Corruption found in datastore");
				}
			}
			bar.finish();
			System.out.println("Finished");
			System.out.println("Succesfully Ran Consistance Check for ["
					+ records + "] records, recovered [" + recordsRecovered
					+ "]");
			if(mismatch.size() > 0) {
				String msg = "======Warning Data Alignment Issue. Mismatched Data found at " +mismatch.size() + "======";
				SDFSLogger.getLog().error(msg);
				System.out.println(msg);
			}
			SDFSLogger
					.getLog()
					.warn("Succesfully Ran Consistance Check for [" + records
							+ "] records, recovered [" + recordsRecovered + "]");
			evt.endEvent("Succesfully Ran Consistance Check for [" + records
					+ "] records, recovered [" + recordsRecovered + "]");
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Unable to recover records because " + e.toString(), e);
		}
	}

}
