package org.opendedup.sdfs.filestore;

import org.opendedup.collections.AbstractHashesMap;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.CommandLineProgressBar;
import org.opendedup.util.SDFSLogger;

public class ConsistancyCheck {
	public static synchronized void runCheck(AbstractHashesMap map,
			AbstractChunkStore store) {
		try {
			store.iterationInit();
			ChunkData data = store.getNextChunck();
			long records = 0;
			long recordsRecovered = 0;
			int count = 0;
			System.out
					.println("Running Consistancy Check on DSE, this may take a while");
			SDFSLogger.getLog().warn("Running Consistancy Check on DSE, this may take a while");
			SDFSEvent evt = SDFSEvent.consistancyCheckEvent("Running Consistancy Check on DSE, this may take a while",Main.mountEvent);
			CommandLineProgressBar bar = new CommandLineProgressBar("Scanning DSE",store.size()/Main.CHUNK_LENGTH,System.out);
			evt.maxCt = store.size()/Main.CHUNK_LENGTH;
			long currentCount = 0;
			while (data != null) {
				count++;
				if (count > 100000) {
					count = 0;
					bar.update(currentCount);
				}
				records++;
				if (!map.containsKey(data.getHash())) {
					map.put(data);
					recordsRecovered++;
				}
				evt.curCt = currentCount;
				data = store.getNextChunck();
				currentCount++;
			}
			bar.finish();
			System.out.println("Finished");
			System.out.println("Succesfully Ran Consistance Check for ["
					+ records + "] records, recovered [" + recordsRecovered
					+ "]");
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
