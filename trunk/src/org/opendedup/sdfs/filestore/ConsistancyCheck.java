package org.opendedup.sdfs.filestore;

import org.opendedup.collections.CSByteArrayLongMap;
import org.opendedup.util.SDFSLogger;

public class ConsistancyCheck {
	public static synchronized void runCheck(CSByteArrayLongMap map,AbstractChunkStore store) {
		try {
			store.iterationInit();
			ChunkData data = store.getNextChunck();
			long records = 0;
			long recordsRecovered = 0;
			int count = 0;
			System.out.println("Running Consistancy Check on DSE, this may take a while");
			System.out.print("Scanning DSE ");
			while(data != null) {
				count++;
				if (count > 500000) {
					count = 0;
					System.out.print("#");
				}
				records++;
				if(!map.containsKey(data.getHash())) {
					map.put(data);
					recordsRecovered++;
				}
				data = store.getNextChunck();
			}
			System.out.println("Finished");
			System.out.println("Succesfully Ran Consistance Check for [" + records + "] records, recovered [" + recordsRecovered + "]");
			SDFSLogger.getLog().error("Succesfully Ran Consistance Check for [" + records + "] records, recovered [" + recordsRecovered + "]");
		} catch(Exception e) {
			SDFSLogger.getLog().error("Unable to recover records because " + e.toString(),e);
		}
	}
	

}
