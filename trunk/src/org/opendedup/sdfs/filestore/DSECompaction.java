package org.opendedup.sdfs.filestore;

import java.io.File;
import java.io.IOException;

import org.opendedup.collections.AbstractHashesMap;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.gc.ManualGC;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StorageUnit;


public class DSECompaction {
	public static synchronized void runCheck(AbstractHashesMap map,
			AbstractChunkStore nstore,AbstractChunkStore ostore) throws IOException {
		try {
			
			ostore.iterationInit();
			ChunkData data = ostore.getNextChunck();
			long records = 0;
			int count = 0;
			System.out
					.println("Running Compaction on DSE, this may take a while");
			SDFSLogger.getLog().warn("Running Compaction on DSE, this may take a while");
			SDFSEvent.mountWarnEvent("Running Compaction on DSE, this may take a while");
			System.out.print("Compacting DSE ");
			while (data != null) {
				count++;
				if (count > 500000) {
					count = 0;
					System.out.print("#");
				}
				
				if (map.containsKey(data.getHash())) {
					data.setcPos(-1);
					data.setWriteStore(nstore);
					map.update(data);
					records++;
				} 
				data = ostore.getNextChunck();
			}
			System.out.println("Finished");
			System.out.println("Succesfully Ran Compaction for ["
					+ records + "] records");
			SDFSLogger
					.getLog()
					.warn("Succesfully Ran Compaction for [" + records
							+ "] records");
			SDFSEvent.mountWarnEvent("Succesfully Ran Compaction for [" + records
					+ "] records");
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Unable to finish compaction because", e);
			throw new IOException("Unable to finish compaction");
		}
	}
	
	public static synchronized void runCheck(AbstractHashesMap map,
			FileChunkStore ostore) throws IOException {
		long z =ManualGC.clearChunks(1);
		SDFSLogger.getLog().info("Running Garbage Collection");
		System.out.println("Running Garbage Collection");
		if(z > 0)
			throw new IOException("Unexpected result from garbage collection run. Records should not be claimed but " + z + " were");
		z =ManualGC.clearChunksMills(1000);
		SDFSLogger.getLog().info("Cleared [" + z + "] records during garbage collection");
		System.out.println("Cleared [" + z + "] records during garbage collection");
		map.initCompact();
		File newStorePath = new File(ostore.f.getPath()+".new");
		FileChunkStore nstore = new FileChunkStore(newStorePath.getPath());
		try {
			runCheck(map,nstore,ostore);
		}catch(IOException e) {
			nstore.close();
			newStorePath.delete();
			map.rollbackCompact();
			throw e;
		}
		map.commitCompact(Main.forceCompact);
		SDFSLogger.getLog().warn("Committing FileStore Changes");
		System.out.println("Committing FileStore Changes");
		long osz = ostore.size();
		long nsz = nstore.size();
		String ostorePath = ostore.f.getPath();
		ostore.close();
		nstore.close();
		File f = new File(ostorePath);
		f.delete();
		newStorePath.renameTo(f);
		System.out.println("Commited FileStore Changes");
		StorageUnit unit = StorageUnit.of(osz-nsz);
		System.out.println("Saved " + unit.format(osz-nsz));
	}
	
	

}
