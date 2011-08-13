package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.util.SDFSLogger;

public class SnapshotCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		return takeSnapshot(file, cmd);
	}

	private String takeSnapshot(String srcPath, String dstPath)
			throws IOException {
		File f = new File(Main.volume.getPath() + File.separator + srcPath);
		File nf = new File(Main.volume.getPath() + File.separator + dstPath);
		
		if(srcPath.startsWith("/"))
			f = new File(srcPath);
		if(dstPath.startsWith("/"));
			nf = new File(dstPath);
			
		try {
			MetaFileStore.snapshot(f.getPath(), nf.getPath(), false);
			return "SUCCESS Snapshot Success: took snapshot Source [" + srcPath
					+ "] " + "Destination [" + dstPath + "]";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Snapshot Failed: unable to take snapshot Source ["
							+ srcPath + "] " + "Destination [" + dstPath
							+ "] because :" + e.toString(), e);
			throw new IOException(
					"Snapshot Failed: unable to take snapshot Source ["
							+ srcPath + "] " + "Destination [" + dstPath
							+ "] because :" + e.toString());
		}
	}

}
