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
		
			
		try {
			if(!f.exists())
				throw new IOException("Path not found [" + srcPath + "]");
			if(nf.exists())
				throw new IOException("Path already exists [" + dstPath + "]");
			MetaFileStore.snapshot(f.getPath(), nf.getPath(), false);
			return "Took snapshot of Source [" + srcPath
					+ "] " + " to Destination [" + dstPath + "]";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Unable to take snapshot Source ["
							+ srcPath + "] " + "Destination [" + dstPath
							+ "] because :" + e.getMessage(), e);
			throw new IOException(
					"Unable to take snapshot Source ["
							+ srcPath + "] " + "Destination [" + dstPath
							+ "] because :" + e.getMessage());
		}
	}

}
