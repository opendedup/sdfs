package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.util.SDFSLogger;

public class CopyOutCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		return takeSnapshot(file, cmd);
	}

	private String takeSnapshot(String srcPath, String dstPath)
			throws IOException {
		File f = new File(Main.volume.getPath() + File.separator + srcPath);
		File nf = new File(dstPath);
		if(!nf.exists())
			nf.mkdirs();
			
		try {
			MetaDataDedupFile mf = MetaFileStore.getMF(f);
			mf.copyTo(dstPath, true);
			return "Copied Source [" + srcPath
					+ "] " + " to Destination [" + dstPath + "]";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"Unable to take snapshot Source ["
							+ srcPath + "] " + "Destination [" + dstPath
							+ "] because :" + e.toString(), e);
			throw new IOException(
					"Unable to take snapshot Source ["
							+ srcPath + "] " + "Destination [" + dstPath
							+ "] because :" + e.toString());
		}
	}

}
