package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.util.SDFSLogger;

public class MultiSnapshotCmd implements XtendedCmd {
	int snaps = 0;

	public MultiSnapshotCmd(int snaps) {
		this.snaps = snaps;
	}

	public String getResult(String cmd, String file) throws IOException {
		return takeSnapshot(file, cmd);
	}

	private String takeSnapshot(String srcPath, String dstPath)
			throws IOException {
		File f = new File(Main.volume.getPath() + File.separator + srcPath);
		for (int i = 0; i < snaps; i++) {
			int index = dstPath.lastIndexOf(".");
			if (index > 0 && index <= dstPath.length() - 2) {
				dstPath = dstPath.substring(0, index) + i
						+ dstPath.substring(index, dstPath.length());
			} else {
				dstPath = dstPath + i;
			}

			File nf = new File(Main.volume.getPath() + File.separator + dstPath);

			if (f.getPath().equalsIgnoreCase(nf.getPath()))
				throw new IOException("Snapshot Failed: Source [" + srcPath
						+ "] and destination [" + dstPath + "] are the same");
			if (nf.exists())
				throw new IOException("Snapshot Failed: destination ["
						+ dstPath + "] already exists");
			try {
				MetaFileStore.snapshot(f.getPath(), nf.getPath(), false);

			} catch (IOException e) {
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
		return "SUCCESS Snapshot Success: took snapshot Source [" + srcPath
				+ "] " + "Destination [" + dstPath + "]";
	}

}
