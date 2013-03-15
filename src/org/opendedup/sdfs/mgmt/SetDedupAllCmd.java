package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;

public class SetDedupAllCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		File f = new File(Main.volume.getPath() + File.separator + file);
		try {
			boolean dedup = Boolean.parseBoolean(cmd);
			MetaFileStore.getMF(f.getPath()).setDedup(dedup, true);
			return "SUCCESS Dedup Success: set dedup to [" + f.getPath()
					+ "]  [" + dedup + "]";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"ERROR Dedup Failed: unable to set dedup Source ["
							+ f.getPath() + "] " + "to [" + cmd + "] because :"
							+ e.toString(), e);
			throw new IOException(
					"ERROR Dedup Failed: unable to set dedup Source ["
							+ f.getPath() + "] " + "to [" + cmd
							+ "]  because :" + e.toString());
		}
	}

}
