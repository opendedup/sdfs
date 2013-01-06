package org.opendedup.sdfs.mgmt;

import java.io.File;


import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.RandomGUID;

public class DeleteArchiveCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		if(file.contains(".."))
			throw new IOException("requeste file " + file + " does not exist");
		File vp = new File(Main.volume.getPath()).getParentFile();
		File f = new File(vp.getPath() + File.separator + "archives" +File.separator + RandomGUID.getGuid() + ".tar.gz");
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist");
		else {
			boolean removed = f.delete();
			SDFSLogger.getLog().debug("deleted archive " + file);
			if(removed)
				return "removed [" + file + "]";
			else
				return "failed to remove [" + file + "]";
		}
	}

}
