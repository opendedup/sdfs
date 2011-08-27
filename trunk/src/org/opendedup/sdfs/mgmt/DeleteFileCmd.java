package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;

public class DeleteFileCmd implements XtendedCmd {

	public String getResult(String cmd, String file) throws IOException {
		String internalPath = Main.volume.getPath() + File.separator + file;
		File f = new File(internalPath);
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist");
		else {
			boolean removed = MetaFileStore.removeMetaFile(internalPath);
			if(removed)
				return "removed [" + file + "]";
			else
				return "failed to remove [" + file + "]";
		}
	}

}
