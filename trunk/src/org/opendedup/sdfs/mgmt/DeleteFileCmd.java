package org.opendedup.sdfs.mgmt;

import java.io.File;

import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.notification.SDFSEvent;

public class DeleteFileCmd implements XtendedCmd {

	@Override
	public String getResult(String cmd, String file) throws IOException {
		if (file.contains(".."))
			throw new IOException("requeste file " + file + " does not exist");
		String internalPath = Main.volume.getPath() + File.separator + file;
		File f = new File(internalPath);
		if (!f.exists())
			throw new IOException("requeste file " + file + " does not exist");
		else {
			boolean removed = MetaFileStore.removeMetaFile(internalPath, true);
			if (removed) {
				SDFSEvent.deleteFileEvent(f);
				return "removed [" + file + "]";

			} else {
				SDFSEvent.deleteFileFailedEvent(f);
				return "failed to remove [" + file + "]";
			}
		}
	}

}
