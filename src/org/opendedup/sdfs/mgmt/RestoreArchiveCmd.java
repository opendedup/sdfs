package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;


import org.opendedup.mtools.RestoreArchive;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.notification.SDFSEvent;

public class RestoreArchiveCmd {

	String srcPath;

	public SDFSEvent getResult(String file) throws IOException,
			ExecutionException {
		this.srcPath = file;
		File f = new File(Main.volume.getPath() + File.separator + srcPath);
		if (!f.exists())
			throw new IOException("Path not found [" + srcPath + "]");
		MetaDataDedupFile mf = MetaFileStore.getMF(f);
		RestoreArchive ar = new RestoreArchive(mf,-1);
		Thread th = new Thread(ar);
		th.start();
		try {
			return ar.getEvent();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

}
