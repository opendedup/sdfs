package org.opendedup.sdfs.mgmt;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.xml.parsers.ParserConfigurationException;

import org.opendedup.mtools.RestoreArchive;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.w3c.dom.Element;

public class RestoreArchiveCmd {

	String srcPath;

	public Element getResult(String file) throws IOException, ExecutionException {
		this.srcPath = file;
		File f = new File(Main.volume.getPath() + File.separator + srcPath);
		if (!f.exists())
			throw new IOException("Path not found [" + srcPath + "]");
		MetaDataDedupFile mf = MetaFileStore.getMF(f);
		RestoreArchive ar = new RestoreArchive(mf);
		Thread th = new Thread(ar);
		th.start();
		try {
			return ar.getEvent().toXML();
		} catch (ParserConfigurationException e) {
			throw new IOException(e);
		}
	}

}
