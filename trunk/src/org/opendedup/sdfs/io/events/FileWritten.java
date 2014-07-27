package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.MetaDataDedupFile;

public class FileWritten {
	
	public MetaDataDedupFile mf;
	public FileWritten(MetaDataDedupFile f) {
		this.mf = f;
	}

}
