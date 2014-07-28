package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.MetaDataDedupFile;

public class MFileWritten {
	
	public MetaDataDedupFile mf;
	public MFileWritten(MetaDataDedupFile f) {
		this.mf = f;
	}

}
