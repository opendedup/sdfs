package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.MetaDataDedupFile;

public class MFileDeleted {
	
	public MetaDataDedupFile mf;
	public MFileDeleted(MetaDataDedupFile f) {
		this.mf = f;
	}

}
