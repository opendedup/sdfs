package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.MetaDataDedupFile;

public class MFileDeleted {
	
	public MetaDataDedupFile mf;
	public boolean dir;
	public MFileDeleted(MetaDataDedupFile f) {
		this.mf = f;
	}
	
	public MFileDeleted(MetaDataDedupFile f,boolean dir) {
		this.mf = f;
		this.dir = dir;
	}

}
