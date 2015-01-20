package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.MetaDataDedupFile;

public class MFileSync {
	
	public MetaDataDedupFile mf;
	public MFileSync(MetaDataDedupFile f) {
		this.mf = f;
	}

}
