package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.MetaDataDedupFile;

public class MFileRenamed {
	
	public MetaDataDedupFile mf;
	public String from;
	public String to;
	public MFileRenamed(MetaDataDedupFile f,String from,String to) {
		this.mf = f;
		this.from = from;
		this.to = to;
	}

}
