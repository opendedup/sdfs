package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.SparseDedupFile;

public class SFileWritten {
	public SparseDedupFile sf = null;
	
	public SFileWritten(SparseDedupFile f) {
		this.sf = f;
	}

}
