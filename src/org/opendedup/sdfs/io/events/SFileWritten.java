package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.SparseDedupFile;

public class SFileWritten {
	SparseDedupFile f = null;
	
	public SFileWritten(SparseDedupFile f) {
		this.f = f;
	}

}
