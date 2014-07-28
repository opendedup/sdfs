package org.opendedup.sdfs.io.events;

import org.opendedup.sdfs.io.SparseDedupFile;

public class SFileDeleted {
	SparseDedupFile f = null;
	
	public SFileDeleted(SparseDedupFile f) {
		this.f = f;
	}

}
