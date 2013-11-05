
package org.opendedup.sdfs.network;

import java.io.IOException;

public class ChunkNotFoundException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5398045346438784590L;
	
	public ChunkNotFoundException(String hash) {
		super("could not find chunk " + hash);
	}

}
