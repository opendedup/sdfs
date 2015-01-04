package org.opendedup.sdfs.io;

import java.util.HashMap;

public class RestoreRequest {
	public SparseDedupFile df;
	public HashMap <String,Boolean> blocks = new HashMap<String,Boolean>();

}
