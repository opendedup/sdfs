package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import org.apache.commons.collections.map.LRUMap;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class ClaimHashCmd implements IOCmd {
	byte[] hash;
	boolean exists = false;

	public ClaimHashCmd(byte[] hash) {
		this.hash = hash;
	}

	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
			os.write(NetworkCMDS.CLAIM_HASH);
			os.writeShort(hash.length);
			os.write(hash);
			os.flush();
			this.exists = is.readBoolean();
	}
	
	public byte[] getHash() {
		return this.hash;
	}

	public boolean exists() {
		return this.exists;
	}
	
	public byte getCmdID() {
		return NetworkCMDS.CLAIM_HASH;
	}

}
