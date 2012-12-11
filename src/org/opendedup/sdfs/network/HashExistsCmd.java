package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HashExistsCmd implements IOCmd {
	byte[] hash;
	boolean exists = false;
	short hops = 0;

	public HashExistsCmd(byte[] hash,short hops) {
		this.hash = hash;
		this.hops = hops;
	}

	@Override
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.write(NetworkCMDS.HASH_EXISTS_CMD);
		os.writeShort(hops);
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

	@Override
	public byte getCmdID() {
		return NetworkCMDS.HASH_EXISTS_CMD;
	}

}
