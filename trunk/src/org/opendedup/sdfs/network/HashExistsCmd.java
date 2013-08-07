package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HashExistsCmd implements IOCmd {
	byte[] hash;
	boolean exists = false;

	public HashExistsCmd(byte[] hash) {
		this.hash = hash;
	}

	@Override
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.write(NetworkCMDS.HASH_EXISTS_CMD);
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

	@Override
	public Boolean getResult() {
		return this.exists;
	}

}
