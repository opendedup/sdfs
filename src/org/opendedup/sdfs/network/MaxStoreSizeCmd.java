package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MaxStoreSizeCmd implements IOCmd {
	private long maxStoreSize = -1;

	public MaxStoreSizeCmd() {
	}

	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.write(NetworkCMDS.STORE_MAX_SIZE_CMD);
		os.flush();
		this.maxStoreSize = is.readLong();
	}

	public long maxStoreSize() {
		return this.maxStoreSize;
	}

	public byte getCmdID() {
		return NetworkCMDS.STORE_MAX_SIZE_CMD;
	}

}
