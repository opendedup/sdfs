package org.opendedup.sdfs.network;

import java.io.DataInputStream;

import java.io.DataOutputStream;
import java.io.IOException;

public class StoreSizeCmd implements IOCmd {
	private long storeSize = -1;

	public StoreSizeCmd() {
	}

	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.write(NetworkCMDS.STORE_SIZE_CMD);
		os.flush();
		this.storeSize = is.readLong();
	}

	public long storeSize() {
		return this.storeSize;
	}

	public byte getCmdID() {
		return NetworkCMDS.STORE_SIZE_CMD;
	}

}
