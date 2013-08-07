package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class MaxStoreSizeCmd implements IOCmd {
	private long maxStoreSize = -1;

	public MaxStoreSizeCmd() {
	}

	@Override
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.write(NetworkCMDS.STORE_MAX_SIZE_CMD);
		os.flush();
		this.maxStoreSize = is.readLong();
	}

	public long maxStoreSize() {
		return this.maxStoreSize;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.STORE_MAX_SIZE_CMD;
	}

	@Override
	public Long getResult() {
		return this.maxStoreSize;
	}

}
