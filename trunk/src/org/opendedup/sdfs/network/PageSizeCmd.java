package org.opendedup.sdfs.network;

import java.io.DataInputStream;

import java.io.DataOutputStream;
import java.io.IOException;

public class PageSizeCmd implements IOCmd {
	private int pageSize = -1;

	public PageSizeCmd() {
	}

	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.write(NetworkCMDS.STORE_PAGE_SIZE);
		os.flush();
		this.pageSize = is.readInt();
	}


	public int pageSize() {
		return this.pageSize;
		}

	public byte getCmdID() {
		return NetworkCMDS.STORE_PAGE_SIZE;
	}

}
