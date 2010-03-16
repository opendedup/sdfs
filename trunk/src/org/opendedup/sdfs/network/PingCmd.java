package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class PingCmd implements IOCmd {

	private short response;

	public PingCmd() {
	}

	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.writeInt(NetworkCMDS.PING_CMD);
		os.flush();
		response = is.readShort();
	}

	public short getResponse() {
		return this.response;
	}

	public byte getCmdID() {
		// TODO Auto-generated method stub
		return NetworkCMDS.PING_CMD;
	}

}
