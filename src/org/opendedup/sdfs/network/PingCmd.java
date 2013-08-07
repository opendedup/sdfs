package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class PingCmd implements IOCmd {

	private short response;

	public PingCmd() {
	}

	@Override
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		os.writeInt(NetworkCMDS.PING_CMD);
		os.flush();
		response = is.readShort();
	}

	public short getResponse() {
		return this.response;
	}

	@Override
	public byte getCmdID() {
		// TODO Auto-generated method stub
		return NetworkCMDS.PING_CMD;
	}

	@Override
	public Short getResult() {
		return this.response;
	}

}
