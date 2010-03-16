package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface IOCmd {
	public abstract void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException;

	public abstract byte getCmdID();

}
