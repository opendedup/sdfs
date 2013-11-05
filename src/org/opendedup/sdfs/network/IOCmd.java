package org.opendedup.sdfs.network;

import java.io.DataInputStream;

import java.io.DataOutputStream;
import java.io.IOException;

public interface IOCmd {
	public abstract void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException,IOCmdException;

	public abstract byte getCmdID();
	public abstract Object getResult();

}
