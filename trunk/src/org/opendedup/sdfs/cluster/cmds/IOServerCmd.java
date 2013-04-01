package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;

import org.opendedup.sdfs.cluster.DSEServerSocket;

public interface IOServerCmd {
	public abstract void executeCmd(DSEServerSocket socket)
			throws IOException;

	public abstract byte getCmdID();

}
