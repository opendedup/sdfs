package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;

import org.opendedup.sdfs.cluster.DSEClientSocket;

public interface IOClientCmd {
	public abstract void executeCmd(DSEClientSocket socket) throws IOException,ClusterCmdException;

	public abstract byte getCmdID();

}
