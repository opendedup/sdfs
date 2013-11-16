package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;

import org.opendedup.sdfs.cluster.ClusterSocket;

public interface IOPeerCmd {
	public abstract void executeCmd(ClusterSocket socket) throws IOException;

	public abstract byte getCmdID();

}
