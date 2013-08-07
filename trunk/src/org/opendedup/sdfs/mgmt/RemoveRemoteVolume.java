package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.cmds.RMVolCmd;
import org.opendedup.sdfs.servers.HCServiceProxy;

public class RemoveRemoteVolume {

	public void getResult(String cmd, String vol) throws IOException {
		try {
			if(Main.chunkStoreLocal)
				throw new IOException("Chunk Store is local");
			RMVolCmd rcmd = new RMVolCmd(vol);
			rcmd.executeCmd(HCServiceProxy.cs);
			
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"unable to fulfill request to remove volume " + vol, e);
			throw new IOException("unable to fulfill request to remove volume " + vol +" because "
					+ e.toString());
		}
	}

}
