package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;

import java.nio.ByteBuffer;

import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.DSEClientSocket;

public class FDiskCmd implements IOClientCmd {
	boolean exists = false;
	RequestOptions opts = null;

	public FDiskCmd() {
		opts = new RequestOptions(ResponseMode.GET_FIRST,
				0);
		
	}

	@Override
	public void executeCmd(final DSEClientSocket soc) throws IOException {
		byte[] b = new byte[1];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.RUN_FDISK);
		try {
			RspList<Object> lst = soc.disp.castMessage(null, new Message(null,
					null, buf.array()), opts);
			for(Rsp<Object> rsp : lst) {
				if(rsp.hasException()) {
					SDFSLogger.getLog().error("FDISK Exception thrown for " + rsp.getSender());
					throw rsp.getException();
				} else if(rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error("FDISK Host unreachable Exception thrown ");
					throw new IOException("FDISK Host unreachable Exception thrown ");
				}
				else {
					if(rsp.getValue() != null) {
						SDFSLogger.getLog().debug("FDisks completed for " +rsp.getSender() + " returned=" +rsp.getValue());
					}
				}
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while running fdisk", e);
			throw new IOException(e);
		}
	}
	

	@Override
	public byte getCmdID() {
		return NetworkCMDS.RUN_FDISK;
	}

}
