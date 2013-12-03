package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.ClusterSocket;
import org.opendedup.sdfs.io.Volume;

public class FindVolOwnerCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	// private ArrayList<String> results = new ArrayList<String>();
	private String volumeStr;
	private Volume vol = null;

	public FindVolOwnerCmd(String volumeStr) {
		this.volumeStr = volumeStr;
		opts = new RequestOptions(ResponseMode.GET_ALL, Main.ClusterRSPTimeout);
	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte[] vb = this.volumeStr.getBytes();
		byte[] b = new byte[1 + 4 + vb.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.FIND_VOLUME_OWNER);
		buf.putInt(vb.length);
		buf.put(vb);
		try {
			RspList<Object> lst = soc.getDispatcher().castMessage(null,
					new Message(null, null, buf.array()), opts);
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException()) {
					SDFSLogger.getLog().error(
							"FIND_VOLUME_OWNER Exception thrown for "
									+ rsp.getSender());
				} else if (rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error(
							"FIND_VOLUME_OWNER Host unreachable for "
									+ rsp.getSender());
				} else if (rsp.getValue() != null) {
					SDFSLogger.getLog().debug(
							"FIND_VOLUME_OWNER completed for "
									+ rsp.getSender() + " returned="
									+ rsp.getValue());
					vol = (Volume) rsp.getValue();
					vol.host = rsp.getSender();

				}
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while running fdisk", e);
			throw new IOException(e);
		}
	}

	public Volume getResults() {
		return this.vol;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.FIND_VOLUME_OWNER;
	}

}
