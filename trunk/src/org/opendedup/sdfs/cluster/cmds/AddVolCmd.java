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

public class AddVolCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	// private ArrayList<String> results = new ArrayList<String>();
	private String volume;

	public AddVolCmd(String volume) {
		this.volume = volume;
		opts = new RequestOptions(ResponseMode.GET_ALL, Main.ClusterRSPTimeout);

	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte[] vb = this.volume.getBytes();
		byte[] b = new byte[1 + 4 + vb.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.ADD_VOLUME);
		buf.putInt(vb.length);
		buf.put(vb);
		try {
			RspList<Object> lst = soc.getDispatcher().castMessage(null,
					new Message(null, null, buf.array()), opts);
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException()) {
					SDFSLogger.getLog().error(
							"Add Volume to Cache Exception thrown for "
									+ rsp.getSender());
					throw rsp.getException();
				} else if (rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error(
							"Add Volume to Cache Host unreachable for "
									+ rsp.getSender());
				}

			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while running fdisk", e);
			throw new IOException(e);
		}
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.ADD_VOLUME;
	}

}
