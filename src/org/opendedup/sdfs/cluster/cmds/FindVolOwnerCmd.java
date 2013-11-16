package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;

import java.nio.ByteBuffer;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.ClusterSocket;

public class FindVolOwnerCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	// private ArrayList<String> results = new ArrayList<String>();
	private String volume;
	private Address owner = null;

	public FindVolOwnerCmd(String volume) {
		this.volume = volume;
		opts = new RequestOptions(ResponseMode.GET_ALL, 0);

	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte[] vb = this.volume.getBytes();
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
					boolean m = (Boolean) rsp.getValue();
					if (m) {
						if (this.owner != null)
							throw new IOException(
									"VOLUME_OWNER already identified at ["
											+ owner.toString()
											+ "] but has also been identified at ["
											+ rsp.getSender() + "].");
						else
							this.owner = rsp.getSender();
					}

				}
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while running fdisk", e);
			throw new IOException(e);
		}
	}

	public Address getResults() {
		return this.owner;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.FIND_VOLUME_OWNER;
	}

}
