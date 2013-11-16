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

public class StopGCMasterCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	private Address gcMaster = null;

	public StopGCMasterCmd() {
		opts = new RequestOptions(ResponseMode.GET_ALL, 0);

	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte[] b = new byte[1];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.STOP_GC_MASTER_CMD);
		try {
			RspList<Object> lst = soc.getDispatcher().castMessage(null,
					new Message(null, null, buf.array()), opts);
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException()) {
					SDFSLogger.getLog().error(
							"STOP_GC_MASTER_CMD Exception thrown for "
									+ rsp.getSender());
					// throw rsp.getException();
				} else if (rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error(
							"STOP_GC_MASTER_CMD Host unreachable Exception thrown for "
									+ rsp.getSender());
					// throw new
					// IOException("FIND_GC_MASTER_CMD Host unreachable Exception thrown for "
					// + rsp.getSender());
				} else {
					if (rsp.getValue() != null) {
						SDFSLogger.getLog().debug(
								"STOP_GC_MASTER_CMD completed for "
										+ rsp.getSender() + " returned="
										+ rsp.getValue());
						boolean m = (Boolean) rsp.getValue();
						if (m) {
							if (this.gcMaster != null)
								throw new IOException(
										"STOP_GC_MASTER_CMD already identified at ["
												+ gcMaster.toString()
												+ "] but has also been identified at ["
												+ rsp.getSender() + "].");
							else
								this.gcMaster = rsp.getSender();
						}

					}
				}
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while running STOP_GC_MASTER_CMD",
					e);
			throw new IOException(e);
		}
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.STOP_GC_MASTER_CMD;
	}

	public Address getResults() {
		return this.gcMaster;
	}

}
