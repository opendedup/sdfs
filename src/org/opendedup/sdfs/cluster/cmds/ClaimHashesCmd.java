package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.ClusterSocket;
import org.opendedup.sdfs.cluster.DSEServer;
import org.opendedup.sdfs.notification.SDFSEvent;

public class ClaimHashesCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	SDFSEvent evt;

	public ClaimHashesCmd(SDFSEvent evt) {
		opts = new RequestOptions(ResponseMode.GET_ALL, 0);
		this.evt = evt;

	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte[] ob = null;
		try {
			ob = Util.objectToByteBuffer(evt);
		} catch (Exception e1) {
			throw new IOException(e1);
		}
		byte[] b = new byte[1 + 4 + ob.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.RUN_CLAIM);
		buf.putInt(ob.length);
		buf.put(ob);
		try {
			List<Address> addrs = new ArrayList<Address>();
			List<DSEServer> servers = soc.getStorageNodes();
			for (DSEServer server : servers) {
				addrs.add(server.address);
			}
			RspList<Object> lst = soc.getDispatcher().castMessage(addrs,
					new Message(null, null, buf.array()), opts);
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException()) {
					SDFSLogger.getLog().error(
							"Claim Exception thrown for " + rsp.getSender());
					throw rsp.getException();
				} else {
					if (rsp.getValue() != null) {
						SDFSLogger.getLog().debug(
								"Claim completed for " + rsp.getSender()
										+ " returned=" + rsp.getValue());
						SDFSEvent sevt = (SDFSEvent) rsp.getValue();
						ArrayList<SDFSEvent> children = sevt.getChildren();
						for (SDFSEvent cevt : children) {
							evt.addChild(cevt);
						}
					} else {
						SDFSLogger.getLog().debug(
								"recieved null from " + rsp.getSender());
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
		return NetworkCMDS.RUN_CLAIM;
	}

}
