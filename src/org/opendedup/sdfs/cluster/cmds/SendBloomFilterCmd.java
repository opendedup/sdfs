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
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.ClusterSocket;
import org.opendedup.sdfs.cluster.DSEServer;
import org.opendedup.util.CompressionUtils;
import org.opendedup.util.LBF;
public class SendBloomFilterCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	private LBF lbf;
	private int id;

	public SendBloomFilterCmd(int id,LBF lbf) {
		opts = new RequestOptions(ResponseMode.GET_ALL, 0);
		this.lbf = lbf;
		this.id = id;
	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte [] lb = lbf.getBytes(); 
		int as = lb.length;
		lb = CompressionUtils.compressLz4(lb);
		byte[] b = new byte[1+4+4+4+lb.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.SEND_BF);
		buf.putInt(this.id);
		buf.putInt(lb.length);
		buf.putInt(as);
		buf.put(lb);
		
		try {
			List<Address> addrs = new ArrayList<Address>();
			List<DSEServer> servers = soc.getStorageNodes();
			for (DSEServer server : servers) {
				addrs.add(server.address);
			}
			
			if (servers.size() == 0)
				throw new IOException(
						"No Servers Found");
			RspList<Object> lst = soc.getDispatcher().castMessage(addrs,
					new Message(null, null, buf.array()), opts);
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException()) {
					SDFSLogger.getLog().error(
							"LBF Send Exception thrown for " + rsp.getSender());
					throw rsp.getException();
				} else if (rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error(
							"LBF Send unreachable Exception thrown for "
									+ rsp.getSender());
					throw new IOException(
							"LBF Send unreachable Exception thrown for "
									+ rsp.getSender());
				} else {
						SDFSLogger.getLog().debug(
								"LBF completed for " + rsp.getSender()
										+ " returned=" + rsp.getValue());
				}
			}
		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while running fdisk", e);
			throw new IOException(e);
		}
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.LIST_VOLUMES;
	}

}
