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
import org.opendedup.sdfs.notification.SDFSEvent;

public class FDiskCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	private ArrayList<SDFSEvent> results = new ArrayList<SDFSEvent>();
	

	public FDiskCmd() {
		opts = new RequestOptions(ResponseMode.GET_ALL,
				0);
		
	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte[] b = new byte[1];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.RUN_FDISK);
		try {
			List<String> vols = soc.getVolumes();
			List<Address> dst = new ArrayList<Address>();
			for(String vol : vols) {
				if(vol != null) {
				Address addr = soc.getAddressForVol(vol);
				if(addr == null)
					throw new IOException("FDISK Could not be completed because no name node found for ["  + vol + "]");
				else {
					SDFSLogger.getLog().debug("Will run fdisk for [" + vol + "]");	
					dst.add(addr);
				}
				}
			}
			if(dst.size() == 0)
				throw new IOException("FDISK Could not be completed because no name nodes found");
			SDFSLogger.getLog().debug("Running fdisk for [" + dst.size() + "] volumes");
			RspList<Object> lst = soc.getDispatcher().castMessage(dst, new Message(null,
					null, buf.array()), opts);
			for(Rsp<Object> rsp : lst) {
				if(rsp.hasException()) {
					SDFSLogger.getLog().error("FDISK Exception thrown for " + rsp.getSender());
					throw rsp.getException();
				} else if(rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error("FDISK Host unreachable Exception thrown for " + rsp.getSender());
					throw new IOException("FDISK Host unreachable Exception thrown for " + rsp.getSender());
				}
				else {
					if(rsp.getValue() != null) {
						SDFSLogger.getLog().debug("FDisks completed for " +rsp.getSender() + " returned=" +rsp.getValue());
						SDFSEvent evt = (SDFSEvent)rsp.getValue();
						this.results.add(evt);
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
		return NetworkCMDS.LIST_VOLUMES;
	}
	
	public List<SDFSEvent> getResults() {
		return this.results;
	}

}
