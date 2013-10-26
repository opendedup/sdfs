package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;



import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.ClusterSocket;

public class ListVolsCmd implements IOPeerCmd {
	boolean exists = false;
	RequestOptions opts = null;
	private ArrayList<String> results = new ArrayList<String>();
	

	public ListVolsCmd() {
		opts = new RequestOptions(ResponseMode.GET_ALL,
				0);
		
	}

	@Override
	public void executeCmd(final ClusterSocket soc) throws IOException {
		byte[] b = new byte[1];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.LIST_VOLUMES);
		try {
			
			RspList<Object> lst = soc.getDispatcher().castMessage(null, new Message(null,
					null, buf.array()), opts);
			for(Rsp<Object> rsp : lst) {
				if(rsp.hasException()) {
					SDFSLogger.getLog().error("List Volume Exception thrown for " + rsp.getSender());
					throw rsp.getException();
				} else if(rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error("List Volume Host unreachable Exception thrown for " + rsp.getSender());
				}
				else {
					if(rsp.getValue() != null) {
						SDFSLogger.getLog().debug("List completed for " +rsp.getSender() + " returned=" +rsp.getValue());
						@SuppressWarnings("unchecked")
						ArrayList<String> rst = (ArrayList<String>)rsp.getValue();
						for(String vol: rst) {
							if(!this.results.contains(vol))
								this.results.add(vol);
						}
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
	
	public List<String> getResults() {
		return this.results;
	}

}
