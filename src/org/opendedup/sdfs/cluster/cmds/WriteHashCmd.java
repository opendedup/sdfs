package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.DSEClientSocket;
import org.opendedup.util.StringUtils;

public class WriteHashCmd implements IOClientCmd {
	byte[] hash;
	byte[] aContents;
	int position;
	int len;
	boolean written = false;
	boolean compress = false;
	byte numberOfCopies = 1;
	byte[] resp = new byte[8];
	RequestOptions opts = null;

	public WriteHashCmd(byte[] hash, byte[] aContents, int len,
			boolean compress, byte numberOfCopies) throws IOException {
		this.hash = hash;
		this.compress = compress;
		this.numberOfCopies = numberOfCopies;

		if (compress) {
			throw new IOException("not implemented");
			/*
			 * try { byte[] compB = CompressionUtils.compress(aContents); if
			 * (compB.length <= aContents.length) { this.aContents = compB;
			 * this.len = this.aContents.length; } else { this.compress = false;
			 * this.aContents = aContents; this.len = len; } } catch
			 * (IOException e) { // TODO Auto-generated catch block
			 * e.printStackTrace(); this.aContents = aContents; this.len = len;
			 * this.compress = false; }
			 */
		} else {
			this.aContents = aContents;
			this.len = len;
		}
		opts = new RequestOptions(ResponseMode.GET_ALL, Main.ClusterRSPTimeout);
		SDFSLogger.getLog().debug("Write Initialized");
	}

	@Override
	public void executeCmd(DSEClientSocket soc) throws IOException {
		SDFSLogger.getLog().debug("Writing " + StringUtils.getHexString(hash));
		if (this.numberOfCopies > 7)
			this.numberOfCopies = 7;
		int stateSz = soc.serverState.size();
		if (soc.serverState.size() < this.numberOfCopies)
			this.numberOfCopies = (byte) stateSz;
		byte[] b = new byte[1 + 2 + hash.length + 4 + aContents.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		if (compress)
			buf.put(NetworkCMDS.WRITE_COMPRESSED_CMD);
		else
			buf.put(NetworkCMDS.WRITE_HASH_CMD);
		buf.putShort((short) hash.length);
		buf.put(hash);
		buf.putInt(len);
		buf.put(aContents);
		try {
			List<Address> addrs = soc.getServers(this.numberOfCopies);
			SDFSLogger.getLog().debug("Servers = " + addrs.size());
			RspList<Object> lst = soc.disp.castMessage(addrs, new Message(null,
					null, buf.array()), opts);
			int pos = 1;
			Collection<Rsp<Object>> responses = lst.values();
			for (Rsp<Object> response : responses) {
				DSEServer svr = soc.serverState.get(response.getSender());
				if (svr != null) {
					resp[pos] = svr.id;
					if(response.hasException()) {
						SDFSLogger.getLog().warn("remote exception found " +response.getException().getMessage());
						throw(new IOException(response.getException()));
					}
					boolean done = (Boolean) response.getValue();
					if (done)
						resp[0] = 1;
					pos++;

				} else {
					SDFSLogger.getLog().info(
							"addr " + response.getSender()
									+ " does not exist in the system");
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public byte[] reponse() {
		return this.resp;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.WRITE_HASH_CMD;
	}

}
