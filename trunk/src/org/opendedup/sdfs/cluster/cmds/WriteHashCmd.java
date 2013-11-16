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
import org.opendedup.sdfs.cluster.DSEServer;

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
	byte[] ignoredhosts = null;

	public WriteHashCmd(byte[] hash, byte[] aContents, boolean compress,
			byte numberOfCopies) throws IOException {
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
			this.len = this.aContents.length;
		}
		opts = new RequestOptions(ResponseMode.GET_ALL,Main.ClusterRSPTimeout * 2,true);
		opts.setFlags(Message.Flag.NO_TOTAL_ORDER);
		opts.setFlags(Message.Flag.DONT_BUNDLE);
		opts.setFlags(Message.Flag.OOB);
		// opts.setFlags(Message.Flag.DONT_BUNDLE);
		// opts.setFlags(Message.Flag.OOB);
		opts.setAnycasting(true);
	}

	public WriteHashCmd(byte[] hash, byte[] aContents, boolean compress,
			byte numberOfCopies, byte[] ignoredHosts) throws IOException {
		this.hash = hash;
		this.compress = compress;
		this.numberOfCopies = numberOfCopies;
		this.ignoredhosts = ignoredHosts;
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
			this.len = this.aContents.length;
		}
		opts = new RequestOptions(ResponseMode.GET_ALL, Main.ClusterRSPTimeout * 2,
				true);
		opts.setFlags(Message.Flag.NO_TOTAL_ORDER);
		opts.setFlags(Message.Flag.DONT_BUNDLE);
		opts.setFlags(Message.Flag.OOB);
		opts.setAnycasting(true);
	}

	@Override
	public void executeCmd(DSEClientSocket soc) throws IOException {
		// SDFSLogger.getLog().info("writing to " + this.numberOfCopies);
		if (this.numberOfCopies > 7)
			this.numberOfCopies = 7;
		int stateSz = soc.serverState.size();
		if (soc.serverState.size() < this.numberOfCopies)
			this.numberOfCopies = (byte) stateSz;
		byte[] b = new byte[1 + 2 + hash.length + 4 + aContents.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.WRITE_HASH_CMD);
		buf.putShort((short) hash.length);
		buf.put(hash);
		buf.putInt(len);
		buf.put(aContents);
		try {
			List<Address> addrs = soc.getServers(this.numberOfCopies,
					ignoredhosts);
			int pos = 1;
			if (addrs.size() > 0) {
				RspList<Object> lst = soc.disp.castMessage(addrs, new Message(
						null, null, buf.array()), opts);

				Collection<Rsp<Object>> responses = lst.values();

				for (Rsp<Object> response : responses) {
					DSEServer svr = soc.serverState.get(response.getSender());
					if (svr != null) {

						if (response.hasException()) {
							SDFSLogger.getLog().debug(
									"remote exception found "
											+ response.getException()
													.getMessage());
						} else if (response.wasSuspected()
								|| response.wasUnreachable()) {

						} else {
							try {
								boolean done = (Boolean) response.getValue();
								if (done)
									resp[0] = 1;
								resp[pos] = svr.id;
								pos++;
							} catch (Exception e) {
								SDFSLogger.getLog().warn(
										"unable to write to "
												+ response.getSender(), e);
							}
						}

					} else {
						SDFSLogger.getLog().info(
								"addr " + response.getSender()
										+ " does not exist in the system");
					}

				}
			}
			if (this.ignoredhosts != null) {
				for (byte bz : ignoredhosts) {
					if (bz != (byte) 0) {
						resp[pos] = bz;
						pos++;
					}
				}
			}
			if (pos == 1)
				throw new IOException("unable to write to any storage nodes");
		} catch (Exception e) {
			// SDFSLogger.getLog().error("error while writing",e);
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
