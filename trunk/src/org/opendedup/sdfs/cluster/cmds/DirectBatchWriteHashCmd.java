package org.opendedup.sdfs.cluster.cmds;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.opendedup.collections.QuickList;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.DSEClientSocket;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.io.BufferClosedException;
import org.opendedup.sdfs.io.WritableCacheBuffer;

public class DirectBatchWriteHashCmd implements IOClientCmd {
	List<WritableCacheBuffer> chunks;
	QuickList<HashChunk> hk;
	boolean exists = false;
	RequestOptions opts = null;
	int sz = 0;

	public DirectBatchWriteHashCmd(List<WritableCacheBuffer> chunks) {
		this.chunks = chunks;
		sz = chunks.size();
		hk = new QuickList<HashChunk>(sz);
		//long tm = System.currentTimeMillis();
		for (int i = 0; i < sz; i++) {
			WritableCacheBuffer buff = chunks.get(i);
			byte[] hashloc = buff.getHashLoc();
			int ncopies = 0;
			for (int z = 1; z < 8; z++) {
				if (hashloc[z] > (byte) 0) {
					ncopies++;
				}
			}
			if (ncopies == 0) {
				buff.resetHashLoc();
				try {
					hk.add(i,
							new HashChunk(buff.getHash(), buff
									.getFlushedBuffer(), false));
				} catch (BufferClosedException e) {
					hk.add(i, null);
				}
			} else {
				hk.add(i, null);
			}
		}
		//tm = System.currentTimeMillis() - tm;
		//SDFSLogger.getLog().info("ph 1 time was " + tm + " sz = " + sz);
	}

	@Override
	public void executeCmd(final DSEClientSocket soc) throws IOException {
		opts = new RequestOptions(ResponseMode.GET_ALL, Main.ClusterRSPTimeout
				* sz, true);
		opts.setFlags(Message.Flag.DONT_BUNDLE);
		// opts.setFlags(Message.Flag.NO_FC);
		//opts.setFlags(Message.Flag.OOB);
		opts.setAnycasting(true);
		try {
			//long tm = System.currentTimeMillis();
			byte[] ar = null;
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = null;
			try {
			  out = new ObjectOutputStream(bos);   
			  out.writeObject(hk);
			  ar = bos.toByteArray();
			} finally {
			  out.close();
			  bos.close();
			}
			//tm = System.currentTimeMillis() - tm;
			//SDFSLogger.getLog().info("ph 2 time was " + tm);
			//tm = System.currentTimeMillis();
			byte[] b = new byte[1 + 4 + ar.length];
			ByteBuffer buf = ByteBuffer.wrap(b);
			buf.put(NetworkCMDS.BATCH_WRITE_HASH_CMD);
			buf.putInt(ar.length);
			buf.put(ar);
			//tm = System.currentTimeMillis() - tm;
			//SDFSLogger.getLog().info("ph 3 time was " + tm);
			//tm = System.currentTimeMillis();
			List<Address> servers = soc.getServers(
					Main.volume.getClusterCopies(), null);
			//tm = System.currentTimeMillis() - tm;
			//SDFSLogger.getLog().info("ph 4 time was " + tm + " server sz =" +servers.size());
			//tm = System.currentTimeMillis();
			RspList<Object> lst = soc.disp.castMessage(servers, new Message(
					null, null, buf.array()), opts);
			//tm = System.currentTimeMillis() - tm;
			//SDFSLogger.getLog().info("ph 5 time was " + tm + " buff sz " + b.length);
			//tm = System.currentTimeMillis();
			//int proc = 0;
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException()) {
					SDFSLogger.getLog().error(
							"Batch Hash Exists Exception thrown for "
									+ rsp.getSender(), rsp.getException());
				} else if (rsp.wasSuspected() | rsp.wasUnreachable()) {
					SDFSLogger.getLog().error(
							"Batch Hash Exists Host unreachable Exception thrown for "
									+ rsp.getSender());
				} else {
					if (rsp.getValue() != null) {
						SDFSLogger.getLog().debug(
								"Batch Hash Exists completed for "
										+ rsp.getSender() + " returned="
										+ rsp.getValue());
						@SuppressWarnings("unchecked")
						List<Boolean> rst = (List<Boolean>) rsp.getValue();
						byte id = soc.serverState.get(rsp.getSender()).id;
						for (int i = 0; i < rst.size(); i++) {
							if (rst.get(i) != null) {
								boolean doop = rst.get(i);
								WritableCacheBuffer buff = chunks.get(i);
								buff.setDoop(doop);
								buff.addHashLoc(id);
								buff.setBatchwritten(true);
								//proc++;
							}
						}
					}
				}
			}
			//tm = System.currentTimeMillis() - tm;
			//SDFSLogger.getLog().info("ph 6 time was " + tm + " blocks processed " +proc);

		} catch (Throwable e) {
			SDFSLogger.getLog().error("error while writing hash", e);
			throw new IOException(e);
		}
	}

	public List<WritableCacheBuffer> getHashes() {
		return this.chunks;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.BATCH_WRITE_HASH_CMD;
	}

}
