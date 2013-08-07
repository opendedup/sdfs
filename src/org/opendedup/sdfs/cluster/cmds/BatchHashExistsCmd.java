package org.opendedup.sdfs.cluster.cmds;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RspFilter;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.cluster.DSEClientSocket;

public class BatchHashExistsCmd implements IOClientCmd {
	byte[] hashes;
	boolean exists = false;
	RequestOptions opts = null;
	byte[] resp = new byte[8];
	boolean waitforall = false;
	byte numtowaitfor = 1;
	boolean meetsRudundancy = false;
	//int rsz = 0;

	public BatchHashExistsCmd(byte[] hashes, boolean waitforall,byte numtowaitfor) {
		this.hashes = hashes;
		this.waitforall = waitforall;
		resp[0] = -1;
		this.numtowaitfor = numtowaitfor;
	}

	@Override
	public void executeCmd(final DSEClientSocket soc) throws IOException {
		if (waitforall)
			opts = new RequestOptions(ResponseMode.GET_ALL,
					0, true);
		opts.setFlags(Message.Flag.DONT_BUNDLE);
		opts.setFlags(Message.Flag.NO_FC);
		opts.setFlags(Message.Flag.OOB);
		byte[] b = new byte[1 + 2 + 2 + hashes.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.BATCH_HASH_EXISTS_CMD);
		buf.putInt(hashes.length);
		buf.put(hashes);
		try {
			List<Address> servers = soc.getServers();
			soc.disp.castMessage(servers,
					new Message(null, null, buf.array()), opts);

		} catch (Exception e) {
			SDFSLogger.getLog().error("error while getting hash", e);
			throw new IOException(e);
		}
	}

	public byte[] getHashes() {
		return this.hashes;
	}

	public byte[] getResponse() {
		return this.resp;
	}

	public boolean exists() {
		return this.exists;
	}
	
	public boolean meetsRedundancyRequirements() {
		return this.meetsRudundancy;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.BATCH_HASH_EXISTS_CMD;
	}

}
