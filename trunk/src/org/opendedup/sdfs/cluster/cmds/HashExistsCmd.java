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
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.DSEClientSocket;

public class HashExistsCmd implements IOClientCmd {
	byte[] hash;
	boolean exists = false;
	RequestOptions opts = null;
	byte[] resp = new byte[8];
	boolean waitforall = false;
	byte numtowaitfor = 1;
	boolean meetsRudundancy = false;
	int csz = 0;

	public HashExistsCmd(byte[] hash, boolean waitforall, byte numtowaitfor) {
		this.hash = hash;
		this.waitforall = waitforall;
		resp[0] = -1;
		this.numtowaitfor = numtowaitfor;
	}

	@Override
	public void executeCmd(final DSEClientSocket soc) throws IOException {
		if (waitforall)
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout, true,

					new RspFilter() {
						private final ReentrantLock lock = new ReentrantLock();
						int pos = 1;

						public boolean needMoreResponses() {

							return true;
						}

						@Override
						public boolean isAcceptable(Object response,
								Address arg1) {

							if (response instanceof Boolean) {
								boolean rsp = ((Boolean) response)
										.booleanValue();
								if (rsp) {
									lock.lock();
									resp[0] = 1;
									resp[pos] = soc.serverState.get(arg1).id;
									pos++;
									csz++;
									exists = true;
									lock.unlock();
								} else {
									lock.lock();
									if (resp[0] == -1)
										resp[0] = 0;
									lock.unlock();
								}
								return rsp;
							} else {

								return false;
							}
						}

					});
		else {
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout, false,

					new RspFilter() {
						private final ReentrantLock lock = new ReentrantLock();

						int pos = 1;

						@Override
						public boolean needMoreResponses() {
							return !meetsRudundancy;
						}

						@Override
						public boolean isAcceptable(Object response,
								Address arg1) {
							try {
								boolean rsp = ((Boolean) response)
										.booleanValue();
								if (rsp) {
									lock.lock();
									resp[0] = 1;
									resp[pos] = soc.serverState.get(arg1).id;

									if (pos >= numtowaitfor) {
										// SDFSLogger.getLog().info("meets requirements");
										meetsRudundancy = true;
									}
									csz++;
									pos++;
									exists = rsp;
									lock.unlock();
								} else {

									lock.lock();
									if (resp[0] == -1)
										resp[0] = 0;
									lock.unlock();
								}
								return true;

							} catch (Exception e) {
								SDFSLogger.getLog().warn(
										"malformed hashexists msg from "
												+ arg1.toString(), e);
								return false;
							}
						}

					});
		}
		// opts.setFlags(Message.Flag.DONT_BUNDLE);
		// opts.setFlags(Message.Flag.NO_TOTAL_ORDER);
		opts.setFlags(Message.Flag.OOB);
		opts.setAnycasting(true);
		byte[] b = new byte[1 + 2 + 2 + hash.length];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NetworkCMDS.HASH_EXISTS_CMD);
		buf.putShort((short) hash.length);
		buf.put(hash);
		try {
			List<Address> servers = soc.getServers();
			soc.disp.castMessage(servers, new Message(null, null, buf.array()),
					opts);

		} catch (Exception e) {
			SDFSLogger.getLog().error("error while getting hash", e);
			throw new IOException(e);
		}
	}

	public byte[] getHash() {
		return this.hash;
	}

	public byte[] getResponse() {
		return this.resp;
	}

	public boolean exists() {
		return this.exists;
	}

	public int responses() {
		return this.csz;
	}

	public boolean meetsRedundancyRequirements() {
		return this.meetsRudundancy;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.HASH_EXISTS_CMD;
	}

}
