package org.opendedup.sdfs.cluster;

import java.io.DataInputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.cmds.DSEServer;
import org.opendedup.sdfs.filestore.HashChunk;
import org.opendedup.sdfs.cluster.cmds.NetworkCMDS;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.StringUtils;

public class DSEServerSocket implements RequestHandler, MembershipListener,
		MessageListener, Runnable {

	JChannel channel;

	public MessageDispatcher disp;

	RspList<?> rsp_list;
	String props; // to be set by application programmer
	final HashMap<Address, DSEServer> serverState = new HashMap<Address, DSEServer>();
	DSEServer server = null;
	DSEServer[] servers = new DSEServer[200];
	Thread th = null;
	boolean closed = false;
	private final String config;
	private final String clusterID;
	private final byte id;

	public DSEServerSocket(String config, String clusterID, byte id)
			throws Exception {
		this.config = config;
		this.clusterID = clusterID;
		this.id = id;
		this.start();
	}

	public void start() throws Exception {
		SDFSLogger.getLog().info(
				"Starting Cluster DSE Listener cluserID=" + this.clusterID
						+ " nodeID=" + id + " configPath=" + this.config);
		channel = new JChannel(config);
		disp = new MessageDispatcher(channel, null, null, this);
		disp.setMembershipListener(this);
		disp.setMessageListener(this);
		channel.connect(clusterID);
		server = new DSEServer(channel.getAddressAsString(), id, false);
		server.address = channel.getAddress();
		this.serverState.put(channel.getAddress(), server);
		channel.getState(null, 10000);

		// new StateHandler().start();
		th = new Thread(this);
		th.start();
		SDFSLogger.getLog().info("Started Cluster DSE Listener");

	}

	public void close() {
		this.closed = true;
		try {
			th.interrupt();
		} catch (Exception e) {

		}
		channel.close();
		disp.stop();
	}

	public Object handle(Message msg) throws Exception {
		try {
			byte[] buffer = msg.getBuffer();
			ByteBuffer buf = ByteBuffer.wrap(buffer);
			buf.position(msg.getOffset());
			byte cmd = buf.get();
			Object rtrn = null;
			switch (cmd) {
				case NetworkCMDS.UPDATE_DSE: {
					try {
						DSEServer s = new DSEServer();
						s.fromByte(buffer);
						if (!s.client) {
							synchronized (serverState) {
								serverState.put(msg.getSrc(), s);
								synchronized (servers) {
									DSEServer cs = servers[s.id];
									if (cs != null && !cs.address.equals(s.address))
										SDFSLogger
												.getLog()
												.warn("Two servers have the same id ["
														+ s.id
														+ "] but are running on different addresses current="
														+ cs.address.toString()
														+ " new="
														+ s.address.toString());
									servers[s.id] = s;
								}
							}
						}
						SDFSLogger.getLog().debug(
								s + " - hashmap size : " + serverState.size());
					} catch (Exception e) {
						SDFSLogger.getLog().error("Unable to update dse state ", e);
						throw new IOException(e);
					}
					rtrn = new Boolean(true);
					break;
				}
				case NetworkCMDS.HASH_EXISTS_CMD: {
					short hops = buf.getShort();
					byte[] hash = new byte[buf.getShort()];
					buf.get(hash);
					rtrn = new Boolean(HCServiceProxy.hashExists(hash, hops));
					break;
				}
				case NetworkCMDS.WRITE_HASH_CMD: {
					byte[] hash = new byte[buf.getShort()];
					buf.get(hash);
					int len = buf.getInt();
					if (len != Main.CHUNK_LENGTH)
						throw new IOException("invalid chunk length " + len);
					byte[] chunkBytes = new byte[len];
					buf.get(chunkBytes);
					boolean dup = false;
					byte[] b = HCServiceProxy.writeChunk(hash, chunkBytes, len,
							len, true);
					if (b[0] == 1)
						dup = true;
					// SDFSLogger.getLog().debug("Writing " +
					// StringUtils.getHexString(hash) + " done=" +done);
					rtrn = new Boolean(dup);
					break;
				}
				case NetworkCMDS.FETCH_CMD: {
					byte[] hash = new byte[buf.getShort()];
					buf.get(hash);
					HashChunk dChunk = null;
					try {
						dChunk = HCServiceProxy.fetchHashChunk(hash);
						rtrn = dChunk.getData();
						break;
					} catch (NullPointerException e) {
						SDFSLogger.getLog().warn(
								"chunk " + StringUtils.getHexString(hash)
										+ " does not exist");
						throw new IOException("chunk "
								+ StringUtils.getHexString(hash)
								+ " does not exist");
					}
				}
				case NetworkCMDS.BULK_FETCH_CMD: {
					throw new IOException("BULK_FETCH_CMD not implemented");
				}
				case NetworkCMDS.RUN_CLAIM: {
					byte[] ob = new byte[buf.getInt()];
					buf.get(ob);
					SDFSEvent evt = (SDFSEvent) Util.objectFromByteBuffer(ob);
					HCServiceProxy.processHashClaims(evt);
					rtrn = evt;
					break;
				} 
				case NetworkCMDS.RUN_REMOVE : {
					long timestamp = buf.getLong();
					byte fb = buf.get();
					boolean force = false;
					if(fb ==1)
						force = true;
					byte[] ob = new byte[buf.getInt()];
					buf.get(ob);
					SDFSEvent evt = (SDFSEvent) Util.objectFromByteBuffer(ob);
					HCServiceProxy.removeStailHashes(timestamp, force, evt);
					rtrn = evt;
					break;
				}
			}
			return rtrn;
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to handle request", e);
			throw e;
		}

	}

	@Override
	public void block() {
		// TODO Auto-generated method stub

	}

	@Override
	public void unblock() {
		// TODO Auto-generated method stub

	}

	public void viewAccepted(View new_view) {
		SDFSLogger.getLog().debug("** view: " + new_view);
		synchronized (serverState) {
			Iterator<Address> iter = serverState.keySet().iterator();
			while (iter.hasNext()) {
				Address addr = iter.next();
				if (!new_view.containsMember(addr)) {
					DSEServer s = serverState.remove(addr);
					synchronized (servers) {
						servers[s.id] = null;
					}

				}
			}
		}
		SDFSLogger.getLog().debug(server + " - size : " + serverState.size());
	}

	public void suspect(Address mbr) {
		SDFSLogger.getLog().warn(mbr.toString() + " is coming off line");
	}

	public void receive(Message msg) {

		try {
			DSEServer server = (DSEServer) msg.getObject();
			synchronized (serverState) {
				serverState.put(msg.getSrc(), server);
				synchronized (servers) {
					servers[server.id] = server;
				}
			}
			System.out.println(server + " - size : " + serverState.size());
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get recieve msg", e);
		}
	}

	public void getState(java.io.OutputStream output) {
		synchronized (serverState) {
			try {
				Util.objectToStream(serverState, new DataOutputStream(output));
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to get state", e);
			}
		}
	}

	public void setState(InputStream input) {
		try {

			@SuppressWarnings("unchecked")
			HashMap<Address, DSEServer> list = (HashMap<Address, DSEServer>) Util
					.objectFromStream(new DataInputStream(input));
			synchronized (serverState) {
				serverState.clear();
				serverState.putAll(list);
			}
			SDFSLogger.getLog().debug(
					"received state (" + list.size() + " state");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (!closed) {

			try {
				server.currentSize = HCServiceProxy.getSize();
				server.maxSize = HCServiceProxy.getMaxSize();
				server.freeBlocks = HCServiceProxy.getFreeBlocks();
				server.pageSize = HCServiceProxy.getPageSize();
				server.address = channel.getAddress();
				rsp_list = disp.castMessage(null, new Message(null, null,
						server.getBytes()), new RequestOptions(
						ResponseMode.GET_NONE, 0));
			} catch (Exception e) {
				SDFSLogger.getLog()
						.error("unable to send server update msg", e);
			}
			Util.sleep(10000);
		}

	}

}
