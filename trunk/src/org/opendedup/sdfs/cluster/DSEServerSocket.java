package org.opendedup.sdfs.cluster;

import java.io.DataInputStream;




import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.cmds.AddVolCmd;
import org.opendedup.sdfs.cluster.cmds.DSEServer;
import org.opendedup.sdfs.filestore.HashChunk;
//import org.opendedup.sdfs.filestore.gc.StandAloneGCScheduler;
import org.opendedup.sdfs.io.SparseDataChunk;
import org.opendedup.sdfs.cluster.cmds.NetworkCMDS;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.sdfs.servers.HCServiceProxy;
import org.opendedup.util.StringUtils;

public class DSEServerSocket implements RequestHandler, MembershipListener,
		MessageListener, Runnable, ClusterSocket {

	JChannel channel;

	public MessageDispatcher disp;

	RspList<?> rsp_list;
	final HashMap<Address, DSEServer> serverState = new HashMap<Address, DSEServer>();
	DSEServer server = null;
	DSEServer[] servers = new DSEServer[200];
	Thread th = null;
	boolean closed = false;
	private final String config;
	private final String clusterID;
	private final byte id;
	private ArrayList<DSEServer> sal = new ArrayList<DSEServer>();
	private ArrayList<DSEServer> nal = new ArrayList<DSEServer>();
	final HashMap<String, Address> volumes = new HashMap<String, Address>();
	LockService lock_service = null;
	private boolean peermaster = false;
	public final ReentrantLock gcUpdateLock = new ReentrantLock();

	public DSEServerSocket(String config, String clusterID, byte id,
			ArrayList<String> remoteVolumes) throws Exception {
		this.config = config;
		this.clusterID = clusterID;
		this.id = id;
		this.start(remoteVolumes);
	}

	private void start(ArrayList<String> remoteVolumes) throws Exception {
		SDFSLogger.getLog().info(
				"Starting Cluster DSE Listener cluserID=" + this.clusterID
						+ " nodeID=" + id + " configPath=" + this.config);
		
		channel = new JChannel(config);
		disp = new MessageDispatcher(channel, null, null, this);
		disp.setMembershipListener(this);
		disp.setMessageListener(this);
		channel.connect(clusterID);
		for (String vol : remoteVolumes) {
			if(vol != null) {
			volumes.put(vol, null);
			new AddVolCmd(vol).executeCmd(this);
			}
		}
		server = new DSEServer(Main.serverHostName, id, DSEServer.SERVER);
		server.address = channel.getAddress();
		server.currentSize = HCServiceProxy.getSize();
		server.maxSize = HCServiceProxy.getMaxSize();
		server.freeBlocks = HCServiceProxy.getFreeBlocks();
		server.pageSize = HCServiceProxy.getPageSize();
		server.address = channel.getAddress();
		server.useSSL = Main.serverUseSSL;
		server.dseport = Main.serverPort;
		server.location = Main.DSEClusterNodeLocation;
		server.rack = Main.DSEClusterNodeRack;

		channel.getState(null, 10000);
		lock_service = new LockService(channel);
		if (servers[this.id] != null) {
			String err = "Duplicate ID found [" + this.id + "] with "
					+ servers[this.id].address;
			SDFSLogger.getLog().fatal(err);
			throw new IOException(err);
		}
		this.addSelfToState();
		th = new Thread(this);
		th.start();
		NetworkUnicastServer.init();
		SDFSLogger.getLog().info("Started Cluster DSE Listener");
	}

	public void close() {
		this.closed = true;
		try {
			NetworkUnicastServer.close();
		} catch (Exception e) {
		}
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
			// SDFSLogger.getLog().debug("recieved cmd " +cmd);
			switch (cmd) {
			case NetworkCMDS.UPDATE_DSE: {
				try {
					DSEServer s = new DSEServer();
					s.fromByte(buffer);

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
						if (s.serverType == DSEServer.SERVER) {
							synchronized (sal) {
								sal.remove(s);
								sal.add(s);
							}
						} else if (s.serverType == DSEServer.CLIENT) {
							synchronized (nal) {
								nal.remove(s);
								nal.add(s);
							}
							synchronized (volumes) {
								if(s.volumeName != null)
								volumes.put(s.volumeName, msg.src());
							}
						}
					}
					//SDFSLogger.getLog().debug(
					//		s + " - hashmap size : " + serverState.size()
					//				+ " sorted arraylist size : " + sal.size());
				} catch (Exception e) {
					SDFSLogger.getLog().error("Unable to update dse state ", e);
					throw new IOException(e);
				}
				rtrn = new Boolean(true);
				break;
			}
			case NetworkCMDS.HASH_EXISTS_CMD: {
				byte[] hash = new byte[buf.getShort()];
				buf.get(hash);
				rtrn = new Boolean(HCServiceProxy.hashExists(hash));
				break;
			}
			case NetworkCMDS.BATCH_HASH_EXISTS_CMD: {
				byte[] arb = new byte[buf.getInt()];
				buf.get(arb);
				@SuppressWarnings("unchecked")
				ArrayList<SparseDataChunk> chunks = (ArrayList<SparseDataChunk>) Util.objectFromByteBuffer(arb);
				ArrayList<Boolean> rsults = new ArrayList<Boolean>(chunks.size());
				for(int i = 0;i<chunks.size();i++) {
					rsults.add(i,new Boolean(HCServiceProxy.hashExists(chunks.get(i).getHash())));
				}
				rtrn = rsults;
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
					// throw new IOException(e);
					rtrn = null;
					break;
				}
			}
			case NetworkCMDS.RUN_CLAIM: {
				byte[] ob = new byte[buf.getInt()];
				buf.get(ob);
				SDFSEvent evt = (SDFSEvent) Util.objectFromByteBuffer(ob);
				HCServiceProxy.processHashClaims(evt);
				SDFSLogger.getLog().debug("sending back claim chunks cmd");
				rtrn = evt;
				break;
			}
			case NetworkCMDS.RUN_REMOVE: {
				SDFSLogger.getLog().debug("recieved remove chunks cmd");
				long timestamp = buf.getLong();
				SDFSLogger.getLog().debug("recieved remove chunks cmd after [" + new Date(timestamp) + "]");
				byte fb = buf.get();
				boolean force = false;
				if (fb == 1)
					force = true;
				byte[] ob = new byte[buf.getInt()];
				buf.get(ob);
				SDFSEvent evt = (SDFSEvent) Util.objectFromByteBuffer(ob);
				HCServiceProxy.removeStailHashes(timestamp, force, evt);
				rtrn = evt;
				break;
			}
			case NetworkCMDS.LIST_VOLUMES: {
				rtrn = this.getVolumes();
				break;
			}
			case NetworkCMDS.RM_VOLUME: {
				byte[] sb = new byte[buf.getInt()];
				buf.get(sb);
				String volume = new String(sb);
				Address addr = this.volumes.get(volume);
				if (addr != null)
					throw new IOException("Volume is mounted by " + addr);
				this.volumes.remove(volume);
				rtrn = new Boolean(true);
				break;
			}
			case NetworkCMDS.ADD_VOLUME: {
				byte[] sb = new byte[buf.getInt()];
				buf.get(sb);
				String volume = new String(sb);
				synchronized (volumes) {
					if (!this.volumes.containsKey(volume) && volume != null)
						this.volumes.put(volume, null);
				}
				rtrn = new Boolean(true);
				break;
			}
			case NetworkCMDS.FIND_GC_MASTER_CMD: {
				rtrn = new Boolean(false);
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
		if (new_view instanceof MergeView) {
			lock_service.unlockAll();
		}
		SDFSLogger.getLog().debug(
				"** view: " + new_view + " peer master = "
						+ Boolean.toString(this.peermaster));
		synchronized (serverState) {
			Iterator<Address> iter = serverState.keySet().iterator();
			while (iter.hasNext()) {
				Address addr = iter.next();
				if (!new_view.containsMember(addr)) {
					DSEServer s = serverState.remove(addr);
					synchronized (servers) {
						servers[s.id] = null;
					}
					synchronized (sal) {
						sal.remove(s);
					}
					synchronized (nal) {
						nal.remove(s);
					}
					if (s.serverType == DSEServer.CLIENT) {
						synchronized (volumes) {
							if(s.volumeName != null)
							volumes.put(s.volumeName, null);
						}
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
		SDFSLogger.getLog().debug("message recieved " + msg);
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
				synchronized (servers) {
					Iterator<DSEServer> iter = serverState.values().iterator();
					while (iter.hasNext()) {
						DSEServer s = iter.next();
						servers[s.id] = s;
					}
				}
				synchronized (sal) {
					sal.clear();
					Iterator<DSEServer> iter = serverState.values().iterator();
					while (iter.hasNext()) {
						DSEServer s = iter.next();
						if (s.serverType == DSEServer.SERVER)
							sal.add(s);
					}
				}
				synchronized (nal) {
					nal.clear();
					Iterator<DSEServer> iter = serverState.values().iterator();
					while (iter.hasNext()) {
						DSEServer s = iter.next();
						if (s.serverType == DSEServer.CLIENT) {
							nal.add(s);
							synchronized (volumes) {
								if(s.volumeName != null)
								volumes.put(s.volumeName, s.address);
							}
						}
					}
				}
			}
			SDFSLogger.getLog().debug(
					"received state (" + list.size() + " state");
		} catch (Exception e) {
			SDFSLogger.getLog().error("error while getting state", e);
		}
	}

	public void addSelfToState() {
		synchronized (serverState) {
			serverState.put(server.address, server);
			synchronized (servers) {
				DSEServer cs = servers[server.id];
				if (cs != null && !cs.address.equals(server.address))
					SDFSLogger
							.getLog()
							.warn("Two servers have the same id ["
									+ server.id
									+ "] but are running on different addresses current="
									+ cs.address.toString() + " new="
									+ server.address.toString());
				servers[server.id] = server;
			}
			if (server.serverType == DSEServer.SERVER) {
				synchronized (sal) {
					sal.remove(server);
					sal.add(server);
				}
			} else if (server.serverType == DSEServer.CLIENT) {
				synchronized (nal) {
					nal.remove(server);
					nal.add(server);
				}
			}
		}
	}

	@Override
	public void run() {
		while (!closed) {

			try {
				server.address = channel.getAddress();
				server.currentSize = HCServiceProxy.getSize();
				server.maxSize = HCServiceProxy.getMaxSize();
				server.freeBlocks = HCServiceProxy.getFreeBlocks();
				server.pageSize = HCServiceProxy.getPageSize();
				server.address = channel.getAddress();
				server.useSSL = Main.serverUseSSL;
				server.dseport = Main.serverPort;
				server.location = Main.DSEClusterNodeLocation;
				server.rack = Main.DSEClusterNodeRack;
				this.addSelfToState();
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

	@Override
	public List<DSEServer> getStorageNodes() {
		ArrayList<DSEServer> sn = null;
		synchronized (this.sal) {
			@SuppressWarnings("unchecked")
			ArrayList<DSEServer> clone = (ArrayList<DSEServer>) sal.clone();
			sn = clone;
		}
		return sn;
	}

	@Override
	public List<DSEServer> getNameNodes() {
		ArrayList<DSEServer> sn = null;
		synchronized (this.nal) {
			@SuppressWarnings("unchecked")
			ArrayList<DSEServer> clone = (ArrayList<DSEServer>) nal.clone();
			sn = clone;
		}
		return sn;
	}

	@Override
	public Lock getLock(String name) {
		return this.lock_service.getLock(name);
	}

	@Override
	public boolean isPeerMaster() {
		return this.peermaster;
	}

	@Override
	public List<String> getVolumes() {
		ArrayList<String> vols = new ArrayList<String>();
		synchronized (volumes) {
			Iterator<String> iter = volumes.keySet().iterator();
			while (iter.hasNext())
				vols.add(iter.next());
		}
		return vols;
	}

	@Override
	public MessageDispatcher getDispatcher() {
		return this.disp;
	}

	@Override
	public Address getAddressForVol(String volumeName) {
		Address addr = null;
		synchronized (volumes) {
			if (volumes.containsKey(volumeName))
				addr = volumes.get(volumeName);
		}
		return addr;
	}
	
	@Override
	public DSEServer getServer() {
		return this.server;
	}

}
