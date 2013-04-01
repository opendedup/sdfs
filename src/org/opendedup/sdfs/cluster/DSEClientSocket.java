package org.opendedup.sdfs.cluster;

import java.io.DataInputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.mtools.FDisk;
import org.opendedup.sdfs.cluster.cmds.DSEServer;
import org.opendedup.sdfs.cluster.cmds.NetworkCMDS;
import org.opendedup.sdfs.notification.SDFSEvent;

public class DSEClientSocket implements RequestHandler, MembershipListener,
		MessageListener {

	JChannel channel;

	public MessageDispatcher disp;

	RspList<?> rsp_list;
	String props; // to be set by application programmer
	public final HashMap<Address, DSEServer> serverState = new HashMap<Address, DSEServer>();
	public DSEServer server = null;
	public DSEServer[] servers = new DSEServer[200];
	private ArrayList<DSEServer> sal = new ArrayList<DSEServer>();
	boolean closed = false;
	private final String config;
	private final String clusterID;

	public DSEClientSocket(String config, String clusterID) throws Exception {
		this.config = config;
		this.clusterID = clusterID;
		this.start();
	}

	public void start() throws Exception {
		SDFSLogger.getLog().info("Starting Cluster DSE Listener");
		channel = new JChannel(config);
		disp = new MessageDispatcher(channel, null, null, this);
		disp.setMembershipListener(this);
		disp.setMessageListener(this);
		channel.connect(clusterID);
		server = new DSEServer(channel.getAddressAsString(), (byte) 0, false);
		server.address = channel.getAddress();
		serverState.put(channel.getAddress(), server);
		channel.getState(null, 10000);
		SDFSLogger.getLog().info(
				"Started Cluster DSE Listener server cluster size is "
						+ this.sal.size());
	}

	public void close() {
		this.closed = true;

		channel.close();
		disp.stop();
	}

	public Object handle(Message msg) throws Exception {
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
						synchronized (sal) {
							sal.remove(s);
							sal.add(s);
							Collections.sort(sal, new CustomComparator());
						}
					}
				}
				SDFSLogger.getLog().debug(
						s + " - hashmap size : " + serverState.size()
								+ " sorted arraylist size : " + sal.size());
			} catch (Exception e) {
				SDFSLogger.getLog().error("Unable to update dse state ", e);
				throw new IOException(e);
			}
			rtrn = new Boolean(true);
		}
		case NetworkCMDS.RUN_FDISK: {
			byte[] ob = new byte[buf.getInt()];
			buf.get(ob);
			SDFSEvent evt = (SDFSEvent) Util.objectFromByteBuffer(ob);
			new FDisk(evt);
			rtrn = evt;
		}
		}

		return rtrn;

	}

	public List<Address> getServers() {
		ArrayList<Address> al = new ArrayList<Address>();
		for (DSEServer server : sal) {
			al.add(server.address);
		}
		return al;
	}

	public List<Address> getServers(byte max) {
		ArrayList<Address> al = new ArrayList<Address>();
		if (sal.size() < max) {
			for (DSEServer server : sal) {
				al.add(server.address);
			}
		} else {
			for (int i = 0; i < max; i++) {
				DSEServer server = sal.get(i);
				al.add(server.address);
			}
		}
		return al;
	}

	public Address getServer(byte[] slist, int start) throws IOException {
		for (int i = start; i < slist.length; i++) {
			if (slist[i] > 0) {
				DSEServer svr = servers[slist[i]];
				if (svr != null)
					return svr.address;
			}
		}
		throw new IOException("no servers available to fulfill request");
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
					synchronized (sal) {
						sal.remove(s);
					}

				}
			}
		}
		SDFSLogger.getLog().debug(
				server + " - size : " + serverState.size()
						+ " arraylist size : " + sal.size());
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
						sal.add(s);
					}
					Collections.sort(sal, new CustomComparator());
				}
			}
			SDFSLogger.getLog().debug(
					"received state (" + list.size() + " state");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public long getMaxSize() {
		long sz = 0;
		for (DSEServer s : sal) {
			sz = sz + s.maxSize;
			SDFSLogger.getLog().debug("sz=" + sz);
		}
		return sz;
	}

	public long getCurrentSize() {
		long sz = 0;
		for (DSEServer s : sal) {
			sz = sz + s.currentSize;
		}
		return sz;
	}

	public long getFreeBlocks() {
		long sz = 0;
		for (DSEServer s : sal) {
			sz = sz + s.freeBlocks;
		}
		return sz;
	}

	private class CustomComparator implements Comparator<DSEServer> {
		@Override
		public int compare(DSEServer o1, DSEServer o2) {
			long fs1 = o1.maxSize - o1.currentSize;
			long fs2 = o2.maxSize - o2.currentSize;
			if (fs1 > fs2)
				return 1;
			else if (fs1 < fs2)
				return -1;
			else
				return 0;
		}
	}

}
