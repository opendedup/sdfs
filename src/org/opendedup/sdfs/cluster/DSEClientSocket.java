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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

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
import org.opendedup.mtools.FDisk;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.cluster.cmds.AddVolCmd;
import org.opendedup.sdfs.cluster.cmds.FindGCMasterCmd;
import org.opendedup.sdfs.cluster.cmds.ListVolsCmd;
import org.opendedup.sdfs.cluster.cmds.NetworkCMDS;
import org.opendedup.sdfs.cluster.cmds.StopGCMasterCmd;
import org.opendedup.sdfs.filestore.gc.StandAloneGCScheduler;
import org.opendedup.sdfs.network.HashClientPool;
import org.opendedup.sdfs.notification.SDFSEvent;

public class DSEClientSocket implements RequestHandler, MembershipListener,
		MessageListener, Runnable, ClusterSocket {

	JChannel channel;

	public MessageDispatcher disp;

	RspList<?> rsp_list;
	String props; // to be set by application programmer
	public final HashMap<Address, DSEServer> serverState = new HashMap<Address, DSEServer>();
	public DSEServer server = null;
	public DSEServer[] servers = new DSEServer[200];
	private ReentrantReadWriteLock ssl = new ReentrantReadWriteLock();
	public HashClientPool[] pools = new HashClientPool[200];
	private ReentrantReadWriteLock pl = new ReentrantReadWriteLock();
	private ArrayList<DSEServer> sal = new ArrayList<DSEServer>();
	private ArrayList<Address> saal = new ArrayList<Address>();
	private ReentrantReadWriteLock sl = new ReentrantReadWriteLock();
	private ArrayList<DSEServer> nal = new ArrayList<DSEServer>();
	private ReentrantReadWriteLock nl = new ReentrantReadWriteLock();
	final HashMap<String, Address> volumes = new HashMap<String, Address>();
	boolean closed = false;
	private final String config;
	private final String clusterID;
	LockService lock_service;
	private boolean peermaster = false;
	StandAloneGCScheduler gcscheduler = null;
	public final ReentrantLock gcUpdateLock = new ReentrantLock();

	public DSEClientSocket(String config, String clusterID,
			ArrayList<String> remoteVolumes) throws Exception {
		this.config = config;
		this.clusterID = clusterID;
		this.start(remoteVolumes);
	}

	public void start(ArrayList<String> remoteVolumes) throws Exception {
		SDFSLogger.getLog().info("Starting Cluster DSE Listener");

		channel = new JChannel(config);
		disp = new MessageDispatcher(channel, null, null, this);
		disp.setMembershipListener(this);
		disp.setMessageListener(this);
		synchronized (volumes) {
			channel.connect(clusterID);
			for (String vol : remoteVolumes) {
				if (vol != null) {
					volumes.put(vol, null);
					new AddVolCmd(vol).executeCmd(this);
				}
			}
			ListVolsCmd cmd = new ListVolsCmd();
			cmd.executeCmd(this);
			HashMap<String, Address> m = cmd.getResults();
			Set<String> vols = m.keySet();
			for (String vol : vols) {
				volumes.put(vol, m.get(vol));
			}
		}
		server = new DSEServer(channel.getAddressAsString(), (byte) 0,
				DSEServer.CLIENT);
		server.address = channel.getAddress();
		server.volumeName = Main.volume.getName();
		server.serverType = DSEServer.CLIENT;
		// serverState.put(channel.getAddress(), server);
		this.addSelfToState();
		channel.getState(null, 10000);
		SDFSLogger.getLog().info(
				"Started Cluster DSE Listener dse client cluster size is "
						+ this.sal.size());
		if (this.sal.size() == 0) {
			SDFSLogger.getLog().fatal("No DSE Servers found. Exiting");
			throw new IOException("No DSE Servers found");
		}
		lock_service = new LockService(channel);
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
											+ cs.address.toString() + " new="
											+ s.address.toString());
						servers[s.id] = s;
					}
					if (s.serverType == DSEServer.SERVER) {
						WriteLock l = this.sl.writeLock();
						l.lock();
						sal.remove(s);
						sal.add(s);
						saal.remove(s.address);
						saal.add(s.address);
						Collections.sort(sal, new CustomComparator());
						l.unlock();
						l = this.pl.writeLock();
						l.lock();
						if (pools[s.id] == null) {
							SDFSLogger.getLog().debug(
									"creating pool for " + s.id);
							pools[s.id] = s.createPool();
						}
						l.unlock();
					} else if (s.serverType == DSEServer.CLIENT) {
						WriteLock l = this.nl.writeLock();
						l.lock();
						nal.remove(s);
						nal.add(s);
						l.unlock();
					}
				}
				// SDFSLogger.getLog().debug(
				// s + " - hashmap size : " + serverState.size()
				// + " sorted arraylist size : " + sal.size());
			} catch (Exception e) {
				SDFSLogger.getLog().error("Unable to update dse state ", e);
				throw new IOException(e);
			}
			rtrn = new Boolean(true);
			break;
		}
		case NetworkCMDS.RUN_FDISK: {
			SDFSEvent evt = SDFSEvent
					.gcInfoEvent("Remote SDFS Volume Cleanup Initiated by "
							+ msg.getSrc() + " for " + Main.volume.getName());
			new FDisk(evt);
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

			if (volume.equals(server.volumeName))
				throw new IOException("Volume is mounted by " + server.address);
			this.volumes.remove(volume);
			rtrn = new Boolean(true);
			break;
		}
		case NetworkCMDS.ADD_VOLUME: {
			byte[] sb = new byte[buf.getInt()];
			buf.get(sb);
			String volume = new String(sb);
			synchronized (volumes) {
				if (!this.volumes.containsKey(volume))
					if (volume != null)
						this.volumes.put(volume, null);
			}
			rtrn = new Boolean(true);
			break;
		}
		case NetworkCMDS.RUN_CLAIM: {
			SDFSLogger.getLog().debug("recieved claim chunks cmd");
			rtrn = null;
			break;
		}
		case NetworkCMDS.RUN_REMOVE: {
			SDFSLogger.getLog().debug("recieved remove chunks cmd");
			rtrn = null;
			break;
		}
		case NetworkCMDS.HASH_EXISTS_CMD: {
			rtrn = new Boolean(false);
			break;
		}
		case NetworkCMDS.FIND_GC_MASTER_CMD: {
			this.gcUpdateLock.lock();
			try {
				rtrn = new Boolean(this.gcscheduler != null);
			} finally {
				this.gcUpdateLock.unlock();
			}
			break;
		}
		case NetworkCMDS.STOP_GC_MASTER_CMD: {
			this.gcUpdateLock.lock();
			try {
				if (this.gcscheduler != null) {
					this.stopGC();
					rtrn = new Boolean(true);
				} else
					rtrn = new Boolean(false);
			} finally {
				this.gcUpdateLock.unlock();
			}
			break;
		}
		case NetworkCMDS.FIND_VOLUME_OWNER: {
			byte[] sb = new byte[buf.getInt()];
			buf.get(sb);
			String volume = new String(sb);
			if (volume.equals(server.volumeName))
				rtrn = new Boolean(true);
			else
				rtrn = new Boolean(false);
		}
		}
		return rtrn;
	}

	@SuppressWarnings("unchecked")
	public List<Address> getServers() {
		synchronized (saal) {
			return (List<Address>) saal.clone();
		}
	}

	public List<Address> getServers(byte max, byte[] ignoredHosts) {
		ArrayList<Address> al = new ArrayList<Address>();
		ReadLock l = this.sl.readLock();
		l.lock();
		if (sal.size() < max) {
			for (DSEServer s : sal) {
				if (!ignoreHost(s, ignoredHosts))
					al.add(s.address);
			}
		} else {
			for (int i = 0; i < max; i++) {
				DSEServer s = sal.get(i);
				if (!ignoreHost(s, ignoredHosts))
					al.add(s.address);
			}
		}
		l.unlock();
		return al;
	}

	public List<HashClientPool> getServerPools(byte max, byte[] ignoredHosts) {
		ArrayList<HashClientPool> al = new ArrayList<HashClientPool>();
		ReadLock l = this.sl.readLock();
		l.lock();
		try {
			if (Main.volume.isClusterRackAware() && sal.size() > 1) {
				HashSet<String> usedRacks = new HashSet<String>();
				if (sal.size() < max) {
					for (DSEServer s : sal) {
						if (!ignoreHost(s, ignoredHosts)
								&& !usedRacks.contains(s.rack)) {
							usedRacks.add(s.rack);
							al.add(pools[s.id]);
						}
					}
				} else {
					for (int i = 0; i < max; i++) {
						DSEServer s = sal.get(i);
						if (!ignoreHost(s, ignoredHosts)
								&& !usedRacks.contains(s.rack)) {
							usedRacks.add(s.rack);
							al.add(pools[s.id]);
						}
					}
				}
			}
			if (al.size() < max) {
				if (sal.size() < max) {
					for (DSEServer s : sal) {
						if (!ignoreHost(s, ignoredHosts))
							al.add(pools[s.id]);
					}
				} else {
					for (int i = 0; i < max; i++) {
						DSEServer s = sal.get(i);
						if (!ignoreHost(s, ignoredHosts))
							al.add(pools[s.id]);
					}
				}
			}
		} finally {
			l.unlock();
		}
		return al;
	}

	private boolean ignoreHost(DSEServer s, byte[] ignoredHosts) {
		if (ignoredHosts == null)
			return false;
		else {
			for (byte b : ignoredHosts) {
				if (b == s.id)
					return true;
			}
			return false;
		}
	}

	public Address getServer(byte[] slist, int start) throws IOException {
		ReadLock l = this.ssl.readLock();
		l.lock();
		try {
			for (int i = start; i < slist.length; i++) {
				if (slist[i] > 0) {
					DSEServer svr = servers[slist[i]];
					if (svr != null)
						return svr.address;
				}
			}
		} finally {
			l.unlock();
		}
		throw new IOException("no servers available to fulfill request");
	}

	public List<Address> getServer(byte[] slist) throws IOException {
		ArrayList<Address> al = new ArrayList<Address>();
		for (int i = 1; i < slist.length; i++) {
			if (slist[i] > 0) {
				DSEServer svr = servers[slist[i]];
				if (svr != null)
					al.add(svr.address);
			}
		}
		if (al.size() == 0)
			throw new IOException("no servers available to fulfill request");
		else
			return al;
	}

	public HashClientPool getPool(byte[] slist, int start) throws IOException {
		ReadLock l = this.pl.readLock();
		l.lock();
		try {
			for (int i = start; i < slist.length; i++) {
				if (slist[i] > 0) {
					HashClientPool svr = pools[slist[i]];
					if (svr != null)
						return svr;
				}
			}
		} finally {
			l.unlock();
		}
		throw new IOException("no pools available to fulfill request");
	}

	@Override
	public void block() {
		// TODO Auto-generated method stub

	}

	@Override
	public void unblock() {
		// TODO Auto-generated method stub

	}
	
	public void startGC() throws IOException{
		Lock l = this.lock_service.getLock("gc");
		try {
			l.lock();
			StopGCMasterCmd m = new StopGCMasterCmd();
			m.executeCmd(this);
			SDFSLogger.getLog().info("Stopped GC Master on " + m.getResults());
			this._startGC();
			SDFSLogger.getLog().error("Started GC");
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to start gc", e);
		} finally {
			l.unlock();
		}
		
	}

	private void _startGC() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException {
		this.gcUpdateLock.lock();
		try {
			synchronized (volumes) {
				ListVolsCmd cmd = new ListVolsCmd();
				cmd.executeCmd(this);
				HashMap<String, Address> m = cmd.getResults();
				Set<String> vols = m.keySet();
				for (String vol : vols) {
					volumes.put(vol, m.get(vol));
				}
			}
			if (this.gcscheduler == null)
				this.gcscheduler = new StandAloneGCScheduler();
			SDFSLogger.getLog().info("Promoted to GC Master");
		} finally {
			this.gcUpdateLock.unlock();
		}
	}

	public void stopGC() {
		this.gcUpdateLock.lock();
		try {
			if (this.gcscheduler != null)
				this.gcscheduler.close();
			SDFSLogger.getLog().info("Demoted from GC Master");
			this.gcscheduler = null;
		} finally {
			this.gcUpdateLock.unlock();
		}
	}

	@Override
	public void viewAccepted(View new_view) {

		if (new_view instanceof MergeView) {
			lock_service.unlockAll();

		}
		Address first = new_view.getMembers().get(0);
		if (first.equals(this.channel.getAddress())) {
			Lock l = this.lock_service.getLock("gc");
			try {
				l.lock();
				FindGCMasterCmd m = new FindGCMasterCmd();
				m.executeCmd(this);
				if (m.getResults() == null)
					this._startGC();
				SDFSLogger.getLog().error("Started GC");
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to start gc", e);
			} finally {
				l.unlock();
			}
			this.peermaster = true;
		} else {
			this.peermaster = false;
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
					Lock l = this.ssl.writeLock();
					l.lock();
					servers[s.id] = null;
					l.unlock();
					l = this.sl.writeLock();
					l.lock();
					sal.remove(s);
					saal.remove(s.address);
					l.unlock();
					l = this.nl.writeLock();
					l.lock();
					nal.remove(s);
					l.unlock();
					synchronized (volumes) {
						if (s.volumeName != null)
							volumes.put(s.volumeName, null);
					}
					try {
						pools[s.id].close();

					} catch (Exception e) {
						SDFSLogger.getLog().debug("unable to shutdown pool", e);
					} finally {
						l = this.pl.writeLock();
						l.lock();
						pools[s.id] = null;
						l.unlock();
					}
				}
			}
			if (serverState.size() < Main.volume.getClusterCopies())
				SDFSLogger
						.getLog()
						.warn("Will not be able to fulfill block redundancy requirements. Current number of DSE Servers is less than "
								+ Main.volume.getClusterCopies());
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
			DSEServer s = (DSEServer) msg.getObject();
			synchronized (serverState) {
				serverState.put(msg.getSrc(), s);
				Lock l = this.ssl.writeLock();
				l.lock();
				DSEServer cs = servers[s.id];
				if (cs != null && !cs.address.equals(s.address))
					SDFSLogger
							.getLog()
							.warn("Two servers have the same id ["
									+ s.id
									+ "] but are running on different addresses current="
									+ cs.address.toString() + " new="
									+ s.address.toString());
				servers[s.id] = s;
				l.unlock();
				if (s.serverType == DSEServer.SERVER) {
					l = this.sl.writeLock();
					l.lock();
					sal.remove(s);
					sal.add(s);
					saal.remove(s.address);
					saal.add(s.address);
					Collections.sort(sal, new CustomComparator());
					l.unlock();
					l = this.pl.writeLock();
					l.lock();
					if (pools[s.id] == null) {
						SDFSLogger.getLog().debug("creating pool for " + s.id);
						pools[s.id] = s.createPool();
					}
					l.unlock();
				} else if (s.serverType == DSEServer.CLIENT) {
					l = this.nl.writeLock();
					l.lock();
					nal.remove(s);
					nal.add(s);
					l.unlock();
					synchronized (volumes) {
						if (s.volumeName != null)
							volumes.put(s.volumeName, s.address);
					}
				}
			}
			SDFSLogger.getLog().debug(
					server + " - size : " + serverState.size());
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

				Iterator<DSEServer> iter = serverState.values().iterator();
				while (iter.hasNext()) {
					DSEServer s = iter.next();
					Lock l = this.ssl.writeLock();
					l.lock();
					try {
						servers[s.id] = s;
						Lock _pl = this.pl.writeLock();
						_pl.lock();
						try {
							if (pools[s.id] == null) {
								pools[s.id] = s.createPool();
							} else {
								SDFSLogger.getLog().debug(
										" pool for " + s.id + " is "
												+ pools[s.id]);
							}
						} finally {
							_pl.unlock();
						}

					} finally {
						l.unlock();
					}
				}
				Lock l = this.sl.writeLock();
				l.lock();
				try {
					sal.clear();
					iter = serverState.values().iterator();
					while (iter.hasNext()) {
						DSEServer s = iter.next();
						if (s.serverType == DSEServer.SERVER) {
							sal.add(s);
							saal.add(s.address);
						}
					}
					Collections.sort(sal, new CustomComparator());
				} finally {
					l.unlock();
				}

				l = this.nl.writeLock();
				l.lock();
				try {
					nal.clear();
					iter = serverState.values().iterator();
					while (iter.hasNext()) {
						DSEServer s = iter.next();
						if (s.serverType == DSEServer.CLIENT) {
							nal.add(s);
							synchronized (volumes) {
								if (s.volumeName != null)
									volumes.put(s.volumeName, s.address);
							}
						}
					}
				} finally {
					l.unlock();
				}
				if (sal.size() < Main.volume.getClusterCopies())
					SDFSLogger
							.getLog()
							.warn("Will not be able to fulfill block redundancy requirements. Current number of DSE Servers is less than "
									+ Main.volume.getClusterCopies());
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

	@Override
	public void run() {
		while (!closed) {
			try {
				server.address = channel.getAddress();
				server.volumeName = Main.volume.getName();
				server.serverType = DSEServer.CLIENT;
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

	public void addSelfToState() throws IOException {
		synchronized (serverState) {
			serverState.put(server.address, server);
			if (server.serverType == DSEServer.SERVER) {
				Lock l = this.ssl.writeLock();
				l.lock();
				try {

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
				} finally {
					l.unlock();
				}
				l = this.sl.writeLock();
				l.lock();
				try {
					sal.remove(server);
					sal.add(server);
					saal.remove(server.address);
					saal.add(server.address);
					Collections.sort(sal, new CustomComparator());
				} finally {
					l.unlock();
				}
				l = this.pl.writeLock();
				l.lock();
				try {
					if (pools[server.id] == null) {
						SDFSLogger.getLog().debug(
								"creating pool for " + server.id);
						pools[server.id] = server.createPool();
					}
				} finally {
					l.unlock();
				}

			} else if (server.serverType == DSEServer.CLIENT) {
				Lock l = this.nl.writeLock();
				l.lock();
				try {
					nal.remove(server);
					nal.add(server);
				} finally {
					l.unlock();
				}
				synchronized (volumes) {
					if (server.volumeName != null)
						volumes.put(server.volumeName, server.address);
				}
			}
		}
	}

	@Override
	public List<DSEServer> getStorageNodes() {
		ArrayList<DSEServer> sn = null;
		Lock l = this.sl.writeLock();
		l.lock();
		try {
			@SuppressWarnings("unchecked")
			ArrayList<DSEServer> clone = (ArrayList<DSEServer>) sal.clone();
			sn = clone;
		} finally {
			l.unlock();
		}
		return sn;
	}

	@Override
	public List<DSEServer> getNameNodes() {
		ArrayList<DSEServer> sn = null;
		Lock l = this.nl.writeLock();
		l.lock();
		try {
			@SuppressWarnings("unchecked")
			ArrayList<DSEServer> clone = (ArrayList<DSEServer>) nal.clone();
			sn = clone;
		} finally {
			l.unlock();
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
