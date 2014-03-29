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
import org.opendedup.sdfs.cluster.cmds.SetGCScheduleCmd;
import org.opendedup.sdfs.cluster.cmds.StopGCMasterCmd;
import org.opendedup.sdfs.filestore.gc.StandAloneGCScheduler;
import org.opendedup.sdfs.io.Volume;
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
	final HashMap<String, Volume> volumes = new HashMap<String, Volume>();
	boolean closed = false;
	private final String config;
	private final String clusterID;
	LockService lock_service;
	private boolean peermaster = false;
	StandAloneGCScheduler gcscheduler = null;
	public final ReentrantLock gcUpdateLock = new ReentrantLock();
	public WeightedRandomServer wr = null;

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
		channel.connect(clusterID);
		/*
		
		*/
		server = new DSEServer(channel.getAddressAsString(), (byte) 0,
				DSEServer.CLIENT);
		server.address = channel.getAddress();
		server.volume = Main.volume;
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
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("finding all volumes");
		try {

			for (String vol : remoteVolumes) {
				if (vol != null) {
					synchronized (volumes) {
						volumes.put(vol, null);
					}
					new AddVolCmd(vol).executeCmd(this);
				}
			}
			ListVolsCmd cmd = new ListVolsCmd();
			cmd.executeCmd(this);
			HashMap<String, Volume> m = cmd.getResults();
			Set<String> vols = m.keySet();
			for (String vol : vols) {
				synchronized (volumes) {
					volumes.put(vol, m.get(vol));
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().warn("unable to list volumes", e);
		}
		if (SDFSLogger.isDebug())
			SDFSLogger.getLog().debug("found [" + volumes.size() + "] volumes");

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
		String st = "";
		for (int i = start; i < slist.length; i++) {
			st = st + " " + slist[i];
		}
		throw new IOException(
				"no pools available to fulfill request. Requested pools [" + st
						+ "]");
	}

	public List<HashClientPool> getServerPools(byte max, byte[] ignoredHosts) {
		ArrayList<HashClientPool> al = new ArrayList<HashClientPool>();
		ReadLock l = this.sl.readLock();
		l.lock();
		try {
			List<DSEServer> ss = this.wr.getServers(max, ignoredHosts);
			for (DSEServer s : ss) {
				al.add(pools[s.id]);
			}
			/*
			 * if (Main.volume.isClusterRackAware() && sal.size() > 1) {
			 * HashSet<String> usedRacks = new HashSet<String>(); if (sal.size()
			 * < max) { for (DSEServer s : sal) { if (!ignoreHost(s,
			 * ignoredHosts) && !usedRacks.contains(s.rack)) {
			 * usedRacks.add(s.rack); if(!pools[s.id].isSuspect())
			 * al.add(pools[s.id]); } } } else { for (int i = 0; i < max; i++) {
			 * DSEServer s = sal.get(i); if (!ignoreHost(s, ignoredHosts) &&
			 * !usedRacks.contains(s.rack)) { usedRacks.add(s.rack);
			 * if(!pools[s.id].isSuspect()) al.add(pools[s.id]); } } } } if
			 * (al.size() < max) { if (sal.size() < max) { for (DSEServer s :
			 * sal) { if (!ignoreHost(s, ignoredHosts)) al.add(pools[s.id]); } }
			 * else { for (int i = 0; i < max; i++) { DSEServer s = sal.get(i);
			 * if (!ignoreHost(s, ignoredHosts)) al.add(pools[s.id]); } } }
			 */
		} finally {
			l.unlock();
		}
		return al;
	}

	public void close() {
		this.closed = true;
		channel.close();
		disp.stop();
	}

	private void setServerWeighting() {
		this.wr = new WeightedRandomServer();
		this.wr.init(sal);
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
					Lock sl = this.ssl.writeLock();
					sl.lock();
					try {
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
					} finally {
						sl.unlock();
					}
					if (s.serverType == DSEServer.SERVER) {
						WriteLock l = this.sl.writeLock();
						l.lock();
						sal.remove(s);
						sal.add(s);
						Main.volume.setOffLine(sal.size() == 0);
						saal.remove(s.address);
						saal.add(s.address);
						Collections.sort(sal, new CustomComparator());
						setServerWeighting();
						l.unlock();
						l = this.pl.writeLock();
						l.lock();
						if (pools[s.id] == null && Main.DSEClusterDirectIO) {
							if (SDFSLogger.isDebug())
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
			rtrn = Boolean.valueOf(true);
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
			if (volume.equals(server.volume.getName()))
				throw new IOException("Volume is mounted by " + server.address);
			this.volumes.remove(volume);
			rtrn = Boolean.valueOf(true);
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
			rtrn = Boolean.valueOf(true);
			break;
		}

		case NetworkCMDS.RUN_CLAIM: {
			rtrn = null;
			break;
		}
		case NetworkCMDS.RUN_REMOVE: {
			rtrn = null;
			break;
		}
		case NetworkCMDS.HASH_EXISTS_CMD: {
			rtrn = Boolean.valueOf(false);
			break;
		}
		case NetworkCMDS.FIND_GC_MASTER_CMD: {
			this.gcUpdateLock.lock();
			try {
				rtrn = Boolean.valueOf(this.gcscheduler != null);
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
					rtrn = Boolean.valueOf(true);
				} else
					rtrn = Boolean.valueOf(false);
			} finally {
				this.gcUpdateLock.unlock();
			}
			break;
		}
		case NetworkCMDS.FIND_VOLUME_OWNER: {
			if (server != null && server.volume != null) {
				byte[] sb = new byte[buf.getInt()];
				buf.get(sb);
				String volume = new String(sb);
				if (volume.equals(server.volume.getName()))
					rtrn = Main.volume;
				else
					rtrn = null;
			}
			break;
		}
		case NetworkCMDS.SET_GC_SCHEDULE: {
			SDFSLogger.getLog().info("setting schedule");
			byte[] sb = new byte[buf.getInt()];
			buf.get(sb);
			String schedule = new String(sb);
			SDFSLogger.getLog().info("setting gc schedule to " + schedule);
			this._changeGCSchedule(schedule);
			rtrn = schedule;
			break;
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
		ReadLock l = this.sl.readLock();
		l.lock();
		try {
			return this.wr.getAddresses(max, ignoredHosts);
		} finally {
			l.unlock();
		}
		/*
		 * ArrayList<Address> al = new ArrayList<Address>(); ReadLock l =
		 * this.sl.readLock(); l.lock(); if (sal.size() < max) { for (DSEServer
		 * s : sal) { if (!ignoreHost(s, ignoredHosts)) al.add(s.address); } }
		 * else { for (int i = 0; i < max; i++) { DSEServer s = sal.get(i); if
		 * (!ignoreHost(s, ignoredHosts)) al.add(s.address); } } l.unlock();
		 * return al;
		 */
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
		String servers = "";
		for (byte b : slist) {
			servers = servers + " " + b;
		}
		throw new IOException(
				"no servers available to fulfill request. Requested servers are ["
						+ servers + "] requested start position was [" + start
						+ "]");
	}

	public List<Address> getServer(byte[] slist) throws IOException {
		ArrayList<Address> al = new ArrayList<Address>();
		ReadLock l = this.ssl.readLock();
		l.lock();
		try {
			for (int i = 1; i < slist.length; i++) {
				if (slist[i] > 0) {
					DSEServer svr = servers[slist[i]];
					if (svr != null)
						al.add(svr.address);
				}
			}
		} finally {
			l.unlock();
		}
		if (al.size() == 0)
			throw new IOException("no servers available to fulfill request");
		else
			return al;
	}

	@Override
	public void block() {
		// TODO Auto-generated method stub

	}

	@Override
	public void unblock() {
		// TODO Auto-generated method stub

	}

	public void startGC() throws IOException {
		Lock l = this.lock_service.getLock("gc");
		try {
			l.lock();
			StopGCMasterCmd m = new StopGCMasterCmd();
			m.executeCmd(this);
			SDFSLogger.getLog().info("Stopped GC Master on " + m.getResults());
			this._startGC();
			SDFSLogger.getLog().info("Started GC");
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to start gc", e);
		} finally {
			l.unlock();
		}

	}

	public void startGCIfNone() {
		if (this.lock_service != null) {
			Lock l = this.lock_service.getLock("gc");
			try {
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("Cheching if GC Master exists");
				l.lock();
				FindGCMasterCmd f = new FindGCMasterCmd();
				f.executeCmd(this);
				if (f.getResults() == null) {
					SetGCScheduleCmd cmd = new SetGCScheduleCmd(
							Main.fDkiskSchedule);
					cmd.executeCmd(this);
					this._startGC();
					SDFSLogger.getLog().info("Started GC");
				} else {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug(
								"Did not start GC because already exists at "
										+ f.getResults());
				}

			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to start gc", e);
			} finally {
				l.unlock();
			}
		}
	}

	private void _changeGCSchedule(String schedule)
			throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, IOException {
		Main.fDkiskSchedule = schedule;
		if (this.gcscheduler != null) {
			Lock l = this.lock_service.getLock("gc");
			l.lock();
			try {
				SDFSLogger.getLog().info("Restarting to GC Master");
				this.gcscheduler.close();
				this.gcscheduler = null;
				this.gcscheduler = new StandAloneGCScheduler();
				SDFSLogger.getLog().info("GC Master Restarted");
			} finally {
				l.unlock();
			}
		}
	}

	private void _startGC() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, IOException {
		this.gcUpdateLock.lock();
		try {

			ListVolsCmd cmd = new ListVolsCmd();
			cmd.executeCmd(this);
			HashMap<String, Volume> m = cmd.getResults();
			Set<String> vols = m.keySet();
			for (String vol : vols) {
				synchronized (volumes) {
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

	private void populateVolumeList() {
		try {
			ListVolsCmd cmd = new ListVolsCmd();
			cmd.executeCmd(this);
			HashMap<String, Volume> m = cmd.getResults();
			Set<String> vols = m.keySet();
			for (String vol : vols) {
				synchronized (volumes) {
					volumes.put(vol, m.get(vol));
				}
			}
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to populate volume list.", e);
		}
	}

	@Override
	public void viewAccepted(View new_view) {

		if (new_view instanceof MergeView) {
			lock_service.unlockAll();

		}
		Address first = new_view.getMembers().get(0);
		if (first.equals(this.channel.getAddress())) {
			this.peermaster = true;
		} else {
			this.peermaster = false;
		}
		this.startGCIfNone();
		this.populateVolumeList();
		SDFSLogger.getLog().info(
				"**client view: " + new_view + " peer master = "
						+ Boolean.toString(this.peermaster));
		synchronized (serverState) {
			Iterator<Address> iter = serverState.keySet().iterator();

			boolean ccp = false;
			if (sal.size() < Main.volume.getClusterCopies())
				ccp = true;

			while (iter.hasNext()) {
				Address addr = iter.next();
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug(
							"found " + addr + " "
									+ new_view.containsMember(addr));
				if (!new_view.containsMember(addr)) {
					if (SDFSLogger.isDebug())
						SDFSLogger.getLog().debug(
								"removed " + addr + " from state.");
					DSEServer s = serverState.remove(addr);
					Lock l = this.ssl.writeLock();
					l.lock();
					servers[s.id] = null;
					l.unlock();
					l = this.sl.writeLock();
					l.lock();
					sal.remove(s);
					Main.volume.setOffLine(sal.size() == 0);
					saal.remove(s.address);
					setServerWeighting();
					l.unlock();
					l = this.nl.writeLock();
					l.lock();
					nal.remove(s);
					l.unlock();
					synchronized (volumes) {
						if (s.volume != null)
							volumes.put(s.volume.getName(), null);
					}
					if (Main.DSEClusterDirectIO) {
						l = this.pl.writeLock();
						try {

							l.lock();
							pools[s.id].close();

						} catch (Exception e) {
							SDFSLogger.getLog().debug(
									"unable to shutdown pool", e);
						} finally {
							pools[s.id] = null;
							l.unlock();
						}
					}
				}
			}
			if (sal.size() < Main.volume.getClusterCopies())
				SDFSLogger
						.getLog()
						.warn("Will not be able to fulfill block redundancy requirements. Current number of DSE Servers is less than "
								+ Main.volume.getClusterCopies());
			else if (ccp)
				SDFSLogger
						.getLog()
						.info("Will now be able to fulfill block redundancy requirements. Current number of DSE Servers is ["
								+ sal.size()
								+ "] and cluster write requirement is "
								+ Main.volume.getClusterCopies());
		}
		SDFSLogger.getLog().info(
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
				try {
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
				} finally {
					l.unlock();
				}
				if (s.serverType == DSEServer.SERVER) {
					l = this.sl.writeLock();
					l.lock();
					sal.remove(s);
					sal.add(s);
					Main.volume.setOffLine(sal.size() == 0);
					saal.remove(s.address);
					saal.add(s.address);
					Collections.sort(sal, new CustomComparator());
					setServerWeighting();
					l.unlock();
					l = this.pl.writeLock();
					l.lock();
					if (pools[s.id] == null && Main.DSEClusterDirectIO) {
						if (SDFSLogger.isDebug())
							SDFSLogger.getLog().debug(
									"creating pool for " + s.id);

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
						if (s.volume != null) {
							s.volume.host = s.address;
							volumes.put(s.volume.getName(), s.volume);
						}
					}
				}
			}
			SDFSLogger.getLog()
					.info("in receive  " + server + " - size : "
							+ serverState.size());
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
			boolean ccp = false;
			synchronized (serverState) {
				if (sal.size() < Main.volume.getClusterCopies())
					ccp = true;
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
							if (pools[s.id] == null && Main.DSEClusterDirectIO) {
								pools[s.id] = s.createPool();
							} else {
								if (SDFSLogger.isDebug())
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
					setServerWeighting();
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
								if (s.volume != null) {
									s.volume.host = s.address;
									volumes.put(s.volume.getName(), s.volume);
								}
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
				else if (ccp)
					SDFSLogger
							.getLog()
							.info("Will now be able to fulfill block redundancy requirements. Current number of DSE Servers is ["
									+ sal.size()
									+ "] and cluster write requirement is "
									+ Main.volume.getClusterCopies());
				Main.volume.setOffLine(sal.size() == 0);

			}
			if (SDFSLogger.isDebug())
				SDFSLogger.getLog().debug(
						"received state (" + list.size() + " state");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public long getMaxSize() {
		Lock l = this.sl.readLock();
		l.lock();
		try {
			long sz = 0;
			for (DSEServer s : sal) {
				sz = sz + s.maxSize;
				if (SDFSLogger.isDebug())
					SDFSLogger.getLog().debug("sz=" + sz);
			}
			return sz;
		} finally {
			l.unlock();
		}
	}

	public long getCurrentSize() {
		Lock l = this.sl.readLock();
		l.lock();
		try {
			long sz = 0;
			for (DSEServer s : sal) {
				sz = sz + s.currentSize;
			}
			return sz;
		} finally {
			l.unlock();
		}
	}

	public long getCurrentDSESize() {
		Lock l = this.sl.readLock();
		l.lock();
		try {
			long sz = 0;
			for (DSEServer s : sal) {
				sz = sz + s.dseSize;
			}
			return sz;
		} finally {
			l.unlock();
		}
	}

	public long getCurrentDSECompSize() {
		Lock l = this.sl.readLock();
		l.lock();
		try {
			long sz = 0;
			for (DSEServer s : sal) {
				sz = sz + s.dseCompressedSize;
			}
			return sz;
		} finally {
			l.unlock();
		}
	}

	public long getDSEMaxSize() {
		Lock l = this.sl.readLock();
		l.lock();
		try {
			long sz = 0;
			for (DSEServer s : sal) {
				sz = sz + s.dseMaxSize;
			}
			return sz;
		} finally {
			l.unlock();
		}
	}

	public long getFreeBlocks() {
		Lock l = this.sl.readLock();
		l.lock();
		try {
			long sz = 0;
			for (DSEServer s : sal) {
				sz = sz + s.freeBlocks;
			}
			return sz;
		} finally {
			l.unlock();
		}
	}

	private class CustomComparator implements Comparator<DSEServer> {
		@Override
		public int compare(DSEServer o1, DSEServer o2) {
			long fs1 = o1.maxSize - o1.currentSize;
			long fs2 = o2.maxSize - o2.currentSize;
			if (fs1 > fs2)
				return -1;
			else if (fs1 < fs2)
				return 1;
			else
				return 0;
		}
	}

	@Override
	public void run() {
		while (!closed) {
			try {
				server.address = channel.getAddress();
				server.volume = Main.volume;
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
					Main.volume.setOffLine(sal.size() == 0);
					setServerWeighting();
				} finally {
					l.unlock();
				}
				l = this.pl.writeLock();
				l.lock();
				try {
					if (pools[server.id] == null) {
						if (SDFSLogger.isDebug())
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
					if (server.volume != null) {
						server.volume.host = server.address;
						volumes.put(server.volume.getName(), server.volume);
					}
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
				addr = volumes.get(volumeName).host;
		}
		return addr;
	}

	@Override
	public DSEServer getServer() {
		return this.server;
	}

}
