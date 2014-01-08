package org.opendedup.sdfs.cluster;

import java.io.DataInputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jgroups.Address;
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
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.CENTRAL_LOCK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.opendedup.buse.sdfsdev.BlockDeviceSmallWriteEvent;
import org.opendedup.collections.DataMapInterface;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.collections.LongKeyValue;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.BlockDev;

import com.google.common.eventbus.Subscribe;

public class BlockDevSocket implements RequestHandler, MembershipListener,
		MessageListener, DataMapInterface, Runnable {
	private static final byte PUT = 0;
	private static final byte GET = 1;
	private static final byte TRIM = 2;
	private static final byte SYNC = 3;
	private static final byte TRUNC = 4;
	private static final byte REMOVE = 5;
	private static final byte ITERINIT = 6;
	private static final byte NXTKEY = 7;
	private static final byte NXTVAL = 8;
	private static final byte SZ = 9;
	private static final byte GTMASTER = 10;
	private static final byte SMWRITE = 11;
	private VolumeSocket vs;
	BlockDev dev;

	private ForkChannel channel;

	private MessageDispatcher disp;
	private boolean peermaster = false;
	private Address pmAddr = null;
	private LongByteArrayMap map;
	private LockService lock_service;
	private ArrayList<LongKeyValue> cmap = new ArrayList<LongKeyValue>();
	private int mxSz = 400;
	private final ReentrantLock flushlock = new ReentrantLock();
	private boolean started = false;
	Lock rsyncLock = null;
	Lock smWriteLock = null;
	private Thread ft = null;

	public BlockDevSocket(BlockDev dev, String path) throws IOException {
		SDFSLogger.getLog().info(
				"Starting block device map for " + dev.getDevName());
		map = new LongByteArrayMap(path);
		if (Main.volume.isClustered()) {
			this.vs = Main.volume.getSoc();
			this.dev = dev;
			try {
				channel = new ForkChannel(vs.channel,

				"dev-stack",

				dev.getDevName(),

				true,

				ProtocolStack.ABOVE,

				CENTRAL_LOCK.class);
				disp = new MessageDispatcher(channel, null, null, this);
				disp.setMembershipListener(this);
				disp.setMessageListener(this);
				channel.connect(dev.getDevName());
				lock_service = new LockService(channel);
				this.rsyncLock = this.lock_service.getLock("rsync");
				this.smWriteLock = this.lock_service.getLock("smwrite");
				this.pmAddr = this.getMaster();
				if (this.pmAddr == null
						|| this.pmAddr.equals(channel.getAddress())) {
					this.pmAddr = channel.getAddress();
					SDFSLogger.getLog().debug("First node in cluster");
				} else {
					SDFSLogger
							.getLog()
							.info("Not first node in cluster, clearing and resyncing");
					this.map.vanish();
					this.map = new LongByteArrayMap(path);
					this.resync();
					SDFSLogger.getLog().info("Done Resync'ing");
				}
				this.started = true;
				this.ft = new Thread(this);
				this.ft.start();
			} catch (Throwable e) {
				throw new IOException(e);
			}

		}

	}

	@Subscribe
	public void smallWriteEvent(BlockDeviceSmallWriteEvent evt) {
		if (Main.volume.isClustered()) {

			this.smWriteLock.lock();
			try {
				RequestOptions opts = null;
				opts = new RequestOptions(ResponseMode.GET_NONE,
						Main.ClusterRSPTimeout, false);
				opts.setFlags(Message.Flag.DONT_BUNDLE);
				byte[] data = Util.objectToByteBuffer(evt);
				byte[] b = new byte[1 + 4 + data.length];

				// dev.getMF().setLastModified(System.currentTimeMillis());
				ByteBuffer buf = ByteBuffer.wrap(b);
				buf.put(SMWRITE);
				buf.putInt(data.length);
				buf.put(data);
				disp.castMessage(null, new Message(null, null, buf.array()),
						opts);
			} catch (Exception e) {
				SDFSLogger.getLog().warn("unable to do small write", e);
			} finally {
				this.smWriteLock.unlock();
			}
		}
	}

	private void resync() throws IOException {
		if (this.peermaster) {
			SDFSLogger.getLog().info(
					"Returning because trying to resync with self");
			return;
		}

		SDFSLogger.getLog()
				.info("Resyncing [" + this.dev.getDevName() + "] with "
						+ this.pmAddr);
		rsyncLock.lock();
		try {
			this.iterInit(this.pmAddr);
			List<LongKeyValue> kvs = this.nextValues(pmAddr);
			boolean done = false;
			Address prevm = this.pmAddr;
			while (!done) {
				for (LongKeyValue kv : kvs) {
					if (kv == null)
						done = true;
					else {
						map.putIfNull(kv.getKey(), kv.getValue());
					}
				}
				if (!done && prevm.equals(this.pmAddr)) {
					kvs = this.nextValues(this.pmAddr);
					prevm = this.pmAddr;
				} else {
					SDFSLogger.getLog().info(
							"eeks " + prevm.toString() + " - "
									+ this.pmAddr.toString());
				}
			}
		} finally {
			rsyncLock.unlock();
		}
	}

	@Override
	public void getState(OutputStream output) throws Exception {
		try {
			Util.objectToStream(pmAddr, new DataOutputStream(output));
		} catch (Exception e) {
			SDFSLogger.getLog().error("unable to get state", e);
		}
	}

	@Override
	public void receive(Message arg0) {

	}

	@Override
	public void setState(InputStream input) throws Exception {
		this.pmAddr = (Address) Util
				.objectFromStream(new DataInputStream(input));
	}

	@Override
	public void block() {
		// TODO Auto-generated method stub

	}

	@Override
	public void suspect(Address arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unblock() {
		// TODO Auto-generated method stub

	}

	@Override
	public void viewAccepted(View new_view) {
		SDFSLogger.getLog().info("View Changed");
		if (new_view instanceof MergeView) {
			// TODO add split brain algo
			SDFSLogger.getLog().info("split brain suspected!!!");
		}
		this.pmAddr = new_view.getMembers().get(0);
		SDFSLogger.getLog().info("Mater is " + this.pmAddr);
		if (pmAddr.equals(this.channel.getAddress())) {
			this.peermaster = true;
		} else {
			this.peermaster = false;
		}
		for (Address addr : new_view.getMembers()) {
			SDFSLogger.getLog().info(addr.toString());
		}

	}

	@Override
	public Object handle(Message msg) throws Exception {
		try {
			byte[] buffer = msg.getBuffer();
			ByteBuffer buf = ByteBuffer.wrap(buffer);
			buf.position(msg.getOffset());
			byte cmd = buf.get();
			Object rtrn = null;
			switch (cmd) {
			case GET:
				rtrn = map.get(buf.getLong());
				break;
			case PUT:
				if (!this.channel.getAddress().equals(msg.getSrc())) {
					byte[] ck = new byte[buf.getInt()];
					buf.get(ck);
					@SuppressWarnings("unchecked")
					List<LongKeyValue> lst = (List<LongKeyValue>) Util
							.objectFromByteBuffer(ck);
					for (LongKeyValue val : lst) {
						map.put(val.getKey(), val.getValue());
					}

					/*
					 * try { byte [] b= engine.getHash(ck);
					 * SDFSLogger.getLog().info("Received "
					 * +StringUtils.getHexString(b)+" for pos " + pos);
					 * }catch(Throwable e) { SDFSLogger.getLog().warn("eeks",e);
					 * }
					 */
				}
				break;
			case TRIM:
				map.trim(buf.getLong(), buf.getInt());
				break;
			case SYNC:
				map.sync();
				break;
			case TRUNC:
				map.truncate(buf.getLong());
				break;
			case REMOVE:
				map.remove(buf.getLong());
				break;
			case ITERINIT:
				SDFSLogger.getLog().info("interinit");
				map.iterInit();
				break;
			case NXTKEY:
				rtrn = map.nextKey();
				break;
			case NXTVAL:
				ArrayList<LongKeyValue> al = new ArrayList<LongKeyValue>();
				for (int i = 0; i < mxSz; i++) {
					LongKeyValue v = map.nextKeyValue();
					al.add(v);
					if (v == null)
						break;
				}
				rtrn = al;
				break;
			case SZ:
				rtrn = map.size();
				break;
			case GTMASTER:
				if (this.isPeermaster())
					rtrn = true;
				else
					rtrn = false;
				break;
			case SMWRITE:
				if (!this.channel.getAddress().equals(msg.getSrc())) {
					if (dev.getDevIO() != null) {
						byte[] ck = new byte[buf.getInt()];
						buf.get(ck);
						BlockDeviceSmallWriteEvent evt = (BlockDeviceSmallWriteEvent) Util
								.objectFromByteBuffer(ck);
						dev.getDevIO().ch.writeFile(evt.buf, evt.len, 0,
								evt.pos,false);
					}
				}
			}
			return rtrn;
		} catch (Exception e) {
			SDFSLogger.getLog().warn(
					"Exception in block dev handle for " + dev.getDevName(), e);
			throw e;
		}
	}

	public boolean isPeermaster() {
		return peermaster;
	}

	public Address getPmAddr() {
		return pmAddr;
	}

	@Override
	public void iterInit() throws IOException {
		map.iterInit();
	}

	public void iterInit(Address addr) throws IOException {
		RequestOptions opts = null;
		opts = new RequestOptions(ResponseMode.GET_ALL, Main.ClusterRSPTimeout,
				false);
		opts.setAnycasting(true);
		byte[] b = new byte[1];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(ITERINIT);
		try {
			List<Address> addrs = new ArrayList<Address>();
			addrs.add(addr);
			RspList<Object> lst = disp.castMessage(addrs, new Message(null,
					null, buf.array()), opts);
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException())
					throw rsp.getException();
			}
		} catch (Throwable e) {
			throw new IOException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public List<LongKeyValue> nextValues(Address addr) throws IOException {
		RequestOptions opts = null;
		opts = new RequestOptions(ResponseMode.GET_ALL, -1, false);
		opts.setFlags(Message.Flag.DONT_BUNDLE);
		opts.setAnycasting(true);
		byte[] b = new byte[1];
		ByteBuffer buf = ByteBuffer.wrap(b);
		buf.put(NXTVAL);
		try {
			List<Address> addrs = new ArrayList<Address>();
			addrs.add(addr);
			RspList<Object> lst = disp.castMessage(addrs, new Message(null,
					null, buf.array()), opts);
			for (Rsp<Object> rsp : lst) {
				if (rsp.hasException()) {
					SDFSLogger.getLog().warn(
							"exception while getting next values",
							rsp.getException());
					throw rsp.getException();
				} else
					return (List<LongKeyValue>) rsp.getValue();
			}
		} catch (Throwable e) {
			throw new IOException(e);
		}
		return null;
	}

	@Override
	public long getIterPos() {
		return map.getIterPos();
	}

	@Override
	public long nextKey() throws IOException {
		return map.nextKey();
	}

	@Override
	public byte[] nextValue() throws IOException {
		return map.nextValue();
	}

	@Override
	public boolean isClosed() {
		return map.isClosed();
	}

	public Address getMaster() throws IOException {
		if (channel != null) {
			RequestOptions opts = null;
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout, false);
			byte[] b = new byte[1];
			ByteBuffer buf = ByteBuffer.wrap(b);
			buf.put(GTMASTER);
			try {
				RspList<Object> lst = disp.castMessage(null, new Message(null,
						null, buf.array()), opts);
				for (Rsp<Object> rsp : lst) {
					if (rsp.wasReceived()) {
						try {
							Boolean val = (Boolean) rsp.getValue();
							if (val == true) {
								return rsp.getSender();
							}

						} catch (Exception e) {
							SDFSLogger.getLog().warn(
									"error while getting master", e);
						}
					}
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		return null;

	}

	private int flush() throws Exception {
		int sz = cmap.size();
		if (cmap.size() > 0) {
			RequestOptions opts = null;
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout, false);
			// opts.setFlags(Message.Flag.DONT_BUNDLE);
			byte[] data = Util.objectToByteBuffer(cmap);
			byte[] b = new byte[1 + 4 + data.length];

			// dev.getMF().setLastModified(System.currentTimeMillis());
			ByteBuffer buf = ByteBuffer.wrap(b);
			buf.put(PUT);
			buf.putInt(data.length);
			buf.put(data);
			/*
			 * try { byte [] hash = engine.getHash(data);
			 * SDFSLogger.getLog().info("putting data " +
			 * StringUtils.getHexString(hash) + " at " + pos); }catch(Throwable
			 * e) { SDFSLogger.getLog().warn("eeks",e); }
			 */
			try {
				disp.castMessage(null, new Message(null, null, buf.array()),
						opts);
			} catch (Exception e) {
				throw new IOException(e);
			}

			cmap = null;
			cmap = new ArrayList<LongKeyValue>();
		}
		return sz;
	}

	@Override
	public void put(long pos, byte[] data) throws IOException {
		try {
			map.put(pos, data);
			if (channel != null) {
				this.flushlock.lock();
				try {
					this.cmap.add(new LongKeyValue(pos, data));
					if (this.cmap.size() > this.mxSz)
						this.flush();
				} finally {
					this.flushlock.unlock();
				}

			} else {

			}

		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public void putIfNull(long pos, byte[] data) throws IOException {
		this.putIfNull(pos, data);

	}

	@Override
	public void trim(long pos, int len) throws IOException {

		if (channel != null) {
			RequestOptions opts = null;
			opts = new RequestOptions(ResponseMode.GET_NONE,
					Main.ClusterRSPTimeout, false);
			byte[] b = new byte[1 + 8 + 4];
			ByteBuffer buf = ByteBuffer.wrap(b);
			buf.put(TRIM);
			buf.putLong(pos);
			buf.putInt(len);
			try {
				disp.castMessage(null, new Message(null, null, buf.array()),
						opts);
			} catch (Exception e) {
				throw new IOException(e);
			}
		} else {
			map.trim(pos, len);
		}

	}

	@Override
	public void truncate(long length) throws IOException {

		if (channel != null) {
			RequestOptions opts = null;
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout, false);
			byte[] b = new byte[1 + 8];
			ByteBuffer buf = ByteBuffer.wrap(b);
			buf.put(TRUNC);
			buf.putLong(length);
			try {
				disp.castMessage(null, new Message(null, null, buf.array()),
						opts);
			} catch (Exception e) {
				throw new IOException(e);
			}
		} else {
			map.truncate(length);
		}

	}

	@Override
	public byte getVersion() {
		return map.getVersion();
	}

	@Override
	public byte[] getFree() {
		return map.getFree();
	}

	@Override
	public void remove(long pos) throws IOException {

		if (channel != null) {
			RequestOptions opts = null;
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout, false);
			byte[] b = new byte[1 + 8];
			ByteBuffer buf = ByteBuffer.wrap(b);
			buf.put(REMOVE);
			buf.putLong(pos);
			try {
				disp.castMessage(null, new Message(null, null, buf.array()),
						opts);
			} catch (Exception e) {
				throw new IOException(e);
			}
		} else {
			map.remove(pos);
		}

	}

	@Override
	public byte[] get(long pos) throws IOException {
		return map.get(pos);
	}

	@Override
	public void sync() throws IOException {
		if (channel != null) {
			this.flushlock.lock();
			try {
				this.flush();
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				this.flushlock.unlock();
			}
			/*
			 * RequestOptions opts = null; opts = new
			 * RequestOptions(ResponseMode.GET_ALL, Main.ClusterRSPTimeout,
			 * false); byte[] b = new byte[1]; ByteBuffer buf =
			 * ByteBuffer.wrap(b); buf.put(SYNC); try { disp.castMessage(null,
			 * new Message(null, null, buf.array()), opts); } catch (Exception
			 * e) { throw new IOException(e); }
			 */
		} else {
			map.sync();
		}

	}

	@Override
	public void vanish() throws IOException {
		map.vanish();

	}

	@Override
	public void copy(String destFilePath) throws IOException {
		map.copy(destFilePath);

	}

	@Override
	public long size() {
		return map.size();
	}

	@Override
	public void close() {
		this.flushlock.lock();

		try {
			this.started = false;
			try {
				ft.interrupt();
			} catch (Exception e) {
			}
			try {
				this.flush();
			} catch (Exception e) {

			}
			if (channel != null) {
				channel.close();
				channel = null;
				this.pmAddr = null;
				this.peermaster = false;
			}

			map.close();
			map = null;
		} finally {
			this.flushlock.unlock();
		}

	}

	@Override
	public void run() {
		while (started) {
			try {
				this.flushlock.lock();
				this.flush();
			} catch (Exception e) {

			} finally {
				this.flushlock.unlock();
			}
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				break;
			}
		}

	}

}
