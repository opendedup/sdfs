package org.opendedup.sdfs.cluster;

import java.io.DataInputStream;


import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

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
import org.jgroups.fork.ForkChannel;
import org.jgroups.protocols.FRAG2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.opendedup.collections.DataMapInterface;
import org.opendedup.collections.LongByteArrayMap;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.io.BlockDev;

public class BlockDevSocket implements RequestHandler, MembershipListener,
		MessageListener, DataMapInterface {
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
	private VolumeSocket vs;
	BlockDev dev;

	private ForkChannel channel;

	private MessageDispatcher disp;
	private boolean peermaster = false;
	private Address pmAddr = null;
	private LongByteArrayMap map;

	public BlockDevSocket(BlockDev dev, String path) throws IOException {
		if (Main.volume.isClustered()) {
			this.vs = Main.volume.getSoc();
			this.dev = dev;
			try {
			channel = new ForkChannel(vs.channel,

			"dev-stack",

			dev.getDevName(),

			true,

			ProtocolStack.ABOVE,

			FRAG2.class);
			disp = new MessageDispatcher(channel, null, null, this);
			disp.setMembershipListener(this);
			disp.setMessageListener(this);
			channel.connect(dev.getDevName(), null, 10000);
			if (this.pmAddr == null) {
				this.pmAddr = channel.getAddress();
			}
			}catch(Throwable e ) {
				throw new IOException(e);
			}

		}
		map = new LongByteArrayMap(path);
	}

	@Override
	public void getState(OutputStream output) throws Exception {
		Util.objectToStream(pmAddr, new DataOutputStream(output));
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
		if (new_view instanceof MergeView) {
			// TODO add split brain algo
			SDFSLogger.getLog().info("split brain suspected!!!");
		}
		this.pmAddr = new_view.getMembers().get(0);
		if (pmAddr.equals(this.channel.getAddress())) {
			this.peermaster = true;
		} else {
			this.peermaster = false;
		}

	}

	@Override
	public Object handle(Message msg) throws Exception {
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
			long pos = buf.getLong();
			byte[] ck = new byte[map.getFree().length];
			buf.get(ck);
			map.put(pos, ck);
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
			map.iterInit();
			break;
		case NXTKEY:
			rtrn = map.nextKey();
			break;
		case NXTVAL:
			rtrn = map.nextValue();
			break;
		case SZ:
			rtrn = map.size();
			break;
		}
		return rtrn;
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

	@Override
	public void put(long pos, byte[] data) throws IOException {
		map.put(pos, data);
		if (channel != null) {
			RequestOptions opts = null;
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout, false);
			byte[] b = new byte[1 + 8 + data.length];
			ByteBuffer buf = ByteBuffer.wrap(b);
			buf.put(PUT);
			buf.putLong(pos);
			buf.put(data);
			try {
				disp.castMessage(null, new Message(null, null, buf.array()),
						opts);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

	}

	@Override
	public void putIfNull(long pos, byte[] data) throws IOException {
		this.putIfNull(pos, data);

	}

	@Override
	public void trim(long pos, int len) throws IOException {
		map.trim(pos, len);
		if (channel != null) {
			RequestOptions opts = null;
			opts = new RequestOptions(ResponseMode.GET_ALL,
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
		}

	}

	@Override
	public void truncate(long length) throws IOException {
		map.truncate(length);
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
		map.remove(pos);
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
		}

	}

	@Override
	public byte[] get(long pos) throws IOException {
		return map.get(pos);
	}

	@Override
	public void sync() throws IOException {
		map.sync();
		if (channel != null) {
			RequestOptions opts = null;
			opts = new RequestOptions(ResponseMode.GET_ALL,
					Main.ClusterRSPTimeout, false);
			byte[] b = new byte[1];
			ByteBuffer buf = ByteBuffer.wrap(b);
			buf.put(SYNC);
			try {
				disp.castMessage(null, new Message(null, null, buf.array()),
						opts);
			} catch (Exception e) {
				throw new IOException(e);
			}
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
		if(channel != null) {
			channel.close();
			channel = null;
		}
		map.close();

	}

}
