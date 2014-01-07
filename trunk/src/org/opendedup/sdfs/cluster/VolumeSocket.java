package org.opendedup.sdfs.cluster;

import java.io.DataInputStream;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MembershipListener;
import org.jgroups.MergeView;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.util.Util;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.io.BlockDev;
import org.opendedup.sdfs.io.Volume;

public class VolumeSocket implements RequestHandler, MembershipListener,
		MessageListener {
	private static final byte ADDDEV = 0;
	private static final byte RMDEV = 1;
	private static final byte SETDEVSZ = 2;
	private static final byte SETDEVAUTO = 4;

	private Volume vol;
	private String cfg;
	protected JChannel channel;

	private MessageDispatcher disp;
	private boolean peermaster = false;
	private Address pmAddr =null;

	public VolumeSocket(Volume vol, String config) throws Exception {
		SDFSLogger.getLog().info("Starting Volume Socket for " + vol.getName());
		this.vol = vol;
		this.cfg = config;
		channel = new JChannel(this.cfg);
		disp = new MessageDispatcher(channel, null, null, this);
		disp.setMembershipListener(this);
		disp.setMessageListener(this);
		channel.connect(this.vol.getName());
		channel.getState(null, 10000);
		SDFSLogger.getLog().info("Started Volume Socket for " + vol.getName());
	}

	@Override
	public void getState(OutputStream output) throws Exception {
		synchronized (vol) {
			try {
				Util.objectToStream(vol, new DataOutputStream(output));
			} catch (Exception e) {
				SDFSLogger.getLog().error("unable to get state", e);
			}
		}

	}

	@Override
	public void receive(Message arg0) {

	}

	@Override
	public void setState(InputStream input) throws Exception {
		synchronized (vol) {
			Volume vl = (Volume) Util.objectFromStream(new DataInputStream(
					input));
			List<BlockDev> devices = vl.devices;
			vol.devices.clear();
			for (BlockDev dev : devices) {
				vol.addBlockDev(dev);
			}
		}
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
			this.peermaster =true;
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
		byte[] arb;
		BlockDev dev;
		arb = new byte[buf.getInt()];
		buf.get(arb);
		dev = (BlockDev) Util.objectFromByteBuffer(arb);
		switch (cmd) {
		case ADDDEV:
			synchronized (vol) {
				vol.addBlockDev(dev);
			}
			break;
		case RMDEV:
			synchronized (vol) {
				vol.removeBlockDev(dev.getDevName());
			}
			break;
		case SETDEVAUTO:
			synchronized (vol) {
				BlockDev _dev = vol.getBlockDev(dev.getDevName());
				_dev.setStartOnInit(dev.isStartOnInit());
			}
			break;
		case SETDEVSZ:
			synchronized (vol) {
				BlockDev _dev = vol.getBlockDev(dev.getDevName());
				_dev.setSize(dev.getSize());
			}
			break;
			
		}
		return null;
	}

	public boolean isPeermaster() {
		return peermaster;
	}

	public Address getPmAddr() {
		return pmAddr;
	}

}
