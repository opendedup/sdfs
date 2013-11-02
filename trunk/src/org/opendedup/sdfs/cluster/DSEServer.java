package org.opendedup.sdfs.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.jgroups.Address;
import org.jgroups.util.Util;
import org.opendedup.sdfs.network.HashClientPool;
import org.opendedup.sdfs.network.NetworkCMDS;
import org.opendedup.sdfs.servers.HCServer;

public class DSEServer implements Externalizable {
	public int serverType;
	public byte id;
	public String hostName;
	public int dseport;
	public boolean useSSL;
	public Address address;
	public long currentSize;
	public long maxSize;
	public long freeBlocks;
	public int pageSize;
	public String location;
	public String rack;
	public String volumeName;
	public static final int SERVER = 0;
	public static final int CLIENT = 1;
	public static final int LISTENER = 2;
	
	public DSEServer() {
		
	}

	public DSEServer(String hostName, byte id, int serverType) {
		this.serverType = serverType;
		this.id = id;
		this.hostName = hostName;
	}

	public HashClientPool createPool() throws IOException {
		HCServer _server = new HCServer(this.hostName, this.dseport, false,
				false, this.useSSL);
		return new HashClientPool(_server,this.address.toString(),10,this.id);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		in.readByte();
		
		this.hostName = (String) in.readObject();
		this.id = in.readByte();
		this.serverType = in.readInt();
		this.address = (Address) in.readObject();
		this.maxSize = in.readLong();
		this.currentSize = in.readLong();
		this.freeBlocks = in.readLong();
		this.pageSize = in.readInt();
		this.dseport = in.readInt();
		this.useSSL = in.readBoolean();
		this.location = (String) in.readObject();
		this.rack = (String)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeByte(NetworkCMDS.UPDATE_DSE);
		out.writeObject(hostName);
		out.writeByte(id);
		out.writeInt(serverType);
		out.writeObject(address);
		out.writeLong(this.maxSize);
		out.writeLong(this.currentSize);
		out.writeLong(this.freeBlocks);
		out.writeInt(pageSize);
		out.writeInt(this.dseport);
		out.writeBoolean(useSSL);
		out.writeObject(location);
		out.writeObject(rack);

	}

	public byte[] getBytes() throws Exception {
		byte[] b = hostName.getBytes();
		byte[] addr = Util.objectToByteBuffer(address);
		byte [] lb = this.location.getBytes();
		byte [] rb = this.rack.getBytes();
		byte[] bz = new byte[1 + 4 + b.length + 1 + 4 + 4 + addr.length + 8 + 8
				+ 8 + 4 + 4 + 1 + 4+lb.length + 4+rb.length];

		ByteBuffer buf = ByteBuffer.wrap(bz);
		buf.put(NetworkCMDS.UPDATE_DSE);
		buf.putInt(b.length);
		buf.put(b);
		buf.put(id);
		buf.putInt(serverType);
		buf.putInt(addr.length);
		buf.put(addr);
		buf.putLong(maxSize);
		buf.putLong(currentSize);
		buf.putLong(freeBlocks);
		buf.putInt(pageSize);
		buf.putInt(this.dseport);
		if (this.useSSL)
			buf.put((byte) 1);
		else
			buf.put((byte) 0);
		
		buf.putInt(lb.length);
		buf.put(lb);
		buf.putInt(rb.length);
		buf.put(rb);
		return buf.array();
	}

	public void fromByte(byte[] bz) throws Exception {
		ByteBuffer buf = ByteBuffer.wrap(bz);
		buf.get();
		byte[] bs = new byte[buf.getInt()];
		buf.get(bs);
		this.hostName = new String(bs);
		this.id = buf.get();
		this.serverType = buf.getInt();
		byte[] addr = new byte[buf.getInt()];
		buf.get(addr);
		this.address = (Address) Util.objectFromByteBuffer(addr);
		this.maxSize = buf.getLong();
		this.currentSize = buf.getLong();
		this.freeBlocks = buf.getLong();
		this.pageSize = buf.getInt();
		this.dseport = buf.getInt();
		this.useSSL = false;
		if (buf.get() == 1)
			this.useSSL = true;
		byte[] lb = new byte[buf.getInt()];
		buf.get(lb);
		byte [] rb = new byte[buf.getInt()];
		buf.get(rb);
		this.location = new  String(lb);
		this.rack = new String(rb);
	}

	public String toString() {
		return this.hostName + " id=" + this.id + " serverType=" + this.serverType
				+ " address=[" + this.address + "] maxsz=" + this.maxSize
				+ " currentsize=" + this.currentSize + " freeblocks="
				+ this.freeBlocks + " dseport=" + this.dseport + " usessl=" + this.useSSL;
	}
	

	public int hashCode() {
		return this.id;
	}

	public boolean equals(Object obj) {
		DSEServer s = (DSEServer) obj;
		return (s.id == this.id);
	}

}
