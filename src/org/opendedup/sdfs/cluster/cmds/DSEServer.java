package org.opendedup.sdfs.cluster.cmds;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.jgroups.Address;
import org.jgroups.util.Util;
import org.opendedup.sdfs.network.NetworkCMDS;

public class DSEServer implements Externalizable {
	public boolean client;
	public byte id;
	public String hostName;
	public Address address;
	public long currentSize;
	public long maxSize;
	public long freeBlocks;
	public int pageSize;
	
	public DSEServer() {
		
	}
	
	public DSEServer(String hostName,byte id,boolean client) {
		this.client = client;
		this.id = id;
		this.hostName = hostName;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		in.readByte();
		int strl = in.readInt();
		byte [] b = new byte[strl];
		in.read(b);
		this.hostName = new String(b);
		this.id = in.readByte();
		byte ic = in.readByte();
		if(ic == 1)
			this.client = true;
		this.address = (Address)in.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		byte [] b = hostName.getBytes();
		out.writeByte(NetworkCMDS.UPDATE_DSE);
		out.writeInt(b.length);
		out.write(b);
		out.writeByte(id);
		if(client)
			out.writeByte(1);
		else
			out.writeByte(0);
		out.writeObject(address);
		
	}
	
	public byte [] getBytes() throws Exception {
		byte [] b = hostName.getBytes();
		byte [] addr = Util.objectToByteBuffer(address);
		byte [] bz = new byte [1+ 4+ b.length + 1 + 1+ 4+ addr.length + 8+8+8+4];
		
		ByteBuffer buf = ByteBuffer.wrap(bz);
		buf.put(NetworkCMDS.UPDATE_DSE);
		buf.putInt(b.length);
		buf.put(b);
		buf.put(id);
		if(client)
			buf.put((byte)1);
		else
			buf.put((byte)0);
		buf.putInt(addr.length);
		buf.put(addr);
		buf.putLong(maxSize);
		buf.putLong(currentSize);
		buf.putLong(freeBlocks);
		buf.putInt(pageSize);
		return buf.array();
	}
	
	public void fromByte(byte [] bz) throws Exception {
		ByteBuffer buf = ByteBuffer.wrap(bz);
		buf.get();
		byte [] bs = new byte[buf.getInt()];
		buf.get(bs);
		this.hostName = new String(bs);
		this.id = buf.get();
		byte ic = buf.get();
		if(ic == 1)
			this.client = true;
		byte [] addr = new byte [buf.getInt()];
		buf.get(addr);
		this.address = (Address)Util.objectFromByteBuffer(addr);
		this.maxSize = buf.getLong();
		this.currentSize = buf.getLong();
		this.freeBlocks = buf.getLong();
		this.pageSize = buf.getInt();
	}
	
	public String toString() {
		return this.hostName + " id=" + this.id + " client=" + this.client + " address=[" + this.address + "] maxsz="+this.maxSize + " currentsize=" +this.currentSize + " freeblocks=" + this.freeBlocks;
	}
	
	public int hashCode() {
		return this.id;
	}
	
	public boolean equals(Object obj) {
		DSEServer s = (DSEServer)obj;
		return (s.id == this.id);
	}

}
