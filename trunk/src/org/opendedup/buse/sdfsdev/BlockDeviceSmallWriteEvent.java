package org.opendedup.buse.sdfsdev;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.opendedup.sdfs.io.BlockDev;

public class BlockDeviceSmallWriteEvent implements Externalizable {
	public BlockDev dev;
	public ByteBuffer buf;
	public long pos;
	public int len;

	public BlockDeviceSmallWriteEvent() {

	}

	public BlockDeviceSmallWriteEvent(BlockDev dev, ByteBuffer buf, long pos,
			int len) {
		this.dev = dev;
		this.buf = buf;
		this.pos = pos;
		this.len = len;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		len = in.readInt();
		byte[] b = new byte[len];
		in.readFully(b);
		buf = ByteBuffer.wrap(b);
		pos = in.readLong();

	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		byte[] b = new byte[len];
		buf.position(0);
		buf.get(b);
		buf.position(0);
		out.writeInt(len);
		out.write(b);
		out.writeLong(pos);
	}

}
