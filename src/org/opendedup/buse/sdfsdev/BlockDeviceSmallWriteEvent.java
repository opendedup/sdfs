/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
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
