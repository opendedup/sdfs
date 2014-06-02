package org.opendedup.collections;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class LongKeyValue implements Externalizable {
	private byte[] value;
	private long key;

	public LongKeyValue(long key, byte[] value) {
		this.key = key;
		this.value = value;
	}

	public LongKeyValue() {

	}

	@Override
	public void readExternal(ObjectInput out) throws IOException,
			ClassNotFoundException {
		this.key = out.readLong();
		this.value = new byte[out.readInt()];
		out.readFully(value);
	}

	@Override
	public void writeExternal(ObjectOutput in) throws IOException {
		in.writeLong(this.key);
		in.writeInt(value.length);
		in.write(value);
	}

	public byte[] getValue() {
		return value;
	}

	public long getKey() {
		return key;
	}

}
