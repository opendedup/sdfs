package org.opendedup.collections;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

public final class ByteArrayWrapper implements Externalizable
{
    private byte[] data;
    public ByteArrayWrapper() {
    	
    }

    public ByteArrayWrapper(byte[] data)
    {
        if (data == null)
        {
            throw new NullPointerException();
        }
        this.data = data;
    }
    
    public byte [] getData() {
    	return this.data;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof ByteArrayWrapper))
        {
            return false;
        }
        return Arrays.equals(data, ((ByteArrayWrapper)other).data);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(data);
    }

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(data.length);
		out.write(data);
		
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		
		data = new byte[in.readInt()];
		in.readFully(data);
		
		
	}
}