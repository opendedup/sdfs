package org.opendedup.sdfs.mgmt;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.sdfs.io.DedupFileChannel;

public class GetDataCmd {

	public byte[] getResult(long fd, long start,int sz) throws IOException,
			ClassNotFoundException, DataArchivedException {
		byte [] b = new byte[sz];
		DedupFileChannel ch = OpenFile.OpenChannels.get(fd);
		ByteBuffer bf = ByteBuffer.wrap(b);
		ch.read(bf, 0, sz, start);
		return b;
		
	}

}
