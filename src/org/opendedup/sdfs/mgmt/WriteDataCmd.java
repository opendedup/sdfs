package org.opendedup.sdfs.mgmt;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.ReadOnlyException;

public class WriteDataCmd {

	public int getResult(long fd, long start,int sz,byte [] data) throws IOException,
			ClassNotFoundException, DataArchivedException, ReadOnlyException {
		byte [] b = new byte[sz];
		DedupFileChannel ch = OpenFile.OpenChannels.get(fd);
		ByteBuffer bf = ByteBuffer.wrap(b);
		ch.writeFile(bf, sz, 0, start, true);
		return sz;
		
	}

}
