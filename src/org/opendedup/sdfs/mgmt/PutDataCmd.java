package org.opendedup.sdfs.mgmt;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.opendedup.collections.DataArchivedException;
import org.opendedup.sdfs.io.DedupFileChannel;
import org.opendedup.sdfs.io.ReadOnlyException;

public class PutDataCmd {

	public void getResult(long fd, long start,int sz,byte [] b,String md5) throws IOException,
			ClassNotFoundException, DataArchivedException, ReadOnlyException {
		DedupFileChannel ch = OpenFile.OpenChannels.get(fd);
		ByteBuffer bf = ByteBuffer.wrap(b);
		ch.writeFile(bf, sz, 0, start, true);
		
	}

}
