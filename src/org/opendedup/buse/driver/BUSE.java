package org.opendedup.buse.driver;

import java.nio.ByteBuffer;

public interface BUSE {
	public int read(ByteBuffer data,int len,long offset);
	public int write(ByteBuffer buff,int len, long offset);
	public void disconnect();
	public int flush();
	public int trim(long from,int len);
	public void close();

}
