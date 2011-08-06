package org.opendedup.hashing;

public interface AbstractHashEngine {
	public byte[] getHash(byte[] data);

	public int getHashLenth();

	public void destroy();
}
