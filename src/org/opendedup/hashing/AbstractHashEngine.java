package org.opendedup.hashing;

public interface AbstractHashEngine {
	public byte[] getHash(byte[] data);
	public void destroy();
}
