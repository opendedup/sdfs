package org.opendedup.hashing;

public interface AbstractHashEngine {

	public boolean isVariableLength();

	public byte[] getHash(byte[] data);

	public void destroy();
}
