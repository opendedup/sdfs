package org.opendedup.hashing;

import java.security.NoSuchAlgorithmException;

public class VariableHashEngine implements AbstractHashEngine {

	public static final int seed = 6442;

	public VariableHashEngine() throws NoSuchAlgorithmException {
	}

	@Override
	public byte[] getHash(byte[] data) {
		byte[] hash = MurmurHash3.murmur128(data, seed);
		return hash;
	}

	public static int getHashLenth() {
		// TODO Auto-generated method stub
		return 16;
	}

	@Override
	public void destroy() {
	}
}
