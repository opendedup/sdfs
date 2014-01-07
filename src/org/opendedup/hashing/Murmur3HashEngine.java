package org.opendedup.hashing;


public class Murmur3HashEngine implements AbstractHashEngine {

	public static final int seed = 6442;

	public Murmur3HashEngine() {
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
