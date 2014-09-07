package org.opendedup.hashing;

import org.opendedup.sdfs.Main;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class Murmur3HashEngine implements AbstractHashEngine {

	public static final int seed = 6442;
	HashFunction hf = Hashing.murmur3_128(seed);
	public Murmur3HashEngine() {
	}

	@Override
	public byte[] getHash(byte[] data) {
		byte[] hash = hf.hashBytes(data).asBytes();
		return hash;
	}

	public static int getHashLenth() {
		// TODO Auto-generated method stub
		return 16;
	}

	@Override
	public void destroy() {
	}

	@Override
	public boolean isVariableLength() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getMaxLen() {
		// TODO Auto-generated method stub
		return Main.CHUNK_LENGTH;
	}
}
