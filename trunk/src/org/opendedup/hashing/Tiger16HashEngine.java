package org.opendedup.hashing;

import java.security.NoSuchAlgorithmException;

import org.opendedup.sdfs.Main;

import jonelo.jacksum.JacksumAPI;
import jonelo.jacksum.algorithm.AbstractChecksum;

public class Tiger16HashEngine implements AbstractHashEngine {

	AbstractChecksum md = null;

	public Tiger16HashEngine() throws NoSuchAlgorithmException {
		md = JacksumAPI.getChecksumInstance("tiger128");
	}

	@Override
	public byte[] getHash(byte[] data) {
		md.update(data);
		byte[] hash = md.getByteArray();
		md.reset();
		return hash;
	}

	public static int getHashLenth() {
		// TODO Auto-generated method stub
		return 16;
	}

	@Override
	public void destroy() {
		md.reset();
		md = null;
	}

	@Override
	public boolean isVariableLength() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public int getMaxLen() {
		// TODO Auto-generated method stub
		return Main.chunkStorePageSize;
	}
}
