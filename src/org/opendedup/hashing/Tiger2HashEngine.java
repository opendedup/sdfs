package org.opendedup.hashing;



import java.security.NoSuchAlgorithmException;

import jonelo.jacksum.JacksumAPI;
import jonelo.jacksum.algorithm.AbstractChecksum;


public class Tiger2HashEngine implements AbstractHashEngine {
	
	AbstractChecksum md = null;
	public Tiger2HashEngine () throws NoSuchAlgorithmException{
		md = JacksumAPI.getChecksumInstance("tiger128");
	}
	
	public byte[] getHash(byte[] data) {
		md.update(data);
		byte [] hash = md.getByteArray();
		md.reset();
		return hash;
	}
	
	public int getHashLenth() {
		// TODO Auto-generated method stub
		return 16;
	}
	
	public void destroy() {
		md.reset();
		md =null;
	}
}
