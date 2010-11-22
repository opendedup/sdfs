package org.opendedup.hashing;


import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

import com.annesam.cudaMD5.StackedMD5Hash;

public class MD5CudaHash implements AbstractHashEngine {
	
	private static  StackedMD5Hash md5 = null;
	
	static {
		try {
			md5 = new StackedMD5Hash(4096,Main.writeThreads);
		} catch(Exception e) {
			SDFSLogger.getLog().error("unable to initialise CudaMD5Hash", e);
		}
	}

	public byte[] getHash(byte[] data) {
		return md5.getHash(data);
	}
	public int getHashLenth() {
		return 16;
	}
	
	public void destroy() {
		
	}
	
	public static void freeMem() {
		if(md5 != null) {
			md5.close();
		}
	}
	

}
