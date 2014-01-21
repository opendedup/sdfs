package org.opendedup.hashing;

import java.security.NoSuchAlgorithmException;

import org.opendedup.sdfs.Main;
import org.rabinfingerprint.handprint.BoundaryDetectors;
import org.rabinfingerprint.handprint.EnhancedFingerFactory;
import org.rabinfingerprint.handprint.FingerFactory.ChunkBoundaryDetector;
import org.rabinfingerprint.polynomial.Polynomial;

public class VariableHashEngine implements AbstractHashEngine {

	public static final int seed = 6442;
	public static final int minLen = 4 * 1024;
	public static final int maxLen = Main.CHUNK_LENGTH;
	static Polynomial p = Polynomial.createFromLong(10923124345206883L);
	ChunkBoundaryDetector boundaryDetector = BoundaryDetectors.DEFAULT_BOUNDARY_DETECTOR;
	static long bytesPerWindow = 48;
	EnhancedFingerFactory ff = new EnhancedFingerFactory(p,
			bytesPerWindow, boundaryDetector,minLen,maxLen);
	
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
	
	public static int getMaxCluster() {
		return maxLen/minLen;
	}

	@Override
	public void destroy() {
	
	}
}
