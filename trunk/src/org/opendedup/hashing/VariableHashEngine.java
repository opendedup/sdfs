package org.opendedup.hashing;

import java.io.IOException;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.opendedup.sdfs.Main;
import org.rabinfingerprint.handprint.BoundaryDetectors;
import org.rabinfingerprint.handprint.EnhancedFingerFactory;
import org.rabinfingerprint.handprint.EnhancedFingerFactory.EnhancedChunkVisitor;
import org.rabinfingerprint.handprint.FingerFactory.ChunkBoundaryDetector;
import org.rabinfingerprint.polynomial.Polynomial;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class VariableHashEngine implements AbstractHashEngine {

	public static final int seed = 6442;
	public static final int minLen = (4 * 1024);
	public static final int maxLen = Main.CHUNK_LENGTH;;
	static Polynomial p = Polynomial.createFromLong(10923124345206883L);
	ChunkBoundaryDetector boundaryDetector = BoundaryDetectors.DEFAULT_BOUNDARY_DETECTOR;
	static long bytesPerWindow = 48;
	EnhancedFingerFactory ff = new EnhancedFingerFactory(p, bytesPerWindow,
			boundaryDetector, minLen, maxLen);
	HashFunction hf = Hashing.murmur3_128(seed);
	
	public VariableHashEngine() throws NoSuchAlgorithmException {

	}

	@Override
	public byte[] getHash(byte[] data) {
		byte[] hash = hf.hashBytes(data).asBytes();
		return hash;
	}

	public List<Finger> getChunks(byte[] data) throws IOException {
		final ArrayList<Finger> al = new ArrayList<Finger>();
		ff.getChunkFingerprints(data, new EnhancedChunkVisitor() {
			public void visit(long fingerprint, long chunkStart, long chunkEnd,
					byte[] chunk) {
				byte[] hash = getHash(chunk);
				Finger f = new Finger();
				f.chunk = chunk;
				f.hash = hash;
				f.len = (int) (chunkEnd - chunkStart);
				f.start = (int) chunkStart;
				al.add(f);
			}
		});
		return al;
	}

	public static int getHashLenth() {
		// TODO Auto-generated method stub
		return 16;
	}

	public static int getMaxCluster() {
		return maxLen / minLen;
	}

	@Override
	public void destroy() {

	}

	@Override
	public boolean isVariableLength() {
		// TODO Auto-generated method stub
		return true;
	}
	
	@Override
	public int getMaxLen() {
		// TODO Auto-generated method stub
		return Main.CHUNK_LENGTH;
	}
}
