/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.hashing;

import java.io.IOException;



import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.StringUtils;
import org.rabinfingerprint.handprint.BoundaryDetectors;
import org.rabinfingerprint.handprint.FingerFactory.ChunkBoundaryDetector;
import org.rabinfingerprint.handprint.EnhancedFingerFactory;
import org.rabinfingerprint.handprint.EnhancedFingerFactory.EnhancedChunkVisitor;
import org.rabinfingerprint.polynomial.Polynomial;

import software.pando.crypto.siphash.SipHash;


public class VariableSipHashEngine implements AbstractHashEngine {


	
	static Polynomial p = Polynomial.createFromLong(10923124345206883L);
	ChunkBoundaryDetector boundaryDetector = BoundaryDetectors.DEFAULT_BOUNDARY_DETECTOR;
	private EnhancedFingerFactory ff = null;
	private SecretKey KEY = null;
	SipHash sipHash128;

	public VariableSipHashEngine() throws NoSuchAlgorithmException {
		this.setSeed(Main.hashSeed);
		while (ff == null) {
			SDFSLogger.getLog().info("Variable minLen=" +HashFunctionPool.minLen + " maxlen=" + HashFunctionPool.maxLen + " windowSize=" + HashFunctionPool.bytesPerWindow);
			ff = new EnhancedFingerFactory(p, HashFunctionPool.bytesPerWindow, boundaryDetector,
					HashFunctionPool.minLen, HashFunctionPool.maxLen);
		}
		sipHash128 = SipHash.getInstance(2, 4,128, KEY);

	}

	@Override
	public byte[] getHash(byte[] data) {
		byte[] hash = sipHash128.mac(data);
		return hash;
	}

	public List<Finger> getChunks(byte [] data,String lookupFilter,String uuid) throws IOException {
		final ArrayList<Finger> al = new ArrayList<Finger>();
		ff.getChunkFingerprints(data, new EnhancedChunkVisitor() {
			public void visit(long fingerprint, long chunkStart, long chunkEnd,
					byte[] chunk) {
				byte[] hash = getHash(chunk);
				Finger f = new Finger(lookupFilter,uuid);
				f.chunk = chunk;
				f.hash = hash;
				f.len = (int) (chunkEnd - chunkStart);
				f.start = (int) chunkStart;
				al.add(f);
			}
		});
		return al;
	}

	public static int getMaxCluster() {
		return Main.CHUNK_LENGTH / HashFunctionPool.minLen;
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

	@Override
	public int getMinLen() {
		// TODO Auto-generated method stub
		return HashFunctionPool.minLen;
	}

	@Override
	public void setSeed(int seed) {
		byte [] b = new byte[16];
		ByteBuffer buf = ByteBuffer.wrap(b);
		for(int i = 0;i<4;i++) {
			buf.putInt(seed);
		}
		KEY = new SecretKeySpec(buf.array(), "RAW");
		sipHash128 = SipHash.getInstance(2, 4,128, KEY);
	}
	
	public static void main(String [] args) throws NoSuchAlgorithmException {
		byte [] test  = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".getBytes();
		VariableSipHashEngine e = new VariableSipHashEngine();
		String h = StringUtils.getHexString(e.getHash(test));
		System.out.println(h);
		
	}
}
