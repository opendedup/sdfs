package org.opendedup.hashing;

/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Collections of strategies of generating the k * log(M) bits required for an
 * element to be mapped to a BloomFilter of M bits and k hash functions. These
 * strategies are part of the serialized form of the Bloom filters that use
 * them, thus they must be preserved as is (no updates allowed, only
 * introduction of new versions).
 *
 * Important: the order of the constants cannot change, and they cannot be
 * deleted - we depend on their ordinal for BloomFilter serialization.
 *
 * @author Dimitris Andreou
 * @author Kurt Alfred Kluever
 */
enum FileBasedBloomFilterStrategies implements FileBasedBloomFilter.Strategy {
	/**
	 * See "Less Hashing, Same Performance: Building a Better Bloom Filter" by
	 * Adam Kirsch and Michael Mitzenmacher. The paper argues that this trick
	 * doesn't significantly deteriorate the performance of a Bloom filter (yet
	 * only needs two 32bit hash functions).
	 */
	MURMUR128_MITZ_32() {
		@Override
		public <T> boolean put(T object, Funnel<? super T> funnel,
				int numHashFunctions, BitArray bits) {
			long bitSize = bits.bitSize();
			long hash64 = Hashing.murmur3_128().hashObject(object, funnel)
					.asLong();
			int hash1 = (int) hash64;
			int hash2 = (int) (hash64 >>> 32);

			boolean bitsChanged = false;
			for (int i = 1; i <= numHashFunctions; i++) {
				int combinedHash = hash1 + (i * hash2);
				// Flip all the bits if it's negative (guaranteed positive
				// number)
				if (combinedHash < 0) {
					combinedHash = ~combinedHash;
				}
				bitsChanged |= bits.set(combinedHash % bitSize);
			}
			return bitsChanged;
		}

		@Override
		public <T> boolean mightContain(T object, Funnel<? super T> funnel,
				int numHashFunctions, BitArray bits) {
			long bitSize = bits.bitSize();
			long hash64 = Hashing.murmur3_128().hashObject(object, funnel)
					.asLong();
			int hash1 = (int) hash64;
			int hash2 = (int) (hash64 >>> 32);

			for (int i = 1; i <= numHashFunctions; i++) {
				int combinedHash = hash1 + (i * hash2);
				// Flip all the bits if it's negative (guaranteed positive
				// number)
				if (combinedHash < 0) {
					combinedHash = ~combinedHash;
				}
				if (!bits.get(combinedHash % bitSize)) {
					return false;
				}
			}
			return true;
		}
	},
	/**
	 * This strategy uses all 128 bits of {@link Hashing#murmur3_128} when
	 * hashing. It looks different than the implementation in MURMUR128_MITZ_32
	 * because we're avoiding the multiplication in the loop and doing a (much
	 * simpler) += hash2. We're also changing the index to a positive number by
	 * AND'ing with Long.MAX_VALUE instead of flipping the bits.
	 */
	MURMUR128_MITZ_64() {
		@Override
		public <T> boolean put(T object, Funnel<? super T> funnel,
				int numHashFunctions, BitArray bits) {
			long bitSize = bits.bitSize();
			byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel)
					.asBytes();
			long hash1 = lowerEight(bytes);
			long hash2 = upperEight(bytes);

			boolean bitsChanged = false;
			long combinedHash = hash1;
			for (int i = 0; i < numHashFunctions; i++) {
				// Make the combined hash positive and indexable
				bitsChanged |= bits.set((combinedHash & Long.MAX_VALUE)
						% bitSize);
				combinedHash += hash2;
			}
			return bitsChanged;
		}

		@Override
		public <T> boolean mightContain(T object, Funnel<? super T> funnel,
				int numHashFunctions, BitArray bits) {
			long bitSize = bits.bitSize();
			byte[] bytes = Hashing.murmur3_128().hashObject(object, funnel)
					.asBytes();
			long hash1 = lowerEight(bytes);
			long hash2 = upperEight(bytes);

			long combinedHash = hash1;
			for (int i = 0; i < numHashFunctions; i++) {
				// Make the combined hash positive and indexable
				if (!bits.get((combinedHash & Long.MAX_VALUE) % bitSize)) {
					return false;
				}
				combinedHash += hash2;
			}
			return true;
		}

		private/* static */long lowerEight(byte[] bytes) {
			return Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4],
					bytes[3], bytes[2], bytes[1], bytes[0]);
		}

		private/* static */long upperEight(byte[] bytes) {
			return Longs.fromBytes(bytes[15], bytes[14], bytes[13], bytes[12],
					bytes[11], bytes[10], bytes[9], bytes[8]);
		}
	};

	// Note: We use this instead of java.util.BitSet because we need access to
	// the long[] data field
	static final class BitArray {
		String path;
		FileChannel ch;
		RandomAccessFile rf;
		long bitCount;
		ByteBuffer bf = ByteBuffer.allocateDirect(8);
		int offset = 64;
		BitArray(long bits, String nm) {
			try {
				this.path = nm;
				long sz = (Ints.checkedCast(LongMath.divide(bits, 64,
						RoundingMode.CEILING)) * 8) + offset;
				File f = new File(path);
				boolean exists = f.exists();
				if(f.exists() && sz != f.length()) {
					throw new IOException("file already exists and lenths do not match");
				}
				System.out.println("size=" + sz + " fn=" + f.getPath());
				rf = new RandomAccessFile(new File(path), "rw");
				if(!exists) {
					rf.setLength(sz);
					ch = rf.getChannel();
				}else {
					rf.seek(0);
					this.bitCount = rf.readLong();
					ch = rf.getChannel();
					if(this.bitCount == -1) {
						long bitCount = 0;
						while (ch.position() < f.length()) {
							bf.position(0);
							ch.read(bf);
							bf.position(0);
							bitCount += Long.bitCount(bf.getLong());
						}

						this.bitCount = bitCount;
						
					}
				}
				System.out.println("bitcount=" +this.bitCount);
				rf.seek(0);
				rf.writeLong(-1);
				
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// Used by serialization
		BitArray(String nm) {
			this.path = nm;
			File f = new File(path);
			checkArgument(f.length() > 0, "data length is zero!");
			try {
				rf = new RandomAccessFile(new File(path), "rw");
				ch = rf.getChannel();
				long bitCount = 0;
				while (ch.position() < f.length()) {
					bf.position(0);
					ch.read(bf);
					bf.position(0);
					bitCount += Long.bitCount(bf.getLong());
				}

				this.bitCount = bitCount;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		BitArray(long [] data, String nm) {
			this.path = nm;
			File f = new File(path);
			checkArgument(f.length() > 0, "data length is zero!");
			try {
				rf = new RandomAccessFile(new File(path), "rw");
				ch = rf.getChannel();
				long bitCount = 0;
				for(long l: data) {
					rf.writeLong(l);
					bitCount += Long.bitCount(l);
				}
				this.bitCount = bitCount;
			} catch (Exception e) {
				e.printStackTrace();
			}
				
		}

		/**
		 * Returns true if the bit changed value.
		 * 
		 * @throws IOException
		 */
		boolean set(long index) {
			synchronized(bf) {
			try {
				if (!get(index)) {
					bf.position(0);
					long pos = ((index >>> 6) * 8) + offset;
					ch.read(bf, pos);
					bf.position(0);
					long idx = bf.getLong();
					idx |= (1L << index);
					
					bf.position(0);
					bf.putLong(idx);
					bf.position(0);
					ch.write(bf,pos);
					bitCount++;
					return true;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return false;
			}

		}

		boolean get(long index) {
			synchronized(bf) {
			try {
				bf.position(0);
				long pos = ((index >>> 6) * 8) + offset;
				ch.read(bf, pos);
				bf.position(0);
				long idx = bf.getLong();
				return (idx & (1L << index)) != 0;
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
			}
		}

		/**
		 * Number of bits
		 * 
		 * @throws IOException
		 */
		long bitSize() {
			try {
			return (long) ((ch.size()-this.offset) / 8) * Long.SIZE;
			}catch(Exception e) {
				e.printStackTrace();
				return 0;
			}
		}

		/** Number of set bits (1s) */
		long bitCount() {
			return bitCount;
		}

		BitArray copy() {
			try {
				Files.copy(new File(this.path), new File(this.path + ".copy"));
				return new BitArray(this.path + ".copy");
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}

		/** Combines the two BitArrays using bitwise OR. */
		void putAll(BitArray array) {
			try{
			checkArgument(array.ch.size() == this.ch.size(),
					"BitArrays must be of equal length (%s != %s)",
					ch.size(), array.ch.size());
			bitCount = 0;
			ch.position(offset);
			while(ch.position() < ch.size()){
				bf.position(0);
				long pos = ch.position();
				ch.read(bf);
				bf.position(0);
				
				long cp = bf.getLong();
				bf.position(0);
				array.ch.read(bf,pos);
				bf.position(0);
				long np = bf.getLong();
				cp |= np;
				bf.position(0);
				bf.putLong(cp);
				bf.position(0);
				ch.write(bf, pos);
				bitCount += Long.bitCount(np);
			}
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		void putAll(byte [] array) {
			try{
			checkArgument(array.length == this.ch.size(),
					"BitArrays must be of equal length (%s != %s)",
					ch.size(), array.length);
			ByteBuffer nbf = ByteBuffer.wrap(array);
			bitCount = nbf.getLong();
			ch.position(offset);
			nbf.position(offset);
			while(ch.position() < ch.size()){
				bf.position(0);
				long pos = ch.position();
				ch.read(bf);
				bf.position(0);
				long cp = bf.getLong();
				bf.position(0);
				long np = nbf.getLong();
				cp |= np;
				bf.position(0);
				bf.putLong(cp);
				bf.position(0);
				ch.write(bf, pos);
				bitCount += Long.bitCount(np);
			}
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		

		@Override
		public boolean equals(Object o) {
			if (o instanceof BitArray) {
				BitArray bitArray = (BitArray) o;
				return Arrays.equals(this.path.getBytes(), bitArray.path.getBytes());
			}
			return false;
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(this.path.getBytes());
		}
		
		public void close() {
			
			try {
				rf.seek(0);
				rf.writeLong(this.bitCount);
				rf.close();
			}catch(Exception e) {}
			try {
				ch.close();
			}catch(Exception e) {
				
			}
		}
		
		public void vanish() {
			this.close();
			File f = new File(this.path);
			f.delete();
		}
	}
}
