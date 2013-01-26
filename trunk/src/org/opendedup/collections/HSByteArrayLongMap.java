package org.opendedup.collections;

////////////////////////////////////////////////////////////////////////////////
//ConcurrentHopscotchHashMap Class
//
////////////////////////////////////////////////////////////////////////////////
//TERMS OF USAGE
//----------------------------------------------------------------------
//
//Permission to use, copy, modify and distribute this software and
//its documentation for any purpose is hereby granted without fee,
//provided that due acknowledgments to the authors are provided and
//this permission notice appears in all copies of the software.
//The software is provided "as is". There is no warranty of any kind.
//
//Authors:
//Maurice Herlihy
//Brown University
//and
//Nir Shavit
//Tel-Aviv University
//and
//Moran Tzafrir
//Tel-Aviv University
//
//Date: Dec 2, 2008.
//
////////////////////////////////////////////////////////////////////////////////
//Programmer : Moran Tzafrir (MoranTza@gmail.com)
//
////////////////////////////////////////////////////////////////////////////////
//package xbird.util.concurrent.map;

import java.io.File;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import jonelo.jacksum.adapt.org.bouncycastle.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.OpenBitSet;
import org.opendedup.collections.threads.SyncThread;
import org.opendedup.hashing.HashFunctionPool;
import org.opendedup.hashing.MurmurHash3;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.ChunkData;
import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.notification.SDFSEvent;
import org.opendedup.util.CommandLineProgressBar;
import org.opendedup.util.OpenBitSetSerialize;
import org.opendedup.util.StringUtils;

public class HSByteArrayLongMap extends ReentrantLock implements AbstractMap,
		AbstractHashesMap {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5818134060407636053L;
	// constants -----------------------------------
	static final short _NULL_DELTA_SHORT = Short.MIN_VALUE;
	static final int _NULL_DELTA_INT = (int) (Short.MIN_VALUE);
	static final long _NULL_DELTA_FIRST_LONG = (long) (Short.MIN_VALUE & 0xFFFFL);
	static final long _NULL_DELTA_NEXT_LONG = (long) ((Short.MIN_VALUE & 0xFFFFL) << 16);
	static final long _NULL_HASH_DELTA = 0x0000000080008000L;
	static final int _NULL_HASH = 0;
	static final int _SEGMENT_SHIFT = 0; // choosed by empirical tests

	static final long _FIRST_MASK = 0x000000000000FFFFL;
	static final long _NEXT_MASK = 0x00000000FFFF0000L;
	static final long _HASH_MASK = 0xFFFFFFFF00000000L;
	static final long _NOT_NEXT_MASK = ~(_NEXT_MASK);
	static final long _NOT_FIRST_MASK = ~(_FIRST_MASK);
	static final long _NOT_HASH_MASK = ~(_HASH_MASK);
	static final int _NEXT_SHIFT = 16;
	static final int _HASH_SHIFT = 32;

	static final int _RETRIES_BEFORE_LOCK = 2;
	static final int _MAX_DELTA_BUCKET = Short.MAX_VALUE;

	static final int _CACHE_MASK = 4 - 1;
	static final int _NOT_CACHE_MASK = ~_CACHE_MASK;
	static final int _NULL_INDX = -1;
	static final long[] vals = { _NULL_DELTA_SHORT, _NULL_DELTA_INT,
			_NULL_DELTA_FIRST_LONG, _NULL_DELTA_NEXT_LONG, _NULL_HASH_DELTA,
			_NULL_HASH, _FIRST_MASK, _NEXT_MASK, _NOT_NEXT_MASK,
			_NOT_FIRST_MASK };

	// inner classes -------------------------------

	static final class Segment extends ReentrantLock {

		/**
		 * 
		 */
		private static final long serialVersionUID = -6272185204011768840L;
		volatile int _timestamp;
		int _bucketk_mask;
		// long[] _table_hash_delta;
		// Object[] _table_key_value;
		private FileChannel kFC = null;
		private FileChannel hFC = null;
		private FileChannel vRaf = null;
		private FileChannel tRaf = null;
		MappedByteBuffer hm = null;
		MappedByteBuffer km = null;
		MappedByteBuffer vm = null;
		MappedByteBuffer tm = null;
		private int hashLength = HashFunctionPool.hashLength;
		// final int pageSize = hashLength + 8 + 8;
		private String path;
		// AtomicInteger _lock;
		int _count;
		private ReentrantLock hashlock = new ReentrantLock();
		private ReentrantLock keylock = new ReentrantLock();
		private int iterpos = 0;
		private long largest = 0;
		private int capacity = 0;
		//private OpenBitSet claimed = null;
		private boolean closed = false;

		public Segment(final int initialCapacity, String path)
				throws IOException {
			this.path = path;
			this.capacity = initialCapacity;
			init(initialCapacity);
		}

		public synchronized void close() {
			this.lock();
			try {
				try {
					this.kFC.close();
				} catch (IOException e) {

				}
				try {
					this.hFC.close();
				} catch (IOException e) {

				}
				try {
					this.vRaf.close();
				} catch (IOException e) {

				}
				try {
					this.tRaf.close();
				} catch (IOException e) {

				}
				this.closed = true;
				try {
					FileChannel sFc = FileChannel.open(Paths.get(path + ".sz"),
							StandardOpenOption.CREATE,
							StandardOpenOption.SPARSE,
							StandardOpenOption.WRITE, StandardOpenOption.READ);
					ByteBuffer buf = ByteBuffer.allocate(12);
					buf.putInt(_count);
					buf.putLong(largest);
					buf.position(0);
					sFc.write(buf);
					sFc.force(true);
					sFc.close();
					sFc = null;
					buf = null;
				} catch (IOException e) {

				}
			} finally {
				this.unlock();
			}

		}

		public void sync() {
			try {
				this.kFC.force(true);
			} catch (IOException e) {

			}
			try {
				this.hFC.force(true);
			} catch (IOException e) {

			}
			try {
				this.vRaf.force(true);
			} catch (IOException e) {

			}
			try {
				this.tRaf.force(true);
			} catch (IOException e) {

			}
		}

		private void init(final int initialCapacity) throws IOException {
			// _lock = new AtomicInteger();
			// _lock.set(0);
			//claimed = new OpenBitSet(initialCapacity);
			File f = new File(path + ".hh");
			boolean newht = !f.exists();
			this.kFC = FileChannel.open(Paths.get(path + ".hk"),
					StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
					StandardOpenOption.WRITE, StandardOpenOption.READ);
			this.hFC = FileChannel.open(Paths.get(path + ".hh"),
					StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
					StandardOpenOption.WRITE, StandardOpenOption.READ);
			boolean closedproperly = new File(path + ".sz").exists();
			FileChannel sFc = FileChannel.open(Paths.get(path + ".sz"),
					StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
					StandardOpenOption.WRITE, StandardOpenOption.READ);

			hm = hFC.map(MapMode.READ_WRITE, 0, initialCapacity * 8);
			// other half of key + value + timestamp
			int kmc = initialCapacity;
			km = kFC.map(MapMode.READ_WRITE, 0, kmc * hashLength);

			vRaf = FileChannel.open(Paths.get(path + ".hhpos"),
					StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
					StandardOpenOption.WRITE, StandardOpenOption.READ);
			tRaf = FileChannel.open(Paths.get(path + ".hhctimes"),
					StandardOpenOption.CREATE, StandardOpenOption.SPARSE,
					StandardOpenOption.WRITE, StandardOpenOption.READ);
			vm = vRaf.map(MapMode.READ_WRITE, 0, kmc * 8);
			tm = tRaf.map(MapMode.READ_WRITE, 0, kmc * 8);
			hm.load();
			km.load();
			vm.load();
			tm.load();
			// km.position(5280);
			_timestamp = 0;
			_bucketk_mask = initialCapacity - 1;
			_count = 0;

			// _table_hash_delta = new long[initialCapacity];
			// _table_key_value = new Object[initialCapacity << 1];

			// create the blocks of buckets
			if (newht) {
				for (int iData = 0; iData < initialCapacity; ++iData) {
					this.hm.putLong(_NULL_HASH_DELTA);
				}
				sFc.close();
				new File(path + ".sz").delete();
			} else {
				if (!closedproperly) {
					this.hm.position(0);
					for (int iData = 0; iData < initialCapacity; ++iData) {
						if (this.hm.getLong() != _NULL_HASH_DELTA) {
							_count++;
							long pos = this.getKeyValue(iData);
							if (pos > this.largest)
								this.largest = pos;
						}
					}
					sFc.close();
					new File(path + ".sz").delete();
				} else {
					ByteBuffer buf = ByteBuffer.allocate(12);
					sFc.read(buf);
					buf.position(0);
					this._count = buf.getInt();
					this.largest = buf.getLong();
					sFc.close();
					new File(path + ".sz").delete();
				}
				/*
				 * System.out.println("count = " + this._count + " largest = " +
				 * this.largest);
				 */
			}
		}

		/*
		 * void lock() { while(!_lock.compareAndSet(0, 0xFFFF)) { } }
		 * 
		 * void unlock() { _lock.set(0); }
		 */

		static final Segment[] newArray(final int numSegments) {
			return new Segment[numSegments];
		}

		private long getHashVal(int pos) {
			this.hashlock.lock();
			try {
				hm.position(pos * 8);
				long data = hm.getLong();
				return data;
			} catch (IllegalArgumentException e) {
				System.out.println("error pos=" + (pos * 8));
				e.printStackTrace();
				return -1;
			} finally {
				this.hashlock.unlock();
			}
		}

		private byte[] getKey(int pos) {
			this.keylock.lock();
			try {
				km.position(pos * this.hashLength);
				byte[] key = new byte[this.hashLength];
				km.get(key);
				return key;
			} finally {
				this.keylock.unlock();
			}
		}

		ByteBuffer lbuf = ByteBuffer.allocate(8);

		private long getKeyValue(int pos) throws IOException {
			this.keylock.lock();
			try {

				vm.position(pos * 8);
				return vm.getLong();
			} finally {
				this.keylock.unlock();
			}
		}

		private long getKeyTime(int pos) throws IOException {
			this.keylock.lock();
			try {
				tm.position(pos * 8);
				return tm.getLong();
			} finally {
				this.keylock.unlock();
			}
		}

		private void setKeyVal(int pos, byte[] key, long val)
				throws IOException {
			this.keylock.lock();
			try {
				int npos = pos * this.hashLength;
				if (val > this.largest)
					this.largest = val;
				km.position(npos);
				km.put(key);

				vm.position(pos * 8);
				vm.putLong(val);

				tm.position(pos * 8);
				tm.putLong(System.currentTimeMillis());
			} finally {
				this.keylock.unlock();
			}
		}

		private void setKeyTime(int pos) throws IOException {
			this.keylock.lock();
			try {

				tm.position(pos * 8);
				tm.putLong(System.currentTimeMillis());
			} finally {
				this.keylock.unlock();
			}
		}

		private void setHashVal(int pos, long val) {
			this.hashlock.lock();
			try {
				/*
				 * for (int i = 0; i < vals.length; i++) { if (val == vals[i])
				 * System.out.println("found at " + i + " val " + vals[i]); }
				 */
				if (val == HSByteArrayLongMap._NULL_HASH_DELTA) {
					//this.claimed.fastClear(pos);
				}
				else {
					//this.claimed.fastSet(pos);
				}
				hm.position(pos * 8);
				hm.putLong(val);
			} finally {
				this.hashlock.unlock();
			}
		}

		boolean containsKey(final byte[] key) {
			// go over the list and look for key
			final int hash = hash(key);
			int start_timestamp = _timestamp;
			int iBucket = (hash & _bucketk_mask);
			hm.position(iBucket * 8);
			long data = hm.getLong();
			final int first_delta = (short) data;
			if (0 != first_delta) {
				if (_NULL_DELTA_SHORT == first_delta)
					return false;
				iBucket += first_delta;

				data = this.getHashVal(iBucket);
			}

			do {
				if (hash == (data >> _HASH_SHIFT)
						&& Arrays.areEqual(key, this.getKey(iBucket))) {
					//this.claimed.fastSet(iBucket);
					return true;
				}
				final int nextDelta = (int) data >> _NEXT_SHIFT;
				if (_NULL_DELTA_INT != nextDelta) {
					iBucket += nextDelta;
					data = this.getHashVal(iBucket);
					continue;
				} else {
					final int curr_timestamp = _timestamp;
					if (curr_timestamp == start_timestamp)
						return false;
					start_timestamp = curr_timestamp;
					iBucket = hash & _bucketk_mask;
					data = this.getHashVal(iBucket);
					final int first_delta2 = (short) data;
					if (0 != first_delta2) {
						if (_NULL_DELTA_SHORT == first_delta2)
							return false;
						iBucket += first_delta2;
						data = this.getHashVal(iBucket);
					}
					continue;
				}
			} while (true);

		}

		long get(final byte[] key, boolean claim) throws IOException {
			// go over the list and look for key
			final int hash = hash(key);
			int start_timestamp = 0;
			int iBucket = 0;
			long data = 0;

			boolean is_need_init = true;
			do {
				if (is_need_init) {
					is_need_init = false;
					start_timestamp = _timestamp;
					iBucket = hash & _bucketk_mask;
					data = this.getHashVal(iBucket);
					final int first_delta = (short) data;
					if (0 != first_delta) {
						if (_NULL_DELTA_SHORT == first_delta)
							return -1;
						iBucket += first_delta;
						data = this.getHashVal(iBucket);
					}
				}

				final int iRef;
				if (hash == (data >> _HASH_SHIFT)
						&& Arrays.areEqual(key, this.getKey(iRef = iBucket))) {
					final long value = this.getKeyValue(iRef);
					if (_timestamp == start_timestamp) {
						if (claim) {
							//this.claimed.fastSet(iBucket);
						}
						return value;
					}
					is_need_init = true;
					continue;
				}
				final int nextDelta = (int) data >> _NEXT_SHIFT;
				if (_NULL_DELTA_INT != nextDelta) {
					iBucket += nextDelta;
					data = this.getHashVal(iBucket);
					continue;
				} else {
					if (_timestamp == start_timestamp)
						return -1;
					is_need_init = true;
					continue;
				}
			} while (true);
		}

		boolean put(final byte[] key, final long value)
				throws HashtableFullException, IOException {
			final int hash = hash(key);
			lock();
			try {
				if (this._count >= this.capacity)
					throw new HashtableFullException(
							"entries is greater than or equal to the maximum number of entries. You need to expand"
									+ "the volume or DSE allocation size");
				// look for key in hash-map
				// .....................................
				final int i_start_bucket = hash & _bucketk_mask;
				int iBucket = i_start_bucket;
				long data = this.getHashVal(i_start_bucket);
				final short first_delta = (short) data;
				if (_NULL_DELTA_SHORT != first_delta) {
					if (0 != first_delta) {
						iBucket += first_delta;
						data = this.getHashVal(iBucket);
					}

					do {
						// final int iRef;
						if (hash == (data >> _HASH_SHIFT)
								&& Arrays.areEqual(key, this.getKey(iBucket))) {
							//this.claimed.fastSet(iBucket);
							// return this.getKeyValue(iRef);
							return false;
						}
						final int next_delta = (int) data >> _NEXT_SHIFT;
						if (_NULL_DELTA_INT == next_delta)
							break;
						else {
							iBucket += next_delta;
							data = this.getHashVal(iBucket);
						}
					} while (true);
				}

				// try to place the key in the same cache-line
				// ..................
				final int i_start_cacheline = i_start_bucket & _NOT_CACHE_MASK;
				final int i_end_cacheline = i_start_cacheline + _CACHE_MASK;
				int i_free_bucket = i_start_bucket;
				do {
					long free_data = this.getHashVal(i_free_bucket);
					if (_NULL_HASH == (free_data >> _HASH_SHIFT)) {
						// we found a free bucket at the cahce-line, so
						// add the new bucket to the begining of the list

						int i_ref_bucket = i_free_bucket;
						// this.claimed.set(i_free_bucket);
						this.setKeyVal(i_ref_bucket, key, value);
						free_data &= _NOT_HASH_MASK;
						free_data |= ((long) hash << _HASH_SHIFT);

						if (0 == first_delta) {

							final long start_data = this
									.getHashVal(i_start_bucket);
							final int start_next = (int) start_data >> _NEXT_SHIFT;
							if (_NULL_DELTA_INT != start_next) {
								final long new_free_next = i_start_bucket
										+ start_next - i_free_bucket;
								this.setHashVal(
										i_free_bucket,
										(free_data & _NOT_NEXT_MASK)
												| ((new_free_next << _NEXT_SHIFT) & _NEXT_MASK));
							} else
								this.setHashVal(i_free_bucket, free_data);
							final long new_start_next = i_free_bucket
									- i_start_bucket;
							this.setHashVal(
									i_start_bucket,
									(start_data & _NOT_NEXT_MASK)
											| ((new_start_next << _NEXT_SHIFT) & _NEXT_MASK));
						} else {// 0 != first_delta
							if (_NULL_DELTA_SHORT != first_delta) {
								final long new_free_next = i_start_bucket
										+ first_delta - i_free_bucket;
								free_data &= _NOT_NEXT_MASK;
								free_data |= ((new_free_next << _NEXT_SHIFT) & _NEXT_MASK);
							}
							final long start_data;
							if (i_free_bucket != i_start_bucket) {
								start_data = this.getHashVal(i_start_bucket);
								this.setHashVal(i_free_bucket, free_data);
							} else
								start_data = free_data;
							final long new_start_first = i_free_bucket
									- i_start_bucket;
							this.setHashVal(i_start_bucket,
									(start_data & _NOT_FIRST_MASK)
											| (new_start_first & _FIRST_MASK));
						}

						++_count;
						++_timestamp;
						return true;
					}

					++i_free_bucket;
					if (i_free_bucket > i_end_cacheline)
						i_free_bucket = i_start_cacheline;
				} while (i_start_bucket != i_free_bucket);

				// place key in arbitrary free forward bucket
				// ...................
				int i_max_bucket = i_start_bucket + _MAX_DELTA_BUCKET;
				if (i_max_bucket > _bucketk_mask)
					i_max_bucket = _bucketk_mask;
				i_free_bucket = i_end_cacheline + 1;

				while (i_free_bucket <= i_max_bucket) {
					long free_data = this.getHashVal(i_free_bucket);
					if (_NULL_HASH == (free_data >> _HASH_SHIFT)) {
						// we found a free bucket outside of the cahce-line, so
						// add the new bucket to the end of the list

						int i_ref_bucket = i_free_bucket;
						this.setKeyVal(i_ref_bucket, key, value);
						free_data &= _NOT_HASH_MASK;
						free_data |= ((long) hash << _HASH_SHIFT);
						this.setHashVal(i_free_bucket, free_data);

						if (_NULL_DELTA_SHORT == first_delta) {
							long new_start_first = (i_free_bucket - i_start_bucket)
									& _FIRST_MASK;
							long start_data = (this.getHashVal(i_start_bucket) & _NOT_FIRST_MASK)
									| new_start_first;
							this.setHashVal(i_start_bucket, start_data);
						} else {
							long new_last_next = ((i_free_bucket - iBucket) << _NEXT_SHIFT)
									& _NEXT_MASK;
							long last_data = (this.getHashVal(iBucket) & _NOT_NEXT_MASK)
									| new_last_next;
							this.setHashVal(iBucket, last_data);
						}

						++_count;
						++_timestamp;
						return true;
					}

					i_free_bucket += 2;
				}

				// place key in arbitrary free backward bucket
				// ...................
				int i_min_bucket = i_start_bucket - _MAX_DELTA_BUCKET;
				if (i_min_bucket < 0)
					i_min_bucket = 0;
				i_free_bucket = i_start_cacheline - 1;

				while (i_free_bucket >= i_min_bucket) {
					long free_data = this.getHashVal(i_free_bucket);
					if (_NULL_HASH == (free_data >> _HASH_SHIFT)) {
						// we found a free bucket outside of the cahce-line, so
						// add the new bucket to the end of the list

						int i_ref_bucket = i_free_bucket;
						// this.claimed.set(i_free_bucket);
						this.setKeyVal(i_ref_bucket, key, value);
						free_data &= _NOT_HASH_MASK;
						free_data |= ((long) hash << _HASH_SHIFT);
						this.setHashVal(i_free_bucket, free_data);

						if (_NULL_DELTA_SHORT == first_delta) {
							long new_start_first = (i_free_bucket - i_start_bucket)
									& _FIRST_MASK;
							long start_data = (this.getHashVal(i_start_bucket) & _NOT_FIRST_MASK)
									| new_start_first;
							this.setHashVal(i_start_bucket, start_data);
						} else {
							long new_last_next = ((i_free_bucket - iBucket) << _NEXT_SHIFT)
									& _NEXT_MASK;
							long last_data = (this.getHashVal(iBucket) & _NOT_NEXT_MASK)
									| new_last_next;
							this.setHashVal(iBucket, last_data);
						}

						++_count;
						++_timestamp;
						return true;
					}

					i_free_bucket -= 2;
				}

			} finally {
				unlock();
			}

			return false;
		}

		private void optimize_cacheline_use(final int i_free_bucket)
				throws IOException {
			final int i_start_cacheline = i_free_bucket & _NOT_CACHE_MASK;
			final int i_end_cacheline = i_start_cacheline + _CACHE_MASK;

			// go over the buckets that reside in the cacheline of the free
			// bucket
			for (int i_cacheline = i_start_cacheline; i_cacheline <= i_end_cacheline; ++i_cacheline) {

				// check if current bucket has keys
				final long data = this.getHashVal(i_cacheline);
				final short first_delta = (short) data;
				if (_NULL_DELTA_INT != first_delta) {

					int last_i_relocate = _NULL_INDX;
					int i_relocate = i_cacheline + first_delta;
					int curr_delta = first_delta;

					// go over the keys in the bucket-list
					do {
						// if the key reside outside the cahceline
						if (curr_delta < 0 || curr_delta > _CACHE_MASK) {

							// copy key, value, & hash to the free bucket
							final int i_key_value = i_free_bucket;
							final int i_rel_key_value = i_relocate;
							// _table_key_value[i_key_value] =
							// _table_key_value[i_rel_key_value];
							// _table_key_value[i_key_value + 1] =
							// _table_key_value[i_rel_key_value + 1];
							this.setKeyVal(i_key_value,
									this.getKey(i_rel_key_value),
									this.getKeyValue(i_rel_key_value));
							/*
							 * this.setKeyVal(i_key_value + 1,
							 * this.getKey(i_rel_key_value + 1),
							 * this.getKeyValue(i_rel_key_value + 1));
							 */
							long relocate_data = this.getHashVal(i_relocate);
							long free_data = this.getHashVal(i_free_bucket);
							free_data &= _NOT_HASH_MASK;
							free_data |= (relocate_data & _HASH_MASK);

							// update the next-field of the free-bucket
							free_data &= _NOT_NEXT_MASK;
							final int relocate_next_delta = (int) relocate_data >> _NEXT_SHIFT;
							if (_NULL_DELTA_INT == relocate_next_delta) {
								free_data |= _NULL_DELTA_NEXT_LONG;
							} else {
								final long new_next = (((i_relocate + relocate_next_delta) - i_free_bucket) & 0xFFFFL) << 16;
								free_data |= new_next;
							}
							// this.claimed.fastSet(i_free_bucket);
							this.setHashVal(i_free_bucket, free_data);

							// update the "first" or "next" field of the last
							if (_NULL_INDX == last_i_relocate) {
								long start_data = this.getHashVal(i_cacheline)
										& _NOT_FIRST_MASK;
								start_data |= ((i_free_bucket - i_cacheline) & 0xFFFFL);
								this.setHashVal(i_cacheline, start_data);
							} else {
								long last_data = this
										.getHashVal(last_i_relocate)
										& _NOT_NEXT_MASK;
								last_data |= (((i_free_bucket - last_i_relocate) & 0xFFFFL) << 16);
								this.setHashVal(last_i_relocate, last_data);
							}

							//
							++_timestamp;
							relocate_data &= _NOT_HASH_MASK;// hash=null
							relocate_data &= _NOT_NEXT_MASK;
							relocate_data |= _NULL_DELTA_NEXT_LONG;// next =
																	// null
							this.setHashVal(i_relocate, relocate_data);
							// _table_key_value[i_rel_key_value] = null;//
							// key=null
							// _table_key_value[i_rel_key_value + 1] = null;//
							// value=null
							return;
						}

						final long relocate_data = this.getHashVal(i_relocate);
						final int next_delta = (int) relocate_data >> _NEXT_SHIFT;
						if (_NULL_DELTA_INT == next_delta)
							break;
						last_i_relocate = i_relocate;
						curr_delta += next_delta;
						i_relocate += next_delta;
					} while (true);// for on list
				}// if list exists
			}// for on list
		}

		boolean remove(final byte[] key) throws IOException {
			final int hash = hash(key);
			lock();
			try {
				// go over the list and look for key
				final int i_start_bucket = hash & _bucketk_mask;
				int iBucket = i_start_bucket;
				long data = this.getHashVal(iBucket);
				final short first_delta = (short) data;
				if (0 != first_delta) {
					if (_NULL_DELTA_SHORT == first_delta)
						return false;
					iBucket += first_delta;
					data = this.getHashVal(iBucket);
				}

				int i_last_bucket = -1;
				do {
					// final int iRef;
					if (hash == (data >> _HASH_SHIFT)
							&& Arrays.areEqual(key, this.getKey(iBucket))) {

						data &= _NOT_HASH_MASK;
						final int next_delta = (int) data >> _NEXT_SHIFT;
						this.setHashVal(iBucket, data); // hash = null
						// _table_key_value[iRef] = null; // key = null;

						// final int iRef2 = iRef + 1;
						// final long key_value = this.getKeyValue(iRef2);
						// _table_key_value[iRef2] = null; // value = null;

						if (-1 == i_last_bucket) {
							long start_data = this.getHashVal(i_start_bucket)
									& _NOT_FIRST_MASK;
							if (_NULL_DELTA_INT == next_delta) {
								start_data |= _NULL_DELTA_FIRST_LONG;
							} else {
								final long new_first = (first_delta + next_delta) & 0xFFFFL;
								start_data |= new_first;
							}
							if (i_start_bucket == iBucket) {
								start_data &= _NOT_NEXT_MASK;
								start_data |= _NULL_DELTA_NEXT_LONG;
								--_count;
								++_timestamp;
								this.setHashVal(i_start_bucket, start_data);
								// return key_value;
							} else
								this.setHashVal(i_start_bucket, start_data);
						} else {
							long last_data = this.getHashVal(i_last_bucket);
							final int last_next_delta = (int) last_data >> _NEXT_SHIFT;
							last_data &= _NOT_NEXT_MASK;
							if (_NULL_DELTA_INT == next_delta) {
								last_data |= _NULL_DELTA_NEXT_LONG;
							} else {
								final long new_next = ((last_next_delta + next_delta) & 0xFFFFL) << 16;
								last_data |= new_next;
							}
							this.setHashVal(i_last_bucket, last_data);
						}

						if (i_start_bucket != iBucket) {
							--_count;
							++_timestamp;
							data &= _NOT_NEXT_MASK;
							data |= _NULL_DELTA_NEXT_LONG;
							this.setHashVal(iBucket, data); // next = null
						}

						optimize_cacheline_use(iBucket);

						return true;
					}
					final int nextDelta = (int) data >> _NEXT_SHIFT;
					if (_NULL_DELTA_INT != nextDelta) {
						i_last_bucket = iBucket;
						iBucket += nextDelta;
						data = this.getHashVal(iBucket);
						continue;
					} else
						return false;
				} while (true);

			} finally {
				unlock();
			}
		}

		void clear() {
		}

		public void iterInit() {
			this.iterpos = 0;

		}

		public byte[] nextKey() throws IOException {
			this.lock();
			try {
				while (iterpos < capacity) {

					long val = this.getHashVal(iterpos);

					if (val == HSByteArrayLongMap._NULL_HASH_DELTA) {
						return this.getKey(iterpos);
					}

				}
				return null;
			} finally {
				iterpos++;
				this.unlock();
			}
		}

		public byte[] nextClaimedKey(boolean clearClaim) throws IOException {
			this.lock();
			try {
				while (iterpos < capacity) {

					long val = this.getHashVal(iterpos);

					if (val == HSByteArrayLongMap._NULL_HASH_DELTA) {
						//boolean cl = this.claimed.fastGet(iterpos);
						//if (cl) {
							//this.claimed.fastSet(iterpos);
							//return this.getKey(iterpos);
						//}
					}
				}
				return null;
			} finally {
				iterpos++;
				this.unlock();
			}
		}

		public long nextClaimedValue(boolean clearClaim) throws IOException {
			this.lock();
			try {
				while (iterpos < capacity) {

					long val = this.getHashVal(iterpos);

					if (val == HSByteArrayLongMap._NULL_HASH_DELTA) {
						//boolean cl = this.claimed.fastGet(iterpos);
						//if (cl) {
							//this.claimed.fastSet(iterpos);
							//return this.getKeyValue(iterpos);
						//}
					}
				}
				return -1;
			} finally {
				iterpos++;
				this.unlock();
			}
		}

		public long getBigestKey() throws IOException {
			return this.largest;
		}

		public boolean isClaimed(byte[] key) throws KeyNotFoundException,
				IOException {
			// go over the list and look for key
			final int hash = hash(key);
			int start_timestamp = _timestamp;
			int iBucket = (hash & _bucketk_mask);
			hm.position(iBucket * 8);
			long data = hm.getLong();
			final int first_delta = (short) data;
			if (0 != first_delta) {
				if (_NULL_DELTA_SHORT == first_delta)
					return false;
				iBucket += first_delta;

				data = this.getHashVal(iBucket);
			}

			do {
				if (hash == (data >> _HASH_SHIFT)
						&& Arrays.areEqual(key, this.getKey(iBucket))) {

					//return this.claimed.fastGet(iBucket);
				}
				final int nextDelta = (int) data >> _NEXT_SHIFT;
				if (_NULL_DELTA_INT != nextDelta) {
					iBucket += nextDelta;
					data = this.getHashVal(iBucket);
					continue;
				} else {
					final int curr_timestamp = _timestamp;
					if (curr_timestamp == start_timestamp)
						return false;
					start_timestamp = curr_timestamp;
					iBucket = hash & _bucketk_mask;
					data = this.getHashVal(iBucket);
					final int first_delta2 = (short) data;
					if (0 != first_delta2) {
						if (_NULL_DELTA_SHORT == first_delta2)
							return false;
						iBucket += first_delta2;
						data = this.getHashVal(iBucket);
					}
					continue;
				}
			} while (true);

		}

		public boolean update(byte[] key, long value) throws IOException {
			final int hash = hash(key);
			lock();
			try {
				// look for key in hash-map
				// .....................................
				final int i_start_bucket = hash & _bucketk_mask;
				int iBucket = i_start_bucket;
				long data = this.getHashVal(i_start_bucket);
				final short first_delta = (short) data;
				if (_NULL_DELTA_SHORT != first_delta) {
					if (0 != first_delta) {
						iBucket += first_delta;
						data = this.getHashVal(iBucket);
					}

					do {
						final int iRef;
						if (hash == (data >> _HASH_SHIFT)
								&& Arrays.areEqual(key,
										this.getKey(iRef = (iBucket)))) {
							//this.claimed.set(iBucket);
							this.setHashVal(iRef, value);
						}
						final int next_delta = (int) data >> _NEXT_SHIFT;
						if (_NULL_DELTA_INT == next_delta)
							break;
						else {
							iBucket += next_delta;
							data = this.getHashVal(iBucket);
						}
					} while (true);
				}

				// try to place the key in the same cache-line
				// ..................
				final int i_start_cacheline = i_start_bucket & _NOT_CACHE_MASK;
				final int i_end_cacheline = i_start_cacheline + _CACHE_MASK;
				int i_free_bucket = i_start_bucket;
				do {
					long free_data = this.getHashVal(i_free_bucket);
					if (_NULL_HASH == (free_data >> _HASH_SHIFT)) {
						// we found a free bucket at the cahce-line, so
						// add the new bucket to the begining of the list

						int i_ref_bucket = i_free_bucket;
						//this.claimed.set(i_free_bucket);
						this.setKeyVal(i_ref_bucket, key, value);
						free_data &= _NOT_HASH_MASK;
						free_data |= ((long) hash << _HASH_SHIFT);

						if (0 == first_delta) {

							final long start_data = this
									.getHashVal(i_start_bucket);
							final int start_next = (int) start_data >> _NEXT_SHIFT;
							if (_NULL_DELTA_INT != start_next) {
								final long new_free_next = i_start_bucket
										+ start_next - i_free_bucket;
								this.setHashVal(
										i_free_bucket,
										(free_data & _NOT_NEXT_MASK)
												| ((new_free_next << _NEXT_SHIFT) & _NEXT_MASK));
							} else
								this.setHashVal(i_free_bucket, free_data);
							final long new_start_next = i_free_bucket
									- i_start_bucket;
							this.setHashVal(
									i_start_bucket,
									(start_data & _NOT_NEXT_MASK)
											| ((new_start_next << _NEXT_SHIFT) & _NEXT_MASK));
						} else {// 0 != first_delta
							if (_NULL_DELTA_SHORT != first_delta) {
								final long new_free_next = i_start_bucket
										+ first_delta - i_free_bucket;
								free_data &= _NOT_NEXT_MASK;
								free_data |= ((new_free_next << _NEXT_SHIFT) & _NEXT_MASK);
							}
							final long start_data;
							if (i_free_bucket != i_start_bucket) {
								start_data = this.getHashVal(i_start_bucket);
								this.setHashVal(i_free_bucket, free_data);
							} else
								start_data = free_data;
							final long new_start_first = i_free_bucket
									- i_start_bucket;
							this.setHashVal(i_start_bucket,
									(start_data & _NOT_FIRST_MASK)
											| (new_start_first & _FIRST_MASK));
						}

						++_count;
						++_timestamp;
						return false;
					}

					++i_free_bucket;
					if (i_free_bucket > i_end_cacheline)
						i_free_bucket = i_start_cacheline;
				} while (i_start_bucket != i_free_bucket);

				// place key in arbitrary free forward bucket
				// ...................
				int i_max_bucket = i_start_bucket + _MAX_DELTA_BUCKET;
				if (i_max_bucket > _bucketk_mask)
					i_max_bucket = _bucketk_mask;
				i_free_bucket = i_end_cacheline + 1;

				while (i_free_bucket <= i_max_bucket) {
					long free_data = this.getHashVal(i_free_bucket);
					if (_NULL_HASH == (free_data >> _HASH_SHIFT)) {
						// we found a free bucket outside of the cahce-line, so
						// add the new bucket to the end of the list

						int i_ref_bucket = i_free_bucket;
						this.setKeyVal(i_ref_bucket, key, value);
						free_data &= _NOT_HASH_MASK;
						free_data |= ((long) hash << _HASH_SHIFT);
						this.setHashVal(i_free_bucket, free_data);

						if (_NULL_DELTA_SHORT == first_delta) {
							long new_start_first = (i_free_bucket - i_start_bucket)
									& _FIRST_MASK;
							long start_data = (this.getHashVal(i_start_bucket) & _NOT_FIRST_MASK)
									| new_start_first;
							this.setHashVal(i_start_bucket, start_data);
						} else {
							long new_last_next = ((i_free_bucket - iBucket) << _NEXT_SHIFT)
									& _NEXT_MASK;
							long last_data = (this.getHashVal(iBucket) & _NOT_NEXT_MASK)
									| new_last_next;
							this.setHashVal(iBucket, last_data);
						}

						++_count;
						++_timestamp;
						return false;
					}

					i_free_bucket += 2;
				}

				// place key in arbitrary free backward bucket
				// ...................
				int i_min_bucket = i_start_bucket - _MAX_DELTA_BUCKET;
				if (i_min_bucket < 0)
					i_min_bucket = 0;
				i_free_bucket = i_start_cacheline - 1;

				while (i_free_bucket >= i_min_bucket) {
					long free_data = this.getHashVal(i_free_bucket);
					if (_NULL_HASH == (free_data >> _HASH_SHIFT)) {
						// we found a free bucket outside of the cahce-line, so
						// add the new bucket to the end of the list

						int i_ref_bucket = i_free_bucket;
						//this.claimed.set(i_free_bucket);
						this.setKeyVal(i_ref_bucket, key, value);
						free_data &= _NOT_HASH_MASK;
						free_data |= ((long) hash << _HASH_SHIFT);
						this.setHashVal(i_free_bucket, free_data);

						if (_NULL_DELTA_SHORT == first_delta) {
							long new_start_first = (i_free_bucket - i_start_bucket)
									& _FIRST_MASK;
							long start_data = (this.getHashVal(i_start_bucket) & _NOT_FIRST_MASK)
									| new_start_first;
							this.setHashVal(i_start_bucket, start_data);
						} else {
							long new_last_next = ((i_free_bucket - iBucket) << _NEXT_SHIFT)
									& _NEXT_MASK;
							long last_data = (this.getHashVal(iBucket) & _NOT_NEXT_MASK)
									| new_last_next;
							this.setHashVal(iBucket, last_data);
						}

						++_count;
						++_timestamp;
						return false;
					}

					i_free_bucket -= 2;
				}

			} finally {
				unlock();
			}

			return false;
		}

		public long claimRecords() throws IOException {
			if (this.closed)
				throw new IOException("Hashtable " + this.path + " is close");
			long k = 0;
			try {
				this.iterInit();
				while (iterpos < capacity) {
					this.lock();
					try {
						//boolean cl = claimed.fastGet(iterpos);
						//if (cl) {
							//claimed.fastClear(iterpos);
							//this.setKeyTime(iterpos);
							//k++;
						//}
					} finally {
						iterpos++;
						this.unlock();
					}

				}
			} catch (NullPointerException e) {

			}
			return k;
		}

		public long removeNextOldRecord(long time) throws IOException {
			while (iterpos < capacity) {
				long val = -1;
				this.lock();
				try {
					if (this.getHashVal(iterpos) != HSByteArrayLongMap._NULL_HASH_DELTA) {

						long tm = this.getKeyTime(iterpos);
						if (tm < time) {
							//boolean cl = claimed.get(iterpos);
							//if (!cl) {
							//	byte[] key = this.getKey(iterpos);
							//	val = this.getKeyValue(iterpos);
							//	this.remove(key);
							//	return val;
							//}
						}
					}
				} finally {
					iterpos++;
					this.unlock();
				}
			}
			return -1;
		}
	}

	// fields --------------------------------------
	int _segment_shift;
	int _segment_mask;
	Segment[] _segments;
	private boolean closed = true;
	private boolean firstGCRun = true;
	private SyncThread st = null;
	private OpenBitSet freeSlots = null;
	private SDFSEvent loadEvent = SDFSEvent.loadHashDBEvent(
			"Loading Hash Database", Main.mountEvent);
	private long endPos = 0;
	private long size = 0;
	private String fileName;
	private String origFileName;
	long ram = 0;

	// small utilities -----------------------------

	private static int nearestPowerOfTwo(long value) {
		int rc = 1;
		while (rc < value) {
			rc <<= 1;
		}
		return rc;
	}

	/*
	 * private static final int hash(int h) { // Spread bits to regularize both
	 * segment and index locations, // using variant of single-word Wang/Jenkins
	 * hash. h += (h << 15) ^ 0xffffcd7d; h ^= (h >>> 10); h += (h << 3); h ^=
	 * (h >>> 6); h += (h << 2) + (h << 14); return h ^ (h >>> 16); }
	 */

	private final Segment segmentFor(int hash) {
		return _segments[(hash >>> _segment_shift) & _segment_mask];
		// return _segments[(hash >>> 8) & _segment_mask];
		// return _segments[hash & _segment_mask];
	}

	// public operations ---------------------------

	public HSByteArrayLongMap(final long initialCapacity, String path)
			throws IOException, HashtableFullException {
		// check for the validity of the algorithems

		this.init(initialCapacity, path);
	}

	public HSByteArrayLongMap() {

	}

	public boolean isEmpty() {
		final Segment[] segments = this._segments;
		/*
		 * We keep track of per-segment "timestamp" to avoid ABA problems in
		 * which an element in one segment was added and in another removed
		 * during traversal, in which case the table was never actually empty at
		 * any point. Note the similar use of "timestamp" in the size() and
		 * containsValue() methods, which are the only other methods also
		 * susceptible to ABA problems.
		 */
		int[] mc = new int[segments.length];
		int mcsum = 0;
		for (int i = 0; i < segments.length; ++i) {
			if (0 != segments[i]._count)
				return false;
			else
				mcsum += mc[i] = segments[i]._timestamp;
		}
		// If mcsum happens to be zero, then we know we got a snapshot
		// before any modifications at all were made. This is
		// probably common enough to bother tracking.
		if (mcsum != 0) {
			for (int i = 0; i < segments.length; ++i) {
				if (0 != segments[i]._count || mc[i] != segments[i]._timestamp)
					return false;
			}
		}
		return true;
	}

	public long claimRecords() throws IOException {
		long claims = 0;
		for (Segment seg : _segments) {
			claims = claims + seg.claimRecords();
		}
		return claims;
	}

	@Override
	public long getSize() {
		final Segment[] segments = this._segments;
		long sum = 0;
		long check = 0;
		int[] mc = new int[segments.length];

		// Try a few times to get accurate count. On failure due to
		// continuous async changes in table, resort to locking.
		for (int iTry = 0; iTry < _RETRIES_BEFORE_LOCK; ++iTry) {
			check = 0;
			sum = 0;
			int mcsum = 0;
			for (int i = 0; i < segments.length; ++i) {
				sum += segments[i]._count;
				mcsum += mc[i] = segments[i]._timestamp;
			}
			if (mcsum != 0) {
				for (int i = 0; i < segments.length; ++i) {
					check += segments[i]._count;
					if (mc[i] != segments[i]._timestamp) {
						check = -1; // force retry
						break;
					}
				}
			}
			if (check == sum)
				break;
		}

		if (check != sum) { // Resort to locking all segments
			sum = 0;
			for (int i = 0; i < segments.length; ++i)
				segments[i].lock();
			for (int i = 0; i < segments.length; ++i)
				sum += segments[i]._count;
			for (int i = 0; i < segments.length; ++i)
				segments[i].unlock();
		}

		return sum;
	}

	private Segment getMap(byte[] key) {
		final int hash = hash(key);
		return segmentFor(hash);
	}

	private boolean isClaimed(ChunkData cm) throws KeyNotFoundException,
			IOException {
		return this.getMap(cm.getHash()).isClaimed(cm.getHash());
	}

	// contains

	public boolean containsKey(final byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		final int hash = hash(key);
		return segmentFor(hash).containsKey(key);
	}

	public long get(final byte[] key) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		final int hash = hash(key);
		return segmentFor(hash).get(key, true);
	}

	// add
	public boolean put(byte[] key, long value) throws Exception {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		if (value == -1)
			throw new NullPointerException();
		final int hash = hash(key);
		return segmentFor(hash).put(key, value);
	}

	// remove
	public boolean remove(final byte[] key) throws Exception {
		final int hash = hash(key);
		return segmentFor(hash).remove(key);
	}

	private static int hash(byte[] key) {
		ByteBuffer b = ByteBuffer.wrap(key);
		b.position(key.length - 4);
		return b.getInt();
	}

	// general
	public void clear() {
	}

	public static void main(String[] args) throws Exception {
		Volume vol = new Volume();
		vol.setName("bob");
		System.out.println(HSByteArrayLongMap._NULL_DELTA_FIRST_LONG);
		System.out.println(HSByteArrayLongMap._NULL_DELTA_NEXT_LONG);
		System.out.println(HSByteArrayLongMap._FIRST_MASK);
		System.out.println(HSByteArrayLongMap._NEXT_MASK);
		
		Main.volume = vol;
		long data = 555555;
		data &= _NOT_HASH_MASK;
		System.out.println("data = " + data + " " + _NOT_HASH_MASK);
		HSByteArrayLongMap map = new HSByteArrayLongMap(268435456, "/tmp/ninja");
		Random r = new Random();
		ArrayList<KVPair> al = new ArrayList<KVPair>();
		int rndl = 64;
		byte[] b = new byte[rndl];
		long st = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			r.nextBytes(b);
			byte[] key = MurmurHash3.murmur128(b, 6442);
			long val = r.nextLong();
			if (val < 0)
				val = val * -1;
			KVPair p = new KVPair();
			p.key = key;
			p.val = val;
			al.add(p);
			map.put(key, val);
		}
		long et = System.currentTimeMillis() - st;
		double mbs = (((double) al.size() / (double) (et / 1000)) * 4096)
				/ (1024 * 1024);
		System.out.println("Took " + et + " to insert " + al.size() + " mb/s="
				+ mbs);
		st = System.currentTimeMillis();
		long sz = map.claimRecords();
		et = System.currentTimeMillis() - st;
		System.out.println("Took " + et + " to claim " + sz);
		st = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			r.nextBytes(b);
			byte[] key = MurmurHash3.murmur128(b, 6442);
			long val = r.nextLong();
			if (val < 0)
				val = val * -1;
			KVPair p = new KVPair();
			p.key = key;
			p.val = val;
			al.add(p);
			map.put(key, val);
		}
		et = System.currentTimeMillis() - st;
		mbs = (((double) al.size() / (double) (et / 1000)) * 4096)
				/ (1024 * 1024);
		System.out.println("Took " + et + " to insert " + al.size() + " mb/s="
				+ mbs);
		st = System.currentTimeMillis();
		int i = 0;
		for (KVPair p : al) {

			long val = map.get(p.key);
			if (val != p.val) {
				System.out.println("eeks! val " + val + " != " + p.val
						+ " keyn " + i);
				System.exit(0);
			}
			i++;
		}
		et = System.currentTimeMillis() - st;
		mbs = (((double) al.size() / (double) (et / 1000)) * 4096)
				/ (1024 * 1024);
		System.out.println("Took " + et + " to get " + al.size() + " mb/s="
				+ mbs);

		st = System.currentTimeMillis();
		sz = map.claimRecords();
		et = System.currentTimeMillis() - st;
		System.out.println("Took " + et + " to claim " + sz);
		System.out.println("Size is " + map.getSize());
		map.close();
		
		/*
		 * st = System.currentTimeMillis(); for(KVPair p : al) {
		 * 
		 * map.remove(p.key); i++; } et = System.currentTimeMillis() - st;
		 * System.out.println("Took " + et + " to remove " + al.size());
		 * 
		 * st = System.currentTimeMillis(); sz = map.claimRecords(); et =
		 * System.currentTimeMillis() - st; System.out.println("Took " + et +
		 * " to claim " + sz);
		 */
		

	}

	private static class KVPair {
		byte[] key;
		long val;
	}

	@Override
	public long endStartingPosition() {
		return this.endPos;
	}

	@Override
	public long getAllocatedRam() {
		return this.ram;
	}

	@Override
	public long getUsedSize() {
		return this.getSize() * Main.CHUNK_LENGTH;
	}

	@Override
	public long getMaxSize() {
		return this.size;
	}

	private void addFreeSlot(long position) {
		this.lock();
		try {
			int pos = (int) (position / Main.CHUNK_LENGTH);
			if (pos >= 0)
				this.freeSlots.set(pos);
			else if (pos < 0) {
				SDFSLogger.getLog().info("Position is less than 0 " + pos);
			}
		} finally {
			this.unlock();
		}
	}

	protected long getFreeSlot() {
		this.lock();
		try {
			int slot = this.freeSlots.nextSetBit(0);
			if (slot != -1) {
				this.freeSlots.clear(slot);
				return ((long) slot * (long) Main.CHUNK_LENGTH);
			} else
				return slot;
		} finally {
			this.unlock();
		}
	}

	@Override
	public void claimRecords(SDFSEvent evt) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		SDFSLogger.getLog().info("claiming records");
		SDFSEvent tEvt = SDFSEvent.claimInfoEvent(
				"Claiming Records [" + this.getSize() + "] from ["
						+ this.fileName + "]", evt);
		tEvt.maxCt = this._segments.length;
		long claims = 0;
		for (int i = 0; i < this._segments.length; i++) {
			tEvt.curCt++;
			try {
				this._segments[i].iterInit();
				claims = claims + this._segments[i].claimRecords();
			} catch (Exception e) {
				tEvt.endEvent("Unable to claim records for " + i
						+ " because : [" + e.toString() + "]", SDFSEvent.ERROR);
				SDFSLogger.getLog()
						.error("Unable to claim records for " + i, e);
				throw new IOException(e);
			}
		}
		tEvt.endEvent("claimed [" + claims + "] records");
		SDFSLogger.getLog().info("claimed [" + claims + "] records");

	}

	@Override
	public long getFreeBlocks() {
		return this.freeSlots.cardinality();
	}

	@Override
	public boolean put(ChunkData cm) throws IOException, HashtableFullException {
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName
					+ " is close");
		// this.flushFullBuffer();
		boolean added = false;
		added = this.put(cm, true);
		return added;
	}

	@Override
	public boolean put(ChunkData cm, boolean persist) throws IOException,
			HashtableFullException {
		// persist = false;
		if (this.isClosed())
			throw new HashtableFullException("Hashtable " + this.fileName
					+ " is close");
		boolean added = false;
		// if (persist)
		// this.flushFullBuffer();
		if (persist) {
			if (!cm.recoverd) {
				cm.setcPos(this.getFreeSlot());
				cm.persistData(true);
			}
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos());
			if (added) {

			} else {
				this.addFreeSlot(cm.getcPos());
				cm = null;
			}
		} else {
			added = this.getMap(cm.getHash()).put(cm.getHash(), cm.getcPos());
		}
		return added;
	}

	@Override
	public boolean update(ChunkData cm) throws IOException {
		try {
			boolean added = false;
			if (!this.isClaimed(cm)) {
				cm.persistData(true);
				added = this.getMap(cm.getHash()).update(cm.getHash(),
						cm.getcPos());
			}
			if (added) {
				// this.compactKsz++;
			}
			return added;
		} catch (KeyNotFoundException e) {
			return false;
		} finally {
			// this.arlock.unlock();

		}
	}

	@Override
	public byte[] getData(byte[] key) throws IOException {
		if (this.isClosed())
			throw new IOException("Hashtable " + this.fileName + " is close");
		long ps = this.get(key);
		if (ps != -1) {
			return ChunkData.getChunk(key, ps);
		} else {
			SDFSLogger.getLog().info(
					"found no data for key [" + StringUtils.getHexString(key)
							+ "]");
			return null;
		}
	}

	@Override
	public boolean remove(ChunkData cm) throws IOException {
		if (this.isClosed()) {
			throw new IOException("hashtable [" + this.fileName + "] is close");
		}
		try {
			if (cm.getHash().length == 0)
				return true;
			if (!this.getMap(cm.getHash()).remove(cm.getHash())) {
				return false;
			} else {
				cm.setmDelete(true);
				if (this.isClosed()) {
					throw new IOException("hashtable [" + this.fileName
							+ "] is close");
				}
				try {
					this.addFreeSlot(cm.getcPos());
				} catch (Exception e) {
				}

				return true;
			}
		} catch (Exception e) {
			SDFSLogger.getLog().fatal("error getting record", e);
			return false;
		}
	}

	@Override
	public long removeRecords(long time, boolean forceRun, SDFSEvent evt)
			throws IOException {
		SDFSLogger.getLog().info(
				"Garbage collection starting for records older than "
						+ new Date(time));
		SDFSEvent tEvt = SDFSEvent
				.claimInfoEvent(
						"Garbage collection starting for records older than "
								+ new Date(time) + " from [" + this.fileName
								+ "]", evt);
		tEvt.maxCt = this._segments.length;
		long rem = 0;
		if (forceRun)
			this.firstGCRun = false;
		if (this.firstGCRun) {
			this.firstGCRun = false;
			tEvt.endEvent(
					"Garbage collection aborted because it is the first run",
					SDFSEvent.WARN);
			throw new IOException(
					"Garbage collection aborted because it is the first run");

		} else {
			if (this.isClosed())
				throw new IOException("Hashtable " + this.fileName
						+ " is close");
			for (int i = 0; i < _segments.length; i++) {
				tEvt.curCt++;
				if (_segments[i] != null) {
					_segments[i].iterInit();
					long fPos = _segments[i].removeNextOldRecord(time);
					while (fPos != -1) {
						this.addFreeSlot(fPos);
						rem++;
						fPos = _segments[i].removeNextOldRecord(time);
					}
				}
			}
		}
		tEvt.endEvent("Removed [" + rem + "] records. Free slots ["
				+ this.freeSlots.cardinality() + "]");
		SDFSLogger.getLog().info(
				"Removed [" + rem + "] records. Free slots ["
						+ this.freeSlots.cardinality() + "]");
		return rem;
	}

	@Override
	public void initCompact() throws IOException {
		this.close();
		this.origFileName = fileName;
		String parent = new File(this.fileName).getParentFile().getPath();
		String fname = new File(this.fileName).getName();
		this.fileName = parent + ".compact" + File.separator + fname;
		File f = new File(this.fileName).getParentFile();
		if (f.exists()) {
			FileUtils.deleteDirectory(f);
		}
		FileUtils
				.copyDirectory(new File(parent), new File(parent + ".compact"));

		try {
			this.init(size, this.fileName);
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	@Override
	public void commitCompact(boolean force) throws IOException {
		this.freeSlots.clear(0, this.freeSlots.size());
		this.close();
		FileUtils.deleteDirectory(new File(this.origFileName).getParentFile());
		SDFSLogger.getLog().info(
				"Deleted " + new File(this.origFileName).getParent());
		new File(this.fileName).getParentFile().renameTo(
				new File(this.origFileName).getParentFile());
		SDFSLogger.getLog().info(
				"moved " + new File(this.fileName).getParent() + " to "
						+ new File(this.origFileName).getParent());
		FileUtils.deleteDirectory(new File(this.fileName).getParentFile());
		SDFSLogger.getLog().info(
				"deleted " + new File(this.fileName).getParent());

	}

	@Override
	public void rollbackCompact() throws IOException {
		FileUtils.deleteDirectory(new File(this.fileName));

	}

	@Override
	public void init(long initialCapacity, String path) throws IOException,
			HashtableFullException {
		this.lock();
		try {
			this.size = initialCapacity;
			this.fileName = path;
			File _fs = new File(path);
			if (!_fs.getParentFile().exists()) {
				_fs.getParentFile().mkdirs();
			}
			SDFSLogger.getLog().info("Loading freebits bitset");
			File bsf = new File(path + "ofreebit.map");
			if (!bsf.exists()) {
				SDFSLogger.getLog().debug("Looks like a new HashStore");
				this.freeSlots = new OpenBitSet();
			} else {
				SDFSLogger.getLog().info(
						"Loading freeslots from " + bsf.getPath());
				try {

					this.freeSlots = OpenBitSetSerialize.readIn(bsf.getPath());
					bsf.delete();
				} catch (Exception e) {
					SDFSLogger.getLog().error(
							"Unable to load bitset from " + bsf.getPath(), e);
				}
				SDFSLogger.getLog().info(
						"Loaded [" + this.freeSlots.cardinality()
								+ "] free slots");
			}
			System.out.println("Loading Hashtable Entries");
			int concurrencyLevel = 128;
			if (initialCapacity < 0 || concurrencyLevel <= 0 /*
															 * ||
															 * machineCachelineSize
															 * <= 0
															 */)
				throw new IllegalArgumentException();

			// set the user preference, should we force cache-line alignment
			// _is_cacheline_alignment = isCachelineAlignment;

			// calculate cache-line mask
			// final int bucketSize = Math.max(8, 2*machinePointerSize);
			// _cache_mask = ( (machineCachelineSize / bucketSize) - 1 );

			// allocate the segments array
			final int numSegments = nearestPowerOfTwo(concurrencyLevel);
			System.out.println("Num Segments = " + numSegments);
			_segment_mask = (numSegments - 1);
			_segments = Segment.newArray(numSegments);

			// Find power-of-two sizes best matching arguments
			int sshift = 0;
			int ssize = 1;
			while (ssize < numSegments) {
				++sshift;
				ssize <<= 1;
			}
			_segment_shift = 32 - sshift;

			// initialize the segmens
			final long initCapacity = nearestPowerOfTwo(initialCapacity);
			final int segmentCapacity = (int) (initCapacity / numSegments);
			CommandLineProgressBar bar = new CommandLineProgressBar(
					"Loading Hashes", numSegments, System.out);
			this.loadEvent.maxCt = numSegments;
			long rsz = 0;
			for (int iSeg = 0; iSeg < numSegments; ++iSeg) {
				_segments[iSeg] = new Segment(segmentCapacity, path + iSeg);
				if (_segments[iSeg].getBigestKey() > this.endPos)
					this.endPos = _segments[iSeg].getBigestKey();
				rsz = rsz + _segments[iSeg]._count;
				this.loadEvent.curCt = iSeg;
				bar.update(iSeg);
			}
			this.loadEvent.endEvent("Loaded entries " + rsz);
			System.out.println("Loaded entries " + rsz);
			SDFSLogger.getLog().info("Loaded entries " + rsz);
			bar.finish();
			this.closed = false;
			// st = new SyncThread(this);
		} finally {
			this.unlock();
		}

	}

	@Override
	public boolean isClosed() {
		this.lock();
		try {
			return this.closed;
		} finally {
			this.unlock();
		}
	}

	@Override
	public synchronized void sync() throws IOException {
		this.lock();
		try {
			for (Segment seg : _segments) {
				seg.sync();
			}
		} finally {
			this.unlock();
		}

	}

	@Override
	public void vanish() throws IOException {
		for (Segment seg : _segments) {
			seg.clear();
		}

	}

	@Override
	public void close() {
		try {
			st.close();
		} catch (Exception e) {
		}
		this.lock();
		this.closed = true;
		try {
			for (Segment seg : _segments) {
				seg.close();
			}
			try {
				File outFile = new File(this.fileName + "ofreebit.map");
				OpenBitSetSerialize.writeOut(outFile.getPath(), this.freeSlots);
				this.freeSlots.clear(0, this.freeSlots.size());
			} catch (Exception e) {
				SDFSLogger.getLog().error(
						"unable to serialize free bits bitmap " + this.fileName
								+ "ofreebit.map", e);
			}
		} finally {
			this.unlock();
		}

	}
}
