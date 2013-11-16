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

import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentHopscotchHashMap<K, V> {

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

	// inner classes -------------------------------

	static final class Segment<K, V> extends ReentrantLock {

		/**
		 * 
		 */
		private static final long serialVersionUID = 4476725515973126431L;
		volatile int _timestamp;
		int _bucketk_mask;
		long[] _table_hash_delta;
		Object[] _table_key_value;
		// AtomicInteger _lock;
		int _count;

		public Segment(final int initialCapacity) {
			init(initialCapacity);
		}

		private void init(final int initialCapacity) {
			// _lock = new AtomicInteger();
			// _lock.set(0);
			_timestamp = 0;
			_bucketk_mask = initialCapacity - 1;
			_count = 0;

			_table_hash_delta = new long[initialCapacity];
			_table_key_value = new Object[initialCapacity << 1];

			// create the blocks of buckets
			for (int iData = 0; iData < initialCapacity; ++iData) {
				_table_hash_delta[iData] = _NULL_HASH_DELTA;
			}
		}

		/*
		 * void lock() { while(!_lock.compareAndSet(0, 0xFFFF)) { } }
		 * 
		 * void unlock() { _lock.set(0); }
		 */

		@SuppressWarnings("unchecked")
		static final <K, V> Segment<K, V>[] newArray(final int numSegments) {
			return new Segment[numSegments];
		}

		boolean containsKey(final K key, final int hash) {
			// go over the list and look for key
			int start_timestamp = _timestamp;
			int iBucket = hash & _bucketk_mask;
			long data = _table_hash_delta[iBucket];
			final int first_delta = (short) data;
			if (0 != first_delta) {
				if (_NULL_DELTA_SHORT == first_delta)
					return false;
				iBucket += first_delta;
				data = _table_hash_delta[iBucket];
			}

			do {
				if (hash == (data >> _HASH_SHIFT)
						&& key.equals(_table_key_value[iBucket << 1]))
					return true;
				final int nextDelta = (int) data >> _NEXT_SHIFT;
				if (_NULL_DELTA_INT != nextDelta) {
					iBucket += nextDelta;
					data = _table_hash_delta[iBucket];
					continue;
				} else {
					final int curr_timestamp = _timestamp;
					if (curr_timestamp == start_timestamp)
						return false;
					start_timestamp = curr_timestamp;
					iBucket = hash & _bucketk_mask;
					data = _table_hash_delta[iBucket];
					final int first_delta2 = (short) data;
					if (0 != first_delta2) {
						if (_NULL_DELTA_SHORT == first_delta2)
							return false;
						iBucket += first_delta2;
						data = _table_hash_delta[iBucket];
					}
					continue;
				}
			} while (true);
		}

		@SuppressWarnings("unchecked")
		V get(final K key, final int hash) {
			// go over the list and look for key
			int start_timestamp = 0;
			int iBucket = 0;
			long data = 0;

			boolean is_need_init = true;
			do {
				if (is_need_init) {
					is_need_init = false;
					start_timestamp = _timestamp;
					iBucket = hash & _bucketk_mask;
					data = _table_hash_delta[iBucket];
					final int first_delta = (short) data;
					if (0 != first_delta) {
						if (_NULL_DELTA_SHORT == first_delta)
							return null;
						iBucket += first_delta;
						data = _table_hash_delta[iBucket];
					}
				}

				final int iRef;
				if (hash == (data >> _HASH_SHIFT)
						&& key.equals(_table_key_value[iRef = iBucket << 1])) {
					final V value = (V) _table_key_value[iRef + 1];
					if (_timestamp == start_timestamp)
						return value;
					is_need_init = true;
					continue;
				}
				final int nextDelta = (int) data >> _NEXT_SHIFT;
				if (_NULL_DELTA_INT != nextDelta) {
					iBucket += nextDelta;
					data = _table_hash_delta[iBucket];
					continue;
				} else {
					if (_timestamp == start_timestamp)
						return null;
					is_need_init = true;
					continue;
				}
			} while (true);
		}

		@SuppressWarnings("unchecked")
		V put(final K key, final int hash, final V value) {
			lock();
			try {
				// look for key in hash-map
				// .....................................
				final int i_start_bucket = hash & _bucketk_mask;
				int iBucket = i_start_bucket;
				long data = _table_hash_delta[i_start_bucket];
				final short first_delta = (short) data;
				if (_NULL_DELTA_SHORT != first_delta) {
					if (0 != first_delta) {
						iBucket += first_delta;
						data = _table_hash_delta[iBucket];
					}

					do {
						final int iRef;
						if (hash == (data >> _HASH_SHIFT)
								&& key.equals(_table_key_value[iRef = (iBucket << 1)]))
							return (V) _table_key_value[iRef + 1];
						final int next_delta = (int) data >> _NEXT_SHIFT;
						if (_NULL_DELTA_INT == next_delta)
							break;
						else {
							iBucket += next_delta;
							data = _table_hash_delta[iBucket];
						}
					} while (true);
				}

				// try to place the key in the same cache-line
				// ..................
				final int i_start_cacheline = i_start_bucket & _NOT_CACHE_MASK;
				final int i_end_cacheline = i_start_cacheline + _CACHE_MASK;
				int i_free_bucket = i_start_bucket;
				do {
					long free_data = _table_hash_delta[i_free_bucket];
					if (_NULL_HASH == (free_data >> _HASH_SHIFT)) {
						// we found a free bucket at the cahce-line, so
						// add the new bucket to the begining of the list

						int i_ref_bucket = i_free_bucket << 1;
						_table_key_value[i_ref_bucket] = key;
						_table_key_value[++i_ref_bucket] = value;
						free_data &= _NOT_HASH_MASK;
						free_data |= ((long) hash << _HASH_SHIFT);

						if (0 == first_delta) {
							final long start_data = _table_hash_delta[i_start_bucket];
							final int start_next = (int) start_data >> _NEXT_SHIFT;
							if (_NULL_DELTA_INT != start_next) {
								final long new_free_next = i_start_bucket
										+ start_next - i_free_bucket;
								_table_hash_delta[i_free_bucket] = (free_data & _NOT_NEXT_MASK)
										| ((new_free_next << _NEXT_SHIFT) & _NEXT_MASK);
							} else
								_table_hash_delta[i_free_bucket] = free_data;
							final long new_start_next = i_free_bucket
									- i_start_bucket;
							_table_hash_delta[i_start_bucket] = (start_data & _NOT_NEXT_MASK)
									| ((new_start_next << _NEXT_SHIFT) & _NEXT_MASK);
						} else {// 0 != first_delta
							if (_NULL_DELTA_SHORT != first_delta) {
								final long new_free_next = i_start_bucket
										+ first_delta - i_free_bucket;
								free_data &= _NOT_NEXT_MASK;
								free_data |= ((new_free_next << _NEXT_SHIFT) & _NEXT_MASK);
							}
							final long start_data;
							if (i_free_bucket != i_start_bucket) {
								start_data = _table_hash_delta[i_start_bucket];
								_table_hash_delta[i_free_bucket] = free_data;
							} else
								start_data = free_data;
							final long new_start_first = i_free_bucket
									- i_start_bucket;
							_table_hash_delta[i_start_bucket] = (start_data & _NOT_FIRST_MASK)
									| (new_start_first & _FIRST_MASK);
						}

						++_count;
						++_timestamp;
						return null;
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
					long free_data = _table_hash_delta[i_free_bucket];
					if (_NULL_HASH == (free_data >> _HASH_SHIFT)) {
						// we found a free bucket outside of the cahce-line, so
						// add the new bucket to the end of the list

						int i_ref_bucket = i_free_bucket << 1;
						_table_key_value[i_ref_bucket] = key;
						_table_key_value[++i_ref_bucket] = value;
						free_data &= _NOT_HASH_MASK;
						free_data |= ((long) hash << _HASH_SHIFT);
						_table_hash_delta[i_free_bucket] = free_data;

						if (_NULL_DELTA_SHORT == first_delta) {
							long new_start_first = (i_free_bucket - i_start_bucket)
									& _FIRST_MASK;
							long start_data = (_table_hash_delta[i_start_bucket] & _NOT_FIRST_MASK)
									| new_start_first;
							_table_hash_delta[i_start_bucket] = start_data;
						} else {
							long new_last_next = ((i_free_bucket - iBucket) << _NEXT_SHIFT)
									& _NEXT_MASK;
							long last_data = (_table_hash_delta[iBucket] & _NOT_NEXT_MASK)
									| new_last_next;
							_table_hash_delta[iBucket] = last_data;
						}

						++_count;
						++_timestamp;
						return null;
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
					long free_data = _table_hash_delta[i_free_bucket];
					if (_NULL_HASH == (free_data >> _HASH_SHIFT)) {
						// we found a free bucket outside of the cahce-line, so
						// add the new bucket to the end of the list

						int i_ref_bucket = i_free_bucket << 1;
						_table_key_value[i_ref_bucket] = key;
						_table_key_value[++i_ref_bucket] = value;
						free_data &= _NOT_HASH_MASK;
						free_data |= ((long) hash << _HASH_SHIFT);
						_table_hash_delta[i_free_bucket] = free_data;

						if (_NULL_DELTA_SHORT == first_delta) {
							long new_start_first = (i_free_bucket - i_start_bucket)
									& _FIRST_MASK;
							long start_data = (_table_hash_delta[i_start_bucket] & _NOT_FIRST_MASK)
									| new_start_first;
							_table_hash_delta[i_start_bucket] = start_data;
						} else {
							long new_last_next = ((i_free_bucket - iBucket) << _NEXT_SHIFT)
									& _NEXT_MASK;
							long last_data = (_table_hash_delta[iBucket] & _NOT_NEXT_MASK)
									| new_last_next;
							_table_hash_delta[iBucket] = last_data;
						}

						++_count;
						++_timestamp;
						return null;
					}

					i_free_bucket -= 2;
				}

			} finally {
				unlock();
			}

			return null;
		}

		private void optimize_cacheline_use(final int i_free_bucket) {
			final int i_start_cacheline = i_free_bucket & _NOT_CACHE_MASK;
			final int i_end_cacheline = i_start_cacheline + _CACHE_MASK;

			// go over the buckets that reside in the cacheline of the free
			// bucket
			for (int i_cacheline = i_start_cacheline; i_cacheline <= i_end_cacheline; ++i_cacheline) {

				// check if current bucket has keys
				final long data = _table_hash_delta[i_cacheline];
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
							final int i_key_value = i_free_bucket << 1;
							final int i_rel_key_value = i_relocate << 1;
							_table_key_value[i_key_value] = _table_key_value[i_rel_key_value];
							_table_key_value[i_key_value + 1] = _table_key_value[i_rel_key_value + 1];
							long relocate_data = _table_hash_delta[i_relocate];
							long free_data = _table_hash_delta[i_free_bucket];
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
							_table_hash_delta[i_free_bucket] = free_data;

							// update the "first" or "next" field of the last
							if (_NULL_INDX == last_i_relocate) {
								long start_data = _table_hash_delta[i_cacheline]
										& _NOT_FIRST_MASK;
								start_data |= ((i_free_bucket - i_cacheline) & 0xFFFFL);
								_table_hash_delta[i_cacheline] = start_data;
							} else {
								long last_data = _table_hash_delta[last_i_relocate]
										& _NOT_NEXT_MASK;
								last_data |= (((i_free_bucket - last_i_relocate) & 0xFFFFL) << 16);
								_table_hash_delta[last_i_relocate] = last_data;
							}

							//
							++_timestamp;
							relocate_data &= _NOT_HASH_MASK;// hash=null
							relocate_data &= _NOT_NEXT_MASK;
							relocate_data |= _NULL_DELTA_NEXT_LONG;// next =
																	// null
							_table_hash_delta[i_relocate] = relocate_data;
							_table_key_value[i_rel_key_value] = null;// key=null
							_table_key_value[i_rel_key_value + 1] = null;// value=null
							return;
						}

						final long relocate_data = _table_hash_delta[i_relocate];
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

		@SuppressWarnings("unchecked")
		V remove(final K key, final int hash) {
			lock();
			try {
				// go over the list and look for key
				final int i_start_bucket = hash & _bucketk_mask;
				int iBucket = i_start_bucket;
				long data = _table_hash_delta[iBucket];
				final short first_delta = (short) data;
				if (0 != first_delta) {
					if (_NULL_DELTA_SHORT == first_delta)
						return null;
					iBucket += first_delta;
					data = _table_hash_delta[iBucket];
				}

				int i_last_bucket = -1;
				do {
					final int iRef;
					if (hash == (data >> _HASH_SHIFT)
							&& key.equals(_table_key_value[iRef = (iBucket << 1)])) {

						data &= _NOT_HASH_MASK;
						final int next_delta = (int) data >> _NEXT_SHIFT;
						_table_hash_delta[iBucket] = data; // hash = null
						_table_key_value[iRef] = null; // key = null;

						final int iRef2 = iRef + 1;
						final V key_value = (V) _table_key_value[iRef2];
						_table_key_value[iRef2] = null; // value = null;

						if (-1 == i_last_bucket) {
							long start_data = _table_hash_delta[i_start_bucket]
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
								_table_hash_delta[i_start_bucket] = start_data;
								// return key_value;
							} else
								_table_hash_delta[i_start_bucket] = start_data;
						} else {
							long last_data = _table_hash_delta[i_last_bucket];
							final int last_next_delta = (int) last_data >> _NEXT_SHIFT;
							last_data &= _NOT_NEXT_MASK;
							if (_NULL_DELTA_INT == next_delta) {
								last_data |= _NULL_DELTA_NEXT_LONG;
							} else {
								final long new_next = ((last_next_delta + next_delta) & 0xFFFFL) << 16;
								last_data |= new_next;
							}
							_table_hash_delta[i_last_bucket] = last_data;
						}

						if (i_start_bucket != iBucket) {
							--_count;
							++_timestamp;
							data &= _NOT_NEXT_MASK;
							data |= _NULL_DELTA_NEXT_LONG;
							_table_hash_delta[iBucket] = data; // next = null
						}

						optimize_cacheline_use(iBucket);

						return key_value;
					}
					final int nextDelta = (int) data >> _NEXT_SHIFT;
					if (_NULL_DELTA_INT != nextDelta) {
						i_last_bucket = iBucket;
						iBucket += nextDelta;
						data = _table_hash_delta[iBucket];
						continue;
					} else
						return null;
				} while (true);

			} finally {
				unlock();
			}
		}

		void clear() {
		}
	}

	// fields --------------------------------------
	final int _segment_shift;
	final int _segment_mask;
	final Segment<K, V>[] _segments;

	// small utilities -----------------------------

	private static int nearestPowerOfTwo(long value) {
		int rc = 1;
		while (rc < value) {
			rc <<= 1;
		}
		return rc;
	}

	private static final int hash(int h) {
		// Spread bits to regularize both segment and index locations,
		// using variant of single-word Wang/Jenkins hash.
		h += (h << 15) ^ 0xffffcd7d;
		h ^= (h >>> 10);
		h += (h << 3);
		h ^= (h >>> 6);
		h += (h << 2) + (h << 14);
		return h ^ (h >>> 16);
	}

	private final Segment<K, V> segmentFor(int hash) {
		return _segments[(hash >>> _segment_shift) & _segment_mask];
		// return _segments[(hash >>> 8) & _segment_mask];
		// return _segments[hash & _segment_mask];
	}

	// public operations ---------------------------

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ConcurrentHopscotchHashMap(final long initialCapacity,
			final int concurrencyLevel) {
		// check for the validity of the algorithems
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
		_segment_mask = (numSegments - 1);
		_segments = Segment.<K, V> newArray(numSegments);

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
		for (int iSeg = 0; iSeg < numSegments; ++iSeg) {
			_segments[iSeg] = new Segment(segmentCapacity);
		}
	}

	public boolean isEmpty() {
		final Segment<K, V>[] segments = this._segments;
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

	public int size() {
		final Segment<K, V>[] segments = this._segments;
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
		if (sum > Integer.MAX_VALUE)
			return Integer.MAX_VALUE;
		else
			return (int) sum;
	}

	// contains

	public boolean containsKey(final K key) {
		final int hash = hash(key.hashCode());
		return segmentFor(hash).containsKey(key, hash);
	}

	public V get(final K key) {
		final int hash = hash(key.hashCode());
		return segmentFor(hash).get(key, hash);
	}

	// add
	public V put(K key, V value) {
		if (value == null)
			throw new NullPointerException();
		final int hash = hash(key.hashCode());
		return segmentFor(hash).put(key, hash, value);
	}

	// remove
	public V remove(final K key) {
		final int hash = hash(key.hashCode());
		return segmentFor(hash).remove(key, hash);
	}

	// general
	public void clear() {
	}

}
