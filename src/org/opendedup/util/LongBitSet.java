package org.opendedup.util;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class LongBitSet implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/** Number of bits allocated to a value in an index */
	private static final int VALUE_BITS = 32; // 1M values per bit set
	private static final int INDEX_BITS = 64 - VALUE_BITS; // 1M values per bit
															// set
	/** Mask for extracting values */
	private static final long VALUE_MASK = Long.MAX_VALUE << VALUE_BITS;
	private static final long INDEX_MASK = Long.MAX_VALUE >> INDEX_BITS;
	/**
	 * Map from a value stored in high bits of a long index to a bit set mapped
	 * to the lower bits of an index. Bit sets size should be balanced - not to
	 * long (otherwise setting a single bit may waste megabytes of memory) but
	 * not too short (otherwise this map will get too big). Update value of
	 * {@code VALUE_BITS} for your needs. In most cases it is ok to keep 1M -
	 * 64M values in a bit set, so each bit set will occupy 128Kb - 8Mb.
	 */
	private Map<Long, BitSet> m_sets = new HashMap<Long, BitSet>();

	/**
	 * Get set index by long index (extract bits 20-63)
	 * 
	 * @param index
	 *            Long index
	 * @return Index of a bit set in the inner map
	 */
	private long getSetIndex(final long index) {
		return index & INDEX_MASK;
	}

	/**
	 * Get index of a value in a bit set (bits 0-19)
	 * 
	 * @param index
	 *            Long index
	 * @return Index of a value in a bit set
	 */
	private int getPos(final long index) {
		return (int) (index & VALUE_MASK);
	}

	/**
	 * Helper method to get (or create, if necessary) a bit set for a given long
	 * index
	 * 
	 * @param index
	 *            Long index
	 * @return A bit set for a given index (always not null)
	 */
	private BitSet bitSet(final long index) {
		final Long iIndex = getSetIndex(index);
		BitSet bitSet = m_sets.get(iIndex);
		if (bitSet == null) {
			bitSet = new BitSet(Integer.MAX_VALUE);
			m_sets.put(iIndex, bitSet);
			System.out.println("set=" + m_sets.size());
		}
		return bitSet;
	}

	/**
	 * Set a given value for a given index
	 * 
	 * @param index
	 *            Long index
	 * @param value
	 *            Value to set
	 */
	public void set(final long index, final boolean value) {
		if (value) {
			bitSet(index).set(getPos(index), value);
		} else { // if value shall be cleared, check first if given partition
					// exists
			final BitSet bitSet = m_sets.get(getSetIndex(index));
			if (bitSet != null)
				bitSet.clear(getPos(index));
		}
	}

	/**
	 * Get a value for a given index
	 * 
	 * @param index
	 *            Long index
	 * @return Value associated with a given index
	 */
	public boolean get(final long index) {
		final BitSet bitSet = m_sets.get(getSetIndex(index));
		return bitSet != null && bitSet.get(getPos(index));
	}

	/**
	 * Clear all bits between {@code fromIndex} (inclusive) and {@code toIndex}
	 * (exclusive)
	 * 
	 * @param fromIndex
	 *            Start index (inclusive)
	 * @param toIndex
	 *            End index (exclusive)
	 */
	public void clear(final long fromIndex, final long toIndex) {
		if (fromIndex >= toIndex)
			return;
		final long fromPos = getSetIndex(fromIndex);
		final long toPos = getSetIndex(toIndex);
		// remove all maps in the middle
		for (long i = fromPos + 1; i < toPos; ++i)
			m_sets.remove(i);
		// clean two corner sets manually
		final BitSet fromSet = m_sets.get(fromPos);
		final BitSet toSet = m_sets.get(toPos);
		// /are both ends in the same subset?
		if (fromSet != null && fromSet == toSet) {
			fromSet.clear(getPos(fromIndex), getPos(toIndex));
			return;
		}
		// clean left subset from left index to the end
		if (fromSet != null)
			fromSet.clear(getPos(fromIndex), fromSet.length());
		// clean right subset from 0 to given index. Note that both checks are
		// independent
		if (toSet != null)
			toSet.clear(0, getPos(toIndex));
	}

	/**
	 * Iteration over all set values in a LongBitSet. Order of iteration is not
	 * specified.
	 * 
	 * @param proc
	 *            Procedure to call. If it returns {@code false}, then iteration
	 *            will stop at once
	 */
	public long nextSetBit(long pos) {
		for (final Map.Entry<Long, BitSet> entry : m_sets.entrySet()) {
			final BitSet bs = entry.getValue();
			final long baseIndex = entry.getKey();
			if (baseIndex >= pos) {
				int i = bs.nextSetBit(0);
				if (i >= 0) {
					return (baseIndex + i);
				}
			}

		}
		return -1;
	}

	/**
	 * Iteration over all set values in a LongBitSet. Order of iteration is not
	 * specified.
	 * 
	 * @param proc
	 *            Procedure to call. If it returns {@code false}, then iteration
	 *            will stop at once
	 */
	public long cardinality() {
		long cd = 0;
		for (final Map.Entry<Long, BitSet> entry : m_sets.entrySet()) {
			final BitSet bs = entry.getValue();
			cd = cd + (long) bs.cardinality();

		}
		return cd;
	}
}
