package org.opendedup.util;

import java.io.ObjectStreamField;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;

/**
 * This class implements a vector of bits that grows as needed. Each component
 * of the bit set has a {@code boolean} value. The bits of a {@code BitSet} are
 * indexed by nonnegative integers. Individual indexed bits can be examined,
 * set, or cleared. One {@code BitSet} may be used to modify the contents of
 * another {@code BitSet} through logical AND, logical inclusive OR, and logical
 * exclusive OR operations.
 * 
 * <p>
 * By default, all bits in the set initially have the value {@code false}.
 * 
 * <p>
 * Every bit set has a current size, which is the number of bits of space
 * currently in use by the bit set. Note that the size is related to the
 * implementation of a bit set, so it may change with implementation. The length
 * of a bit set relates to logical length of a bit set and is defined
 * independently of implementation.
 * 
 * <p>
 * Unless otherwise noted, passing a null parameter to any of the methods in a
 * {@code BitSet} will result in a {@code NullPointerException}.
 * 
 * <p>
 * A {@code BitSet} is not safe for multithreaded use without external
 * synchronization.
 * 
 * @author Arthur van Hoff
 * @author Michael McCloskey
 * @author Martin Buchholz
 * @since JDK1.0
 */
public class ByteBufferBitSet implements Cloneable, java.io.Serializable {
	/*
	 * BitSets are packed into arrays of "words." Currently a word is a long,
	 * which consists of 64 bits, requiring 6 address bits. The choice of word
	 * size is determined purely by performance concerns.
	 */
	private final static int ADDRESS_BITS_PER_WORD = 6;
	private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;

	/* Used to shift left or right for a partial word mask */
	private static final long WORD_MASK = 0xffffffffffffffffL;

	/**
	 * @serialField
	 *                  bits long[]
	 * 
	 *                  The bits in this BitSet. The ith bit is stored in
	 *                  bits[i/64] at bit position i % 64 (where bit position 0
	 *                  refers to the least significant bit and 63 refers to the
	 *                  most significant bit).
	 */
	private static final ObjectStreamField[] serialPersistentFields = { new ObjectStreamField(
			"bits", long[].class), };

	/**
	 * The internal field corresponding to the serialField "bits".
	 */
	ByteBuffer buf = null;
	// private long[] words;

	/**
	 * The number of words in the logical size of this BitSet.
	 */
	private transient int wordsInUse = 0;

	/* use serialVersionUID from JDK 1.0.2 for interoperability */
	private static final long serialVersionUID = 7997698588986878753L;

	/**
	 * Given a bit index, return word index containing it.
	 */
	private static int wordIndex(int bitIndex) {
		return bitIndex >> ADDRESS_BITS_PER_WORD;
	}

	private long getWord(int arrayPos) {
		int pos = arrayPos * 8;
		return this.buf.getLong(pos);
	}

	private void setWord(int arrayPos, long value) {
		int pos = arrayPos * 8;
		this.buf.putLong(pos, value);
	}

	private int getWordLength() {
		return buf.capacity() / 8;
	}

	/**
	 * Every public method must preserve these invariants.
	 */
	private void checkInvariants() {
		assert (wordsInUse == 0 || getWord(wordsInUse - 1) != 0);
		assert (wordsInUse >= 0 && wordsInUse <= this.getWordLength());
		assert (wordsInUse == this.getWordLength() || this.getWord(wordsInUse) == 0);
	}

	/**
	 * Sets the field wordsInUse to the logical size in words of the bit set.
	 * WARNING:This method assumes that the number of words actually in use is
	 * less than or equal to the current value of wordsInUse!
	 */
	private void recalculateWordsInUse() {
		// Traverse the bitset until a used word is found
		int i;
		for (i = wordsInUse - 1; i >= 0; i--)
			if (this.getWord(i) != 0)
				break;

		wordsInUse = i + 1; // The new logical size
	}

	/**
	 * Creates a new bit set. All bits are initially {@code false}.
	 */
	public ByteBufferBitSet() {
		initWords(BITS_PER_WORD);
	}

	/**
	 * Creates a bit set whose initial size is large enough to explicitly
	 * represent bits with indices in the range {@code 0} through
	 * {@code nbits-1}. All bits are initially {@code false}.
	 * 
	 * @param nbits
	 *            the initial size of the bit set
	 * @throws NegativeArraySizeException
	 *             if the specified initial size is negative
	 */
	public ByteBufferBitSet(int nbits) {
		// nbits can't be negative; size 0 is OK
		if (nbits < 0)
			throw new NegativeArraySizeException("nbits < 0: " + nbits);

		initWords(nbits);
	}

	private void initWords(int nbits) {
		buf = ByteBuffer.allocateDirect((wordIndex(nbits - 1) + 1) * 8);
	}

	/**
	 * Creates a bit set using words as the internal representation. The last
	 * word (if there is one) must be non-zero.
	 */
	private ByteBufferBitSet(long[] words) {
		buf = ByteBuffer.allocateDirect(words.length * 8);
		for (int i = 0; i < words.length; i++) {
			buf.putLong(words[i]);
		}
		this.wordsInUse = words.length;
		checkInvariants();
	}

	/**
	 * Returns a new bit set containing all the bits in the given long array.
	 * 
	 * <p>
	 * More precisely, <br>
	 * {@code BitSet.valueOf(longs).get(n) == ((longs[n/64] & (1L<<(n%64))) != 0)}
	 * <br>
	 * for all {@code n < 64 * longs.length}.
	 * 
	 * <p>
	 * This method is equivalent to
	 * {@code BitSet.valueOf(LongBuffer.wrap(longs))}.
	 * 
	 * @param longs
	 *            a long array containing a little-endian representation of a
	 *            sequence of bits to be used as the initial bits of the new bit
	 *            set
	 * @since 1.7
	 */
	public static ByteBufferBitSet valueOf(long[] longs) {
		int n;
		for (n = longs.length; n > 0 && longs[n - 1] == 0; n--)
			;
		return new ByteBufferBitSet(Arrays.copyOf(longs, n));
	}

	/**
	 * Returns a new bit set containing all the bits in the given long buffer
	 * between its position and limit.
	 * 
	 * <p>
	 * More precisely, <br>
	 * {@code BitSet.valueOf(lb).get(n) == ((lb.get(lb.position()+n/64) & (1L<<(n%64))) != 0)}
	 * <br>
	 * for all {@code n < 64 * lb.remaining()}.
	 * 
	 * <p>
	 * The long buffer is not modified by this method, and no reference to the
	 * buffer is retained by the bit set.
	 * 
	 * @param lb
	 *            a long buffer containing a little-endian representation of a
	 *            sequence of bits between its position and limit, to be used as
	 *            the initial bits of the new bit set
	 * @since 1.7
	 */
	public static ByteBufferBitSet valueOf(LongBuffer lb) {
		lb = lb.slice();
		int n;
		for (n = lb.remaining(); n > 0 && lb.get(n - 1) == 0; n--)
			;
		long[] words = new long[n];
		lb.get(words);
		return new ByteBufferBitSet(words);
	}

	/**
	 * Returns a new bit set containing all the bits in the given byte array.
	 * 
	 * <p>
	 * More precisely, <br>
	 * {@code BitSet.valueOf(bytes).get(n) == ((bytes[n/8] & (1<<(n%8))) != 0)}
	 * <br>
	 * for all {@code n <  8 * bytes.length}.
	 * 
	 * <p>
	 * This method is equivalent to
	 * {@code BitSet.valueOf(ByteBuffer.wrap(bytes))}.
	 * 
	 * @param bytes
	 *            a byte array containing a little-endian representation of a
	 *            sequence of bits to be used as the initial bits of the new bit
	 *            set
	 * @since 1.7
	 */
	public static ByteBufferBitSet valueOf(byte[] bytes) {
		return ByteBufferBitSet.valueOf(ByteBuffer.wrap(bytes));
	}

	/**
	 * Returns a new bit set containing all the bits in the given byte buffer
	 * between its position and limit.
	 * 
	 * <p>
	 * More precisely, <br>
	 * {@code BitSet.valueOf(bb).get(n) == ((bb.get(bb.position()+n/8) & (1<<(n%8))) != 0)}
	 * <br>
	 * for all {@code n < 8 * bb.remaining()}.
	 * 
	 * <p>
	 * The byte buffer is not modified by this method, and no reference to the
	 * buffer is retained by the bit set.
	 * 
	 * @param bb
	 *            a byte buffer containing a little-endian representation of a
	 *            sequence of bits between its position and limit, to be used as
	 *            the initial bits of the new bit set
	 * @since 1.7
	 */
	public static ByteBufferBitSet valueOf(ByteBuffer bb) {
		bb = bb.slice().order(ByteOrder.LITTLE_ENDIAN);
		int n;
		for (n = bb.remaining(); n > 0 && bb.get(n - 1) == 0; n--)
			;
		long[] words = new long[(n + 7) / 8];
		bb.limit(n);
		int i = 0;
		while (bb.remaining() >= 8)
			words[i++] = bb.getLong();
		for (int remaining = bb.remaining(), j = 0; j < remaining; j++)
			words[i] |= (bb.get() & 0xffL) << (8 * j);
		return new ByteBufferBitSet(words);
	}

	/**
	 * Returns a new byte array containing all the bits in this bit set.
	 * 
	 * <p>
	 * More precisely, if <br>
	 * {@code byte[] bytes = s.toByteArray();} <br>
	 * then {@code bytes.length == (s.length()+7)/8} and <br>
	 * {@code s.get(n) == ((bytes[n/8] & (1<<(n%8))) != 0)} <br>
	 * for all {@code n < 8 * bytes.length}.
	 * 
	 * @return a byte array containing a little-endian representation of all the
	 *         bits in this bit set
	 * @since 1.7
	 */
	public byte[] toByteArray() {
		int n = wordsInUse;
		if (n == 0)
			return new byte[0];
		int len = 8 * (n - 1);
		for (long x = this.getWord(n - 1); x != 0; x >>>= 8)
			len++;
		byte[] bytes = new byte[len];
		ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
		for (int i = 0; i < n - 1; i++)
			bb.putLong(this.getWord(i));
		for (long x = this.getWord(n - 1); x != 0; x >>>= 8)
			bb.put((byte) (x & 0xff));
		return bytes;
	}

	/**
	 * Returns a new long array containing all the bits in this bit set.
	 * 
	 * <p>
	 * More precisely, if <br>
	 * {@code long[] longs = s.toLongArray();} <br>
	 * then {@code longs.length == (s.length()+63)/64} and <br>
	 * {@code s.get(n) == ((longs[n/64] & (1L<<(n%64))) != 0)} <br>
	 * for all {@code n < 64 * longs.length}.
	 * 
	 * @return a long array containing a little-endian representation of all the
	 *         bits in this bit set
	 * @since 1.7
	 */
	public long[] toLongArray() {
		long[] words = new long[this.getWordLength()];
		int i = 0;
		while (i <= buf.capacity()) {
			words[i] = buf.getLong(i);
			i = i + 8;
		}
		return words;
	}

	/**
	 * Ensures that the BitSet can hold enough words.
	 * 
	 * @param wordsRequired
	 *            the minimum acceptable number of words.
	 */
	private void ensureCapacity(int wordsRequired) {
		if (this.getWordLength() < wordsRequired) {
			// Allocate larger of doubled size or required size
			int request = Math.max(2 * this.getWordLength(), wordsRequired);
			ByteBuffer newBuf = ByteBuffer.allocateDirect(request * 8);
			newBuf.put(buf);
			buf = newBuf;
		}
	}

	/**
	 * Ensures that the BitSet can accommodate a given wordIndex, temporarily
	 * violating the invariants. The caller must restore the invariants before
	 * returning to the user, possibly using recalculateWordsInUse().
	 * 
	 * @param wordIndex
	 *            the index to be accommodated.
	 */
	private void expandTo(int wordIndex) {
		int wordsRequired = wordIndex + 1;
		if (wordsInUse < wordsRequired) {
			ensureCapacity(wordsRequired);
			wordsInUse = wordsRequired;
		}
	}

	/**
	 * Checks that fromIndex ... toIndex is a valid range of bit indices.
	 */
	private static void checkRange(int fromIndex, int toIndex) {
		if (fromIndex < 0)
			throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);
		if (toIndex < 0)
			throw new IndexOutOfBoundsException("toIndex < 0: " + toIndex);
		if (fromIndex > toIndex)
			throw new IndexOutOfBoundsException("fromIndex: " + fromIndex
					+ " > toIndex: " + toIndex);
	}

	/**
	 * Sets the bit at the specified index to the complement of its current
	 * value.
	 * 
	 * @param bitIndex
	 *            the index of the bit to flip
	 * @throws IndexOutOfBoundsException
	 *             if the specified index is negative
	 * @since 1.4
	 */
	public void flip(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int wordIndex = wordIndex(bitIndex);
		expandTo(wordIndex);
		long cv = this.getWord(wordIndex);
		cv ^= (1L << bitIndex);
		this.setWord(wordIndex, cv);

		recalculateWordsInUse();
		checkInvariants();
	}

	/**
	 * Sets each bit from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to the complement of its current
	 * value.
	 * 
	 * @param fromIndex
	 *            index of the first bit to flip
	 * @param toIndex
	 *            index after the last bit to flip
	 * @throws IndexOutOfBoundsException
	 *             if {@code fromIndex} is negative, or {@code toIndex} is
	 *             negative, or {@code fromIndex} is larger than {@code toIndex}
	 * @since 1.4
	 */
	public void flip(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return;

		int startWordIndex = wordIndex(fromIndex);
		int endWordIndex = wordIndex(toIndex - 1);
		expandTo(endWordIndex);

		long firstWordMask = WORD_MASK << fromIndex;
		long lastWordMask = WORD_MASK >>> -toIndex;
		long cv = this.getWord(startWordIndex);
		if (startWordIndex == endWordIndex) {
			// Case 1: One word

			cv ^= (firstWordMask & lastWordMask);
			this.setWord(startWordIndex, cv);
		} else {
			// Case 2: Multiple words
			// Handle first word
			cv ^= firstWordMask;
			this.setWord(startWordIndex, cv);
			// Handle intermediate words, if any
			for (int i = startWordIndex + 1; i < endWordIndex; i++) {
				long word = this.getWord(i);
				word ^= WORD_MASK;
				this.setWord(i, word);
			}
			long word = this.getWord(endWordIndex);
			// Handle last word
			word ^= lastWordMask;
			this.setWord(endWordIndex, word);
		}

		recalculateWordsInUse();
		checkInvariants();
	}

	/**
	 * Sets the bit at the specified index to {@code true}.
	 * 
	 * @param bitIndex
	 *            a bit index
	 * @throws IndexOutOfBoundsException
	 *             if the specified index is negative
	 * @since JDK1.0
	 */
	public void set(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int wordIndex = wordIndex(bitIndex);
		expandTo(wordIndex);
		long word = this.getWord(wordIndex);
		// Handle last word
		word |= (1L << bitIndex);
		this.setWord(wordIndex, word);

		checkInvariants();
	}

	/**
	 * Sets the bit at the specified index to the specified value.
	 * 
	 * @param bitIndex
	 *            a bit index
	 * @param value
	 *            a boolean value to set
	 * @throws IndexOutOfBoundsException
	 *             if the specified index is negative
	 * @since 1.4
	 */
	public void set(int bitIndex, boolean value) {
		if (value)
			set(bitIndex);
		else
			clear(bitIndex);
	}

	/**
	 * Sets the bits from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to {@code true}.
	 * 
	 * @param fromIndex
	 *            index of the first bit to be set
	 * @param toIndex
	 *            index after the last bit to be set
	 * @throws IndexOutOfBoundsException
	 *             if {@code fromIndex} is negative, or {@code toIndex} is
	 *             negative, or {@code fromIndex} is larger than {@code toIndex}
	 * @since 1.4
	 */
	public void set(int fromIndex, int toIndex) {
		checkRange(fromIndex, toIndex);

		if (fromIndex == toIndex)
			return;

		// Increase capacity if necessary
		int startWordIndex = wordIndex(fromIndex);
		int endWordIndex = wordIndex(toIndex - 1);
		expandTo(endWordIndex);

		long firstWordMask = WORD_MASK << fromIndex;
		long lastWordMask = WORD_MASK >>> -toIndex;
		long cv = this.getWord(startWordIndex);
		if (startWordIndex == endWordIndex) {
			// Case 1: One word
			cv |= (firstWordMask & lastWordMask);
			this.setWord(startWordIndex, cv);
		} else {
			// Case 2: Multiple words
			// Handle first word
			cv |= firstWordMask;
			this.setWord(startWordIndex, cv);
			// Handle intermediate words, if any
			for (int i = startWordIndex + 1; i < endWordIndex; i++) {
				long word = this.getWord(i);
				word = WORD_MASK;
				this.setWord(i, word);
			}
			// Handle last word (restores invariants)
			long word = this.getWord(endWordIndex);
			word |= lastWordMask;
			this.setWord(endWordIndex, word);
		}

		checkInvariants();
	}

	/**
	 * Sets the bits from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to the specified value.
	 * 
	 * @param fromIndex
	 *            index of the first bit to be set
	 * @param toIndex
	 *            index after the last bit to be set
	 * @param value
	 *            value to set the selected bits to
	 * @throws IndexOutOfBoundsException
	 *             if {@code fromIndex} is negative, or {@code toIndex} is
	 *             negative, or {@code fromIndex} is larger than {@code toIndex}
	 * @since 1.4
	 */
	/*
	 * public void set(int fromIndex, int toIndex, boolean value) { if (value)
	 * set(fromIndex, toIndex); else clear(fromIndex, toIndex); }
	 */

	/**
	 * Sets the bit specified by the index to {@code false}.
	 * 
	 * @param bitIndex
	 *            the index of the bit to be cleared
	 * @throws IndexOutOfBoundsException
	 *             if the specified index is negative
	 * @since JDK1.0
	 */
	public void clear(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		int wordIndex = wordIndex(bitIndex);
		if (wordIndex >= wordsInUse)
			return;
		long cv = this.getWord(wordIndex);
		cv &= ~(1L << bitIndex);
		this.setWord(wordIndex, cv);
		recalculateWordsInUse();
		checkInvariants();
	}

	/**
	 * Sets the bits from the specified {@code fromIndex} (inclusive) to the
	 * specified {@code toIndex} (exclusive) to {@code false}.
	 * 
	 * @param fromIndex
	 *            index of the first bit to be cleared
	 * @param toIndex
	 *            index after the last bit to be cleared
	 * @throws IndexOutOfBoundsException
	 *             if {@code fromIndex} is negative, or {@code toIndex} is
	 *             negative, or {@code fromIndex} is larger than {@code toIndex}
	 * @since 1.4
	 */
	/*
	 * public void clear(int fromIndex, int toIndex) { checkRange(fromIndex,
	 * toIndex);
	 * 
	 * if (fromIndex == toIndex) return;
	 * 
	 * int startWordIndex = wordIndex(fromIndex); if (startWordIndex >=
	 * wordsInUse) return;
	 * 
	 * int endWordIndex = wordIndex(toIndex - 1); if (endWordIndex >=
	 * wordsInUse) { toIndex = length(); endWordIndex = wordsInUse - 1; }
	 * 
	 * long firstWordMask = WORD_MASK << fromIndex; long lastWordMask =
	 * WORD_MASK >>> -toIndex; if (startWordIndex == endWordIndex) { // Case 1:
	 * One word words[startWordIndex] &= ~(firstWordMask & lastWordMask); } else
	 * { // Case 2: Multiple words // Handle first word words[startWordIndex] &=
	 * ~firstWordMask;
	 * 
	 * // Handle intermediate words, if any for (int i = startWordIndex+1; i <
	 * endWordIndex; i++) words[i] = 0;
	 * 
	 * // Handle last word words[endWordIndex] &= ~lastWordMask; }
	 * 
	 * recalculateWordsInUse(); checkInvariants(); }
	 */

	/**
	 * Sets all of the bits in this BitSet to {@code false}.
	 * 
	 * @since 1.4
	 */
	public void clear() {
		int cap = buf.capacity();
		buf = ByteBuffer.allocateDirect(cap);
	}

	/**
	 * Returns the value of the bit with the specified index. The value is
	 * {@code true} if the bit with the index {@code bitIndex} is currently set
	 * in this {@code BitSet}; otherwise, the result is {@code false}.
	 * 
	 * @param bitIndex
	 *            the bit index
	 * @return the value of the bit with the specified index
	 * @throws IndexOutOfBoundsException
	 *             if the specified index is negative
	 */
	public boolean get(int bitIndex) {
		if (bitIndex < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);

		checkInvariants();

		int wordIndex = wordIndex(bitIndex);
		return (wordIndex < wordsInUse)
				&& ((this.getWord(wordIndex) & (1L << bitIndex)) != 0);
	}

	/**
	 * Returns a new {@code BitSet} composed of bits from this {@code BitSet}
	 * from {@code fromIndex} (inclusive) to {@code toIndex} (exclusive).
	 * 
	 * @param fromIndex
	 *            index of the first bit to include
	 * @param toIndex
	 *            index after the last bit to include
	 * @return a new {@code BitSet} from a range of this {@code BitSet}
	 * @throws IndexOutOfBoundsException
	 *             if {@code fromIndex} is negative, or {@code toIndex} is
	 *             negative, or {@code fromIndex} is larger than {@code toIndex}
	 * @since 1.4
	 */

	/**
	 * Returns the "logical size" of this {@code BitSet}: the index of the
	 * highest set bit in the {@code BitSet} plus one. Returns zero if the
	 * {@code BitSet} contains no set bits.
	 * 
	 * @return the logical size of this {@code BitSet}
	 * @since 1.2
	 */
	public int length() {
		if (wordsInUse == 0)
			return 0;

		return BITS_PER_WORD
				* (wordsInUse - 1)
				+ (BITS_PER_WORD - Long.numberOfLeadingZeros(this
						.getWord(wordsInUse - 1)));
	}

	/**
	 * Returns true if this {@code BitSet} contains no bits that are set to
	 * {@code true}.
	 * 
	 * @return boolean indicating whether this {@code BitSet} is empty
	 * @since 1.4
	 */
	public boolean isEmpty() {
		return wordsInUse == 0;
	}

	/**
	 * Returns the number of bits set to {@code true} in this {@code BitSet}.
	 * 
	 * @return the number of bits set to {@code true} in this {@code BitSet}
	 * @since 1.4
	 */
	public int cardinality() {
		int sum = 0;
		for (int i = 0; i < wordsInUse; i++)
			sum += Long.bitCount(this.getWord(i));
		return sum;
	}

	/**
	 * Returns the hash code value for this bit set. The hash code depends only
	 * on which bits are set within this {@code BitSet}.
	 * 
	 * <p>
	 * The hash code is defined to be the result of the following calculation:
	 * 
	 * <pre>
	 * {@code
	 * public int hashCode() {
	 *     long h = 1234;
	 *     long[] words = toLongArray();
	 *     for (int i = words.length; --i >= 0; )
	 *         h ^= words[i] * (i + 1);
	 *     return (int)((h >> 32) ^ h);
	 * }}
	 * </pre>
	 * 
	 * Note that the hash code changes if the set of bits is altered.
	 * 
	 * @return the hash code value for this bit set
	 */
	@Override
	public int hashCode() {
		long h = 1234;
		for (int i = wordsInUse; --i >= 0;)
			h ^= this.getWord(i) * (i + 1);

		return (int) ((h >> 32) ^ h);
	}

	/**
	 * Returns the number of bits of space actually in use by this
	 * {@code BitSet} to represent bit values. The maximum element in the set is
	 * the size - 1st element.
	 * 
	 * @return the number of bits currently in this bit set
	 */
	public int size() {
		return (this.getWordLength()) * BITS_PER_WORD;
	}

	public static void main(String[] args) {
		ByteBufferBitSet bs = new ByteBufferBitSet();
		bs.set(1000000);
		System.out.println(bs.get(1000));
		for (int i = 0; i < 100000; i++) {
			bs.set(i * 2);
		}
		for (int i = 0; i < 100000; i++) {
			System.out.println(bs.get(i * 2));
		}
		System.out.println(bs.get(1000000));
		System.out.println(bs.cardinality());
	}

}
