package org.opendedup.util;

import java.io.File;
import java.io.IOException;
import java.io.ObjectStreamField;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

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
public class MappedByteBufferBitSet implements Cloneable, java.io.Serializable {
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
	MappedByteBuffer buf = null;
	FileChannel fc = null;
	// private long[] words;

	/**
	 * The number of words in the logical size of this BitSet.
	 */
	private transient int wordsInUse = 0;

	private String fileName;

	/* use serialVersionUID from JDK 1.0.2 for interoperability */
	private static final long serialVersionUID = 7997698588986878753L;

	/**
	 * Given a bit index, return word index containing it.
	 */
	private static int wordIndex(long bitIndex) {
		return (int) (bitIndex >> ADDRESS_BITS_PER_WORD);
	}

	private long getWord(int arrayPos) {
		int pos = 0;
		try {
			pos = (arrayPos * 8) + 8;
			return this.buf.getLong(pos);
		} catch (Exception e) {
			System.out.println("unable to get " + pos + " buf cap = "
					+ this.buf.capacity());
			return 0;
		}
	}

	private void setWord(int arrayPos, long value) {
		int pos = (arrayPos * 8) + 8;
		this.buf.putLong(pos, value);
	}

	private int getWordLength() {
		return (buf.capacity() - 8) / 8;
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
	 * 
	 * @throws IOException
	 */
	public MappedByteBufferBitSet(String fileName) throws IOException {
		this.fileName = fileName;
		File f = new File(fileName);
		if (f.exists()) {
			this.initWords((int) f.length());
		} else {
			int len = ((wordIndex(BITS_PER_WORD - 1) + 1) * 8) + 8;
			this.initWords(len);
		}
	}

	/**
	 * Creates a bit set whose initial size is large enough to explicitly
	 * represent bits with indices in the range {@code 0} through
	 * {@code nbits-1}. All bits are initially {@code false}.
	 * 
	 * @param nbits
	 *            the initial size of the bit set
	 * @throws IOException
	 * @throws NegativeArraySizeException
	 *             if the specified initial size is negative
	 */
	public MappedByteBufferBitSet(String fileName, int nbits)
			throws IOException {
		// nbits can't be negative; size 0 is OK
		this.fileName = fileName;
		if (nbits < 0)
			throw new NegativeArraySizeException("nbits < 0: " + nbits);

		initWords(nbits);
	}

	private void initWords(int length) throws IOException {
		Path p = Paths.get(fileName);
		fc = (FileChannel) Files.newByteChannel(p, StandardOpenOption.CREATE,
				StandardOpenOption.WRITE, StandardOpenOption.READ,
				StandardOpenOption.SPARSE);
		buf = fc.map(MapMode.READ_WRITE, 0, length);
		this.wordsInUse = buf.getInt(0);
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
	 * @throws IOException
	 */
	private void ensureCapacity(int wordsRequired) throws IOException {
		if (this.getWordLength() < wordsRequired) {
			// Allocate larger of doubled size or required size
			int request = Math.max(2 * this.getWordLength(), wordsRequired);
			Path p = Paths.get(fileName);
			fc.close();
			fc = null;
			fc = (FileChannel) Files.newByteChannel(p,
					StandardOpenOption.CREATE, StandardOpenOption.WRITE,
					StandardOpenOption.READ, StandardOpenOption.SPARSE);
			buf = fc.map(MapMode.READ_WRITE, 0, (request * 8) + 8);
			buf.putInt(0, this.wordsInUse);
		}
	}

	/**
	 * Ensures that the BitSet can accommodate a given wordIndex, temporarily
	 * violating the invariants. The caller must restore the invariants before
	 * returning to the user, possibly using recalculateWordsInUse().
	 * 
	 * @param wordIndex
	 *            the index to be accommodated.
	 * @throws IOException
	 */
	private void expandTo(int wordIndex) throws IOException {
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
	 * @throws IOException
	 * @throws IndexOutOfBoundsException
	 *             if the specified index is negative
	 * @since 1.4
	 */
	public void flip(int bitIndex) throws IOException {
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
	 * @throws IOException
	 * @throws IndexOutOfBoundsException
	 *             if {@code fromIndex} is negative, or {@code toIndex} is
	 *             negative, or {@code fromIndex} is larger than {@code toIndex}
	 * @since 1.4
	 */
	public void flip(int fromIndex, int toIndex) throws IOException {
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
	 * @throws IOException
	 * @throws IndexOutOfBoundsException
	 *             if the specified index is negative
	 * @since JDK1.0
	 */
	public void set(long bitIndex) throws IOException {
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
	 * @throws IOException
	 * @throws IndexOutOfBoundsException
	 *             if the specified index is negative
	 * @since 1.4
	 */
	public void set(long bitIndex, boolean value) throws IOException {
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
	 * @throws IOException
	 * @throws IndexOutOfBoundsException
	 *             if {@code fromIndex} is negative, or {@code toIndex} is
	 *             negative, or {@code fromIndex} is larger than {@code toIndex}
	 * @since 1.4
	 */
	public void set(int fromIndex, int toIndex) throws IOException {
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
	public void clear(long bitIndex) {
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
	 * @throws IOException
	 * 
	 * @since 1.4
	 */
	public void clear() throws IOException {
		fc.close();
		fc = null;
		Path p = Paths.get(fileName);
		Files.deleteIfExists(p);
		this.wordsInUse = 0;

		int cap = buf.capacity();
		buf = null;
		fc = (FileChannel) Files.newByteChannel(p, StandardOpenOption.CREATE,
				StandardOpenOption.WRITE, StandardOpenOption.READ,
				StandardOpenOption.SPARSE);
		buf = fc.map(MapMode.READ_WRITE, 0, cap);
	}

	/**
	 * Deletes the MappedByteBufferBitSet {@code false}.
	 * 
	 * @throws IOException
	 * 
	 * @since 1.4
	 */
	public void delete() throws IOException {
		fc.close();
		fc = null;
		Path p = Paths.get(fileName);
		Files.deleteIfExists(p);
		buf = null;
	}

	/**
	 * Deletes the MappedByteBufferBitSet {@code false}.
	 * 
	 * @throws IOException
	 * 
	 * @since 1.4
	 */
	public void close() throws IOException {
		buf.putInt(0, this.wordsInUse);
		fc.force(false);
		fc.close();
		fc = null;
		buf = null;
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
	public boolean get(long bitIndex) {
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

	public static void main(String[] args) throws IOException {
		MappedByteBufferBitSet bs = new MappedByteBufferBitSet(
				"/home/annesam/test.map");
		System.out.println(bs.cardinality());
		for (int i = 0; i < 10000000; i++) {
			bs.set(i, true);
		}
		System.out.println(bs.cardinality());
		bs.close();
	}

}
