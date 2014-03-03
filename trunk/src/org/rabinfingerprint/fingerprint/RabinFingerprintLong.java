package org.rabinfingerprint.fingerprint;

import org.rabinfingerprint.polynomial.Polynomial;

/**
 * A {@link Fingerprint} builder that uses longs and lookup tables to increase
 * performance.
 * 
 * Note, the polynomial must be of degree 64 - 8 - 1 - 1 = 54 or less!
 * 
 * <pre>
 *   64 for the size of a long
 *    8 for the space we need when shifting
 *    1 for the sign bit (Java doesn't support unsigned longs)
 *    1 for the conversion between degree and bit offset.
 * </pre>
 * 
 * Some good choices are 53, 47, 31, 15
 * 
 * @see RabinFingerprintPolynomial for a rundown of the math
 */
public class RabinFingerprintLong extends AbstractFingerprint {
	protected final long[] pushTable;
	protected final int degree;
	protected final int shift;
	
	protected long fingerprint;

	public RabinFingerprintLong(Polynomial poly) {
		super(poly);
		this.degree = poly.degree().intValue();
		this.shift = degree - 8;
		this.fingerprint = 0;
		this.pushTable = new long[512];
		precomputePushTable();
	}

	public RabinFingerprintLong(RabinFingerprintLong that) {
		super(that.poly);
		this.degree = that.degree;
		this.shift = that.shift;
		this.pushTable = that.pushTable;
		this.fingerprint = 0;
	}

	/**
	 * Precomputes the results of pushing and popping bytes. These use the more
	 * accurate Polynomial methods (they won't overflow like longs, and they
	 * compute in GF(2^k)).
	 * 
	 * These algorithms should be synonymous with
	 * {@link RabinFingerprintPolynomial#pushByte} and
	 * {@link RabinFingerprintPolynomial#popByte}, but the results are stored to
	 * be xor'red with the fingerprint in the inner loop of our own
	 * {@link #pushByte} and {@link #popByte}
	 */
	private void precomputePushTable() {
		for (int i = 0; i < 512; i++) {
			Polynomial f = Polynomial.createFromLong(i);
			f = f.shiftLeft(poly.degree());
			f = f.xor(f.mod(poly));
			pushTable[i] = f.toBigInteger().longValue();
		}
	}

	@Override
	public void pushBytes(final byte[] bytes) {
		for (byte b : bytes) {
			int j = (int) ((fingerprint >> shift) & 0x1FF);
			fingerprint = ((fingerprint << 8) | (b & 0xFF)) ^ pushTable[j];
		}
	}

	@Override
	public void pushBytes(final byte[] bytes, final int offset, final int length) {
		final int max = offset + length;
		int i = offset;
		while (i < max) {
			int j = (int) ((fingerprint >> shift) & 0x1FF);
			fingerprint = ((fingerprint << 8) | (bytes[i++] & 0xFF)) ^ pushTable[j];
		}
	}

	@Override
	public void pushByte(byte b) {
		int j = (int) ((fingerprint >> shift) & 0x1FF);
		fingerprint = ((fingerprint << 8) | (b & 0xFF)) ^ pushTable[j];
	}

	@Override
	public void reset() {
		this.fingerprint = 0L;
	}
	
	@Override
	public Polynomial getFingerprint() {
		return Polynomial.createFromLong(fingerprint);
	}

	public long getFingerprintLong() {
		return fingerprint;
	}
}
