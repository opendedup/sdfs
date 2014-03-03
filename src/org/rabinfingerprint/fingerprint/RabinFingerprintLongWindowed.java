package org.rabinfingerprint.fingerprint;

import java.math.BigInteger;

import org.rabinfingerprint.datastructures.CircularByteQueue;
import org.rabinfingerprint.fingerprint.Fingerprint.WindowedFingerprint;
import org.rabinfingerprint.polynomial.Polynomial;

public class RabinFingerprintLongWindowed extends RabinFingerprintLong implements WindowedFingerprint<Polynomial> {

	protected final CircularByteQueue byteWindow;
	protected final long bytesPerWindow;
	protected final long[] popTable;

	public RabinFingerprintLongWindowed(Polynomial poly, long bytesPerWindow) {
		super(poly);
		this.bytesPerWindow = bytesPerWindow;
		this.byteWindow = new CircularByteQueue((int) bytesPerWindow + 1);
		this.popTable = new long[256];
		precomputePopTable();
	}

	public RabinFingerprintLongWindowed(RabinFingerprintLongWindowed that) {
		super(that);
		this.bytesPerWindow = that.bytesPerWindow;
		this.byteWindow = new CircularByteQueue((int) bytesPerWindow + 1);
		this.popTable = that.popTable;
	}

	private void precomputePopTable() {
		for (int i = 0; i < 256; i++) {
			Polynomial f = Polynomial.createFromLong(i);
			f = f.shiftLeft(BigInteger.valueOf(bytesPerWindow * 8));
			f = f.mod(poly);
			popTable[i] = f.toBigInteger().longValue();
		}
	}

	@Override
	public void pushBytes(final byte[] bytes) {
		for (byte b : bytes) {
			int j = (int) ((fingerprint >> shift) & 0x1FF);
			fingerprint = ((fingerprint << 8) | (b & 0xFF)) ^ pushTable[j];
			byteWindow.add(b);
			if (byteWindow.isFull()) popByte();
		}
	}

	@Override
	public void pushBytes(final byte[] bytes, final int offset, final int length) {
		final int max = offset + length;
		int i = offset;
		while (i < max) {
			byte b = bytes[i++];
			int j = (int) ((fingerprint >> shift) & 0x1FF);
			fingerprint = ((fingerprint << 8) | (b & 0xFF)) ^ pushTable[j];
			byteWindow.add(b);
			if (byteWindow.isFull()) popByte();
		}
	}

	@Override
	public void pushByte(byte b) {
		int j = (int) ((fingerprint >> shift) & 0x1FF);
		fingerprint = ((fingerprint << 8) | (b & 0xFF)) ^ pushTable[j];
		byteWindow.add(b);
		if (byteWindow.isFull()) popByte();
	}

	/**
	 * Removes the contribution of the first byte in the byte queue from the
	 * fingerprint.
	 * 
	 * {@link RabinFingerprintPolynomial#popByte}
	 */
	public void popByte() {
		byte b = byteWindow.poll();
		fingerprint ^= popTable[(b & 0xFF)];
	}

	@Override
	public void reset() {
		super.reset();
		byteWindow.clear();
	}
}
