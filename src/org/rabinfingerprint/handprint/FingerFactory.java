package org.rabinfingerprint.handprint;

import java.io.IOException;
import java.io.InputStream;

import org.rabinfingerprint.fingerprint.RabinFingerprintLong;
import org.rabinfingerprint.fingerprint.RabinFingerprintLongWindowed;
import org.rabinfingerprint.polynomial.Polynomial;

import com.google.common.io.ByteStreams;

public class FingerFactory {
	public static interface ChunkBoundaryDetector {
		public boolean isBoundary(RabinFingerprintLong fingerprint);
	}

	public static interface ChunkVisitor {
		public void visit(long fingerprint, long chunkStart, long chunkEnd);
	}
	
	private final RabinFingerprintLong finger;
	private final RabinFingerprintLongWindowed fingerWindow;
	private final ChunkBoundaryDetector boundaryDetector;

	public FingerFactory(Polynomial p, long bytesPerWindow, ChunkBoundaryDetector boundaryDetector) {
		this.finger = new RabinFingerprintLong(p);
		this.fingerWindow = new RabinFingerprintLongWindowed(p, bytesPerWindow);
		this.boundaryDetector = boundaryDetector;
	}

	private RabinFingerprintLong newFingerprint() {
		return new RabinFingerprintLong(finger);
	}

	private RabinFingerprintLongWindowed newWindowedFingerprint() {
		return new RabinFingerprintLongWindowed(fingerWindow);
	}

	/**
	 * Fingerprint the file into chunks called "Fingers". The chunk boundaries
	 * are determined using a windowed fingerprinter
	 * {@link RabinFingerprintLongWindowed}.
	 * 
	 * The chunk detector is position independent. Therefore, even if a file is
	 * rearranged or partially corrupted, the untouched chunks can be
	 * efficiently discovered.
	 */
	public void getChunkFingerprints(InputStream is, ChunkVisitor visitor) throws IOException {
		// windowing fingerprinter for finding chunk boundaries. this is only
		// reset at the beginning of the file
		final RabinFingerprintLong window = newWindowedFingerprint();

		// fingerprinter for chunks. this is reset after each chunk
		final RabinFingerprintLong finger = newFingerprint();

		// counters
		long chunkStart = 0;
		long chunkEnd = 0;

		/*
		 * fingerprint one byte at a time. we have to use this granularity to
		 * ensure that, for example, a one byte offset at the beginning of the
		 * file won't effect the chunk boundaries
		 */
		for (byte b : ByteStreams.toByteArray(is)) {
			// push byte into fingerprints
			window.pushByte(b);
			finger.pushByte(b);
			chunkEnd++;

			/*
			 * if we've reached a boundary (which we will at some probability
			 * based on the boundary pattern and the size of the fingerprint
			 * window), we store the current chunk fingerprint and reset the
			 * chunk fingerprinter.
			 */
			if (boundaryDetector.isBoundary(window)) {
				visitor.visit(finger.getFingerprintLong(), chunkStart, chunkEnd);
				finger.reset();
				
				// store last chunk offset
				chunkStart = chunkEnd;
			}
		}

		// final chunk
		visitor.visit(finger.getFingerprintLong(), chunkStart, chunkEnd);
	}

	/**
	 * Rapidly fingerprint an entire stream's contents.
	 */
	public long getFullFingerprint(InputStream is) throws IOException {
		final RabinFingerprintLong finger = newFingerprint();
		finger.pushBytes(ByteStreams.toByteArray(is));
		return finger.getFingerprintLong();
	}
}