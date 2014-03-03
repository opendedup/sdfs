package org.rabinfingerprint.handprint;

import java.io.IOException;




import java.io.InputStream;
import java.nio.ByteBuffer;

import org.rabinfingerprint.fingerprint.RabinFingerprintLong;
import org.rabinfingerprint.fingerprint.RabinFingerprintLongWindowed;
import org.rabinfingerprint.handprint.FingerFactory.ChunkBoundaryDetector;
import org.rabinfingerprint.polynomial.Polynomial;

import com.google.common.io.ByteStreams;

public class EnhancedFingerFactory {
	
	private final RabinFingerprintLong finger;
	private final RabinFingerprintLongWindowed fingerWindow;
	private final ChunkBoundaryDetector boundaryDetector;
	private int MIN_CHUNK_SIZE = 4096;
	private int MAX_CHUNK_SIZE = 1024 * 1024;
	DirectBufPool bpool = null;

	public EnhancedFingerFactory(Polynomial p, long bytesPerWindow, ChunkBoundaryDetector boundaryDetector) {
		this.finger = new RabinFingerprintLong(p);
		this.fingerWindow = new RabinFingerprintLongWindowed(p, bytesPerWindow);
		this.boundaryDetector = boundaryDetector;
	}
	
	public EnhancedFingerFactory(Polynomial p, long bytesPerWindow, ChunkBoundaryDetector boundaryDetector, int minChunkSize, int maxChunkSize) {
		this.finger = new RabinFingerprintLong(p);
		this.fingerWindow = new RabinFingerprintLongWindowed(p, bytesPerWindow);
		this.boundaryDetector = boundaryDetector;
		this.MIN_CHUNK_SIZE = minChunkSize;
		this.MAX_CHUNK_SIZE = maxChunkSize;
		bpool = new DirectBufPool(this.MAX_CHUNK_SIZE);
	}
	
	public static interface EnhancedChunkVisitor {
		public void visit(long fingerprint, long chunkStart, long chunkEnd,byte [] chunk);
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
	public void getChunkFingerprints(InputStream is, EnhancedChunkVisitor visitor) throws IOException {
		// windowing fingerprinter for finding chunk boundaries. this is only
		// reset at the beginning of the file
		final RabinFingerprintLong window = newWindowedFingerprint();

		// fingerprinter for chunks. this is reset after each chunk
		final RabinFingerprintLong finger = newFingerprint();

		// counters
		long chunkStart = 0;
		long chunkEnd = 0;
		int chunkLength = 0;
		ByteBuffer buf = ByteBuffer.allocateDirect(MAX_CHUNK_SIZE);
		buf.clear();
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
			chunkLength++;
			buf.put(b);
			/*
			 * if we've reached a boundary (which we will at some probability
			 * based on the boundary pattern and the size of the fingerprint
			 * window), we store the current chunk fingerprint and reset the
			 * chunk fingerprinter.
			 */
			
			if (boundaryDetector.isBoundary(window) && chunkLength > MIN_CHUNK_SIZE) {
				byte [] c = new byte[chunkLength];
				buf.position(0);
				buf.get(c);
				// store last chunk offset
				chunkLength = 0;
				visitor.visit(finger.getFingerprintLong(), chunkStart, chunkEnd,c);
				chunkStart = chunkEnd;
				finger.reset();
				buf.clear();
			}
			else if(chunkLength >= MAX_CHUNK_SIZE) {
				byte [] c = new byte[chunkLength];
				buf.position(0);
				buf.get(c);
				visitor.visit(finger.getFingerprintLong(), chunkStart, chunkEnd,c);
				finger.reset();
				buf.clear();
				// store last chunk offset
				chunkStart = chunkEnd;
				chunkLength = 0;
			}
		}

		byte [] c = new byte[chunkLength];
		buf.position(0);
		buf.get(c);
		visitor.visit(finger.getFingerprintLong(), chunkStart, chunkEnd,c);
		finger.reset();
		buf.clear();
	}
	
	
	
	public void getChunkFingerprints(byte [] chunk, EnhancedChunkVisitor visitor) throws IOException {
		
		// windowing fingerprinter for finding chunk boundaries. this is only
		// reset at the beginning of the file
		final RabinFingerprintLong window = newWindowedFingerprint();

		// fingerprinter for chunks. this is reset after each chunk
		//final RabinFingerprintLong finger = newFingerprint();

		// counters
		long chunkStart = 0;
		long chunkEnd = 0;
		int chunkLength = 0;
		ByteBuffer buf = ByteBuffer.wrap(new byte[MAX_CHUNK_SIZE]);
		//buf.clear();
		/*
		 * fingerprint one byte at a time. we have to use this granularity to
		 * ensure that, for example, a one byte offset at the beginning of the
		 * file won't effect the chunk boundaries
		 */
		for (byte b : chunk) {
			// push byte into fingerprints
			window.pushByte(b);
			//finger.pushByte(b);
			chunkEnd++;
			chunkLength++;
			buf.put(b);
			/*
			 * if we've reached a boundary (which we will at some probability
			 * based on the boundary pattern and the size of the fingerprint
			 * window), we store the current chunk fingerprint and reset the
			 * chunk fingerprinter.
			 */
			
			if (boundaryDetector.isBoundary(window) && chunkLength > MIN_CHUNK_SIZE) {
				byte [] c = new byte[chunkLength];
				buf.position(0);
				buf.get(c);
				// store last chunk offset
				chunkLength = 0;
				visitor.visit(0, chunkStart, chunkEnd,c);
				chunkStart = chunkEnd;
				//finger.reset();
				buf.clear();
			}
			else if(chunkLength >= MAX_CHUNK_SIZE) {
				byte [] c = new byte[chunkLength];
				buf.position(0);
				buf.get(c);
				visitor.visit(0, chunkStart, chunkEnd,c);
				//finger.reset();
				buf.clear();
				// store last chunk offset
				chunkStart = chunkEnd;
				chunkLength = 0;
			}
		}

		byte [] c = new byte[chunkLength];
		buf.position(0);
		buf.get(c);
		visitor.visit(0, chunkStart, chunkEnd,c);
		//finger.reset();
		buf.clear();
	}
}