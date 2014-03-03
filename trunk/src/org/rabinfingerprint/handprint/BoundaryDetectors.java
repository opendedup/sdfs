package org.rabinfingerprint.handprint;

import org.rabinfingerprint.fingerprint.RabinFingerprintLong;

import org.rabinfingerprint.handprint.FingerFactory.ChunkBoundaryDetector;

public class BoundaryDetectors {
	public static class BitmaskBoundaryDetector implements ChunkBoundaryDetector {
		private final long chunkBoundaryMask;
		private final long chunkPattern;

		public BitmaskBoundaryDetector(long chunkBoundaryMask, long chunkPattern) {
			this.chunkBoundaryMask = chunkBoundaryMask;
			this.chunkPattern = chunkPattern;
		}

		public boolean isBoundary(RabinFingerprintLong fingerprint) {
			return (fingerprint.getFingerprintLong() & chunkBoundaryMask) == chunkPattern;
		}
	}

	public static final ChunkBoundaryDetector DEFAULT_BOUNDARY_DETECTOR = new BitmaskBoundaryDetector(
			0x1FFF,0);
}
