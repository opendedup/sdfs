package org.rabinfingerprint.handprint;

import java.io.InputStream;

import org.rabinfingerprint.handprint.FingerFactory.ChunkBoundaryDetector;
import org.rabinfingerprint.polynomial.Polynomial;

public class Handprints {
	@SuppressWarnings("serial")
	public static class HandprintException extends RuntimeException {
		public HandprintException(String msg, Throwable wrapped) {
			super(msg, wrapped);
		}
	}
	
	public static HandPrintFactory newFactory(Polynomial p) {
		return new HandPrintFactory(p);
	}

	public static class HandPrintFactory {
		private final Polynomial p;
		private int fingersPerHand = 10;
		private long bytesPerWindow = 48;
		private ChunkBoundaryDetector boundaryDetector = BoundaryDetectors.DEFAULT_BOUNDARY_DETECTOR;
		
		public HandPrintFactory(Polynomial p) {
			this.p = p;
		}

		public HandPrintFactory bytesPerWindow(long bytesPerWindow) {
			this.bytesPerWindow = bytesPerWindow;
			return this;
		}

		public HandPrintFactory setBoundaryDetector(ChunkBoundaryDetector boundaryDetector) {
			this.boundaryDetector = boundaryDetector;
			return this;
		}
		
		public Handprint newHandprint(InputStream is){
			return new Handprint(is,
					fingersPerHand,
					new FingerFactory(p, bytesPerWindow, boundaryDetector));
		}
	}
}
