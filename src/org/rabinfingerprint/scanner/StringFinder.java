package org.rabinfingerprint.scanner;

import java.io.File;

import org.rabinfingerprint.fingerprint.RabinFingerprintLongWindowed;
import org.rabinfingerprint.polynomial.Polynomial;

public class StringFinder {

	private final Polynomial polynomial;
	private final String target;
	private final long targetFingerprint;
	private final RabinFingerprintLongWindowed rabin;

	public StringFinder(String target) {
		this.polynomial = Polynomial.createIrreducible(53);
		this.target = target;

		// calculate target fingerprint
		this.rabin = new RabinFingerprintLongWindowed(polynomial, target.length());
		rabin.pushBytes(target.getBytes());
		this.targetFingerprint = rabin.getFingerprintLong();
	}

	public StringMatcher matcher(String string) {
		return new StringMatcher(string);
	}

	public final class StringMatcher {
		private final RabinFingerprintLongWindowed localRabin;
		private final String string;
		private final byte[] bytes;
		private int offset = 0;
		private int start = -1;
		private int end = -1;

		private StringMatcher(String string) {
			this.localRabin = new RabinFingerprintLongWindowed(rabin);
			this.string = string;
			this.bytes = string.getBytes();
		}

		public boolean find() {
			for (; offset < bytes.length; offset++) {
				localRabin.pushByte(bytes[offset]);
				if (localRabin.getFingerprintLong() == targetFingerprint) {
					final int i0 = offset - target.length() + 1;
					final int i1 = offset + 1;
					if (i0 < 0 || i1 >= string.length()) continue;
					final String substring = string.substring(i0, i1);
					if (substring.equals(target)) {
						start = i0;
						end = i1;
						return true;
					}
				}
			}
			return false;
		}
		
		public int getStart() {
			return start;
		}
		
		public int getEnd() {
			return end;
		}
	}

	public static class StringMatch {
		private final File file;
		private final String line;
		private final int lineOffset;
		private final int characterOffset;

		public StringMatch(File file, String line, int lineOffset, int characterOffset) {
			this.file = file;
			this.line = line;
			this.lineOffset = lineOffset;
			this.characterOffset = characterOffset;
		}

		public File getFile() {
			return file;
		}

		public String getLine() {
			return line;
		}

		public int getLineOffset() {
			return lineOffset;
		}

		public int getCharacterOffset() {
			return characterOffset;
		}

		@Override
		public String toString() {
			return String.format("%d:%s", lineOffset, line);
		}
	}

	public static interface StringMatchVisitor {
		public void found(StringMatch match);
	}

}
