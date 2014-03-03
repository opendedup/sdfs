package org.rabinfingerprint;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.rabinfingerprint.fingerprint.RabinFingerprintLong;
import org.rabinfingerprint.fingerprint.RabinFingerprintLongWindowed;
import org.rabinfingerprint.polynomial.Polynomial;

import com.google.common.io.ByteStreams;

public class Samples {
	public static void fingerprint() throws FileNotFoundException, IOException {
		// Create new random irreducible polynomial
		// These can also be created from Longs or hex Strings
		Polynomial polynomial = Polynomial.createIrreducible(53);
		
		// Create a fingerprint object
		RabinFingerprintLong rabin = new RabinFingerprintLong(polynomial);
		
		// Push bytes from a file stream
		rabin.pushBytes(ByteStreams.toByteArray(new FileInputStream("file.test")));
		
		// Get fingerprint value and output
		System.out.println(Long.toString(rabin.getFingerprintLong(), 16));
	}
	
	public static void slidingWindowFingerprint() throws FileNotFoundException, IOException {
		// Create new random irreducible polynomial
		// These can also be created from Longs or hex Strings
		Polynomial polynomial = Polynomial.createIrreducible(53);

		// Create a windowed fingerprint object with a window size of 48 bytes.
		RabinFingerprintLongWindowed window = new RabinFingerprintLongWindowed(polynomial, 48);
		for (byte b : ByteStreams.toByteArray(new FileInputStream("file.test"))) {
			// Push in one byte. Old bytes are automatically popped.
			window.pushByte(b);

			// Output current window's fingerprint
			System.out.println(Long.toString(window.getFingerprintLong(), 16));
		}
	}
}
