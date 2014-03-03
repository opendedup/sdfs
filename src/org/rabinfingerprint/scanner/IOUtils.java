package org.rabinfingerprint.scanner;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class IOUtils {

	public static String readEntireFile(File file) throws IOException {
		return new String(readBytes(file));
	}

	public static String readEntireUnicodeFile(File file) throws IOException {
		return new String(readBytes(file), "UTF8");
	}

	private static byte[] readBytes(File file) throws FileNotFoundException, IOException {
		// read entire file into string
		byte[] buffer = new byte[(int) file.length()];
		BufferedInputStream f = new BufferedInputStream(new FileInputStream(file));
		try {
			f.read(buffer);
		} finally {
			f.close();
		}
		return buffer;
	}

	public static void writeEntireFile(File file, String contents) throws IOException {
		// read entire file into string
		BufferedOutputStream f = new BufferedOutputStream(new FileOutputStream(file));
		try {
			f.write(contents.getBytes());
		} finally {
			f.close();
		}
	}
	
	public static String toCamelCase(String s) {
		if (Character.isUpperCase(s.charAt(0))) {
			return Character.toLowerCase(s.charAt(0)) + s.substring(1, s.length());
		} else {
			return s;
		}
	}

}
