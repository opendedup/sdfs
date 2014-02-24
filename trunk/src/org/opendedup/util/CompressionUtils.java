package org.opendedup.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.lz4.LZ4Factory;

import org.iq80.snappy.Snappy;
import org.opendedup.sdfs.Main;

//import org.h2.compress.LZFInputStream;
//import org.h2.compress.LZFOutputStream;



public class CompressionUtils {
	static {
		byte[] b;
		b = Snappy.compress(new byte[4096]);
		Snappy.uncompress(b, 0, b.length);

	}
	static final LZ4Compressor lz4Compressor = LZ4Factory.fastestJavaInstance().fastCompressor();
	static final LZ4FastDecompressor lz4Decompressor = LZ4Factory.fastestJavaInstance().fastDecompressor();
	
	public static byte[] compressZLIB(byte[] input) throws IOException {
		// Create the compressor with highest level of compression
		Deflater compressor = new Deflater();
		compressor.setLevel(Deflater.BEST_SPEED);
		// Give the compressor the data to compress
		compressor.setInput(input);
		compressor.finish();
		// Create an expandable byte array to hold the compressed data.
		// You cannot use an array that's the same size as the orginal because
		// there is no guarantee that the compressed data will be smaller than
		// the uncompressed data.
		ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);
		// Compress the data
		byte[] buf = new byte[1024];
		while (!compressor.finished()) {
			int count = compressor.deflate(buf);
			bos.write(buf, 0, count);
		}
		bos.close();
		// Get the compressed data
		byte[] compressedData = bos.toByteArray();
		return compressedData;
	}

	public static byte[] decompressZLIB(byte[] input) throws IOException {
		Inflater decompressor = new Inflater();
		decompressor.setInput(input);
		// Create an expandable byte array to hold the decompressed data
		ByteArrayOutputStream bos = new ByteArrayOutputStream(input.length);

		// Decompress the data
		byte[] buf = new byte[1024];
		while (!decompressor.finished()) {
			try {
				int count = decompressor.inflate(buf);
				bos.write(buf, 0, count);
			} catch (DataFormatException e) {
				throw new IOException(e.toString());
			}
		}
		bos.close();

		// Get the decompressed data
		byte[] decompressedData = bos.toByteArray();
		return decompressedData;
	}

	public static byte[] compressSnappy(byte[] input) throws IOException {
		return Snappy.compress(input);
	}

	public static byte[] decompressSnappy(byte[] input) throws IOException {
		return Snappy.uncompress(input, 0, input.length);
	}
	
	public static byte[] compressLz4(byte[] input) throws IOException {
		return lz4Compressor.compress(input);
	}

	public static byte[] decompressLz4(byte[] input) throws IOException {
		return lz4Decompressor.decompress(input, Main.CHUNK_LENGTH);
	}
	
	public static byte[] decompressSnappy(byte[] input, int len) throws IOException {
		return Snappy.uncompress(input, 0, len);
	}

	public static void main(String[] args) throws IOException {
		String t = "This is a test";
		System.out.println(new String(decompressSnappy(compressSnappy(t
				.getBytes()))));
	}

}
