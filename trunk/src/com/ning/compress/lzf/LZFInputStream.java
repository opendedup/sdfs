package com.ning.compress.lzf;

import java.io.IOException;
import java.io.InputStream;

public class LZFInputStream extends InputStream {
	public static int EOF_FLAG = -1;

	/**
	 * stream to be decompressed
	 * */
	protected final InputStream inputStream;

	/**
	 * Flag that indicates whether we force full reads (reading of as many bytes
	 * as requested), or 'optimal' reads (up to as many as available, but at
	 * least one). Default is false, meaning that 'optimal' read is used.
	 */
	protected boolean cfgFullReads = false;

	/* the current buffer of compressed bytes */
	private final byte[] compressedBytes = new byte[LZFChunk.MAX_CHUNK_LEN];

	/* the buffer of uncompressed bytes from which */
	private final byte[] uncompressedBytes = new byte[LZFChunk.MAX_CHUNK_LEN];

	/*
	 * The current position (next char to output) in the uncompressed bytes
	 * buffer.
	 */
	private int bufferPosition = 0;

	/* Length of the current uncompressed bytes buffer */
	private int bufferLength = 0;

	public LZFInputStream(final InputStream inputStream) throws IOException {
		this(inputStream, false);
	}

	/**
	 * @param inputStream
	 *            Underlying input stream to use
	 * @param fullReads
	 *            Whether {@link #read(byte[])} should try to read exactly as
	 *            many bytes as requested (true); or just however many happen to
	 *            be available (false)
	 */
	public LZFInputStream(final InputStream inputStream, boolean fullReads)
			throws IOException {
		super();
		this.inputStream = inputStream;
		cfgFullReads = fullReads;
	}

	@Override
	public int read() throws IOException {
		int returnValue = EOF_FLAG;
		readyBuffer();
		if (bufferPosition < bufferLength) {
			returnValue = (uncompressedBytes[bufferPosition++] & 255);
		}
		return returnValue;
	}

	public int read(final byte[] buffer) throws IOException {
		return (read(buffer, 0, buffer.length));

	}

	public int read(final byte[] buffer, int offset, int length)
			throws IOException {
		if (length < 1) {
			return 0;
		}
		readyBuffer();
		if (bufferLength == -1) {
			return -1;
		}
		// First let's read however much data we happen to have...
		int chunkLength = Math.min(bufferLength - bufferPosition, length);
		System.arraycopy(uncompressedBytes, bufferPosition, buffer, offset,
				chunkLength);
		bufferPosition += chunkLength;

		if (chunkLength == length || !cfgFullReads) {
			return chunkLength;
		}
		// Need more data, then
		int totalRead = chunkLength;
		do {
			offset += chunkLength;
			readyBuffer();
			if (bufferLength == -1) {
				break;
			}
			chunkLength = Math.min(bufferLength - bufferPosition,
					(length - totalRead));
			System.arraycopy(uncompressedBytes, bufferPosition, buffer, offset,
					chunkLength);
			bufferPosition += chunkLength;
			totalRead += chunkLength;
		} while (totalRead < length);

		return totalRead;
	}

	public void close() throws IOException {
		bufferPosition = bufferLength = 0;
		inputStream.close();
	}

	/**
	 * Fill the uncompressed bytes buffer by reading the underlying inputStream.
	 * 
	 * @throws IOException
	 */
	private final void readyBuffer() throws IOException {
		if (bufferPosition >= bufferLength) {
			bufferLength = LZFDecoder.decompressChunk(inputStream,
					compressedBytes, uncompressedBytes);
			bufferPosition = 0;
		}
	}
}
