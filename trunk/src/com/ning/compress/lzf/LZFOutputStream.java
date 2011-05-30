package com.ning.compress.lzf;

import java.io.IOException;
import java.io.OutputStream;

public class LZFOutputStream extends OutputStream 
{
	private static int OUTPUT_BUFFER_SIZE = LZFChunk.MAX_CHUNK_LEN;
	
	protected final OutputStream outputStream;
	protected byte[] outputBuffer = new byte[OUTPUT_BUFFER_SIZE];
	protected int position = 0;
	
	public LZFOutputStream(final OutputStream outputStream)
	{
		this.outputStream = outputStream;
	}
	
	@Override
	public void write(final int singleByte) throws IOException 
	{
		if(position >= outputBuffer.length) {
			writeCompressedBlock();
		}
		outputBuffer[position++] = (byte) singleByte;
	}

	@Override
	public void write(final byte[] buffer, final int offset, final int length) throws IOException
	{
		int inputCursor = offset;
		int remainingBytes = length;
		while(remainingBytes > 0) {
			if(position >= outputBuffer.length) {
				writeCompressedBlock();
			}
			int chunkLength = (remainingBytes > (outputBuffer.length - position))?outputBuffer.length - position: remainingBytes;
			System.arraycopy(buffer, inputCursor, outputBuffer, position, chunkLength);
			position += chunkLength;
			remainingBytes -= chunkLength;
			inputCursor += chunkLength;
		}
	}
	
	@Override
	public void flush() throws IOException
	{
	    writeCompressedBlock();
	    outputStream.flush();
	}
	
	@Override
	public void close() throws IOException  
	{
	    try {
	        flush();
	    } finally {
	        outputStream.close();
	    }
	}

	/** 
	 * Compress and write the current block to the OutputStream
	 */
	private void writeCompressedBlock() throws IOException
	{
	    if (position > 0) {
		final byte[] compressedBytes = LZFEncoder.encode(outputBuffer, position);
		outputStream.write(compressedBytes);
		position = 0;
	    }
	}
}
