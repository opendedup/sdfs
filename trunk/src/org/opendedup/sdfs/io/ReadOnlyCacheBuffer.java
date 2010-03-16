package org.opendedup.sdfs.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import org.opendedup.sdfs.Main;

public class ReadOnlyCacheBuffer extends DedupChunk {

	byte[] fileContents = null;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ReadOnlyCacheBuffer(DedupChunk dk, DedupFile df) throws IOException {
		super(dk.getHash(), dk.getFilePosition(), dk.getLength(), dk
				.isNewChunk());
		if (Main.safeSync) {
			StringBuffer sb = new StringBuffer();
			sb.append(df.getDatabaseDirPath());
			sb.append(File.separator);
			sb.append(dk.getFilePosition());
			sb.append(".chk");
			Path blockFile = Paths.get(sb.toString());
			try {
				this.fileContents = this.readBlockFile(blockFile);
			} catch (Exception e) {
				this.fileContents = null;
			}

			blockFile = null;
			sb = null;
		}
		if (this.fileContents == null) {
			this.fileContents = dk.getChunk();
		}
	}

	private byte[] readBlockFile(Path blockFile) throws IOException {
		SeekableByteChannel fc = (SeekableByteChannel) blockFile
				.newByteChannel(StandardOpenOption.READ);
		byte[] b = new byte[(int) fc.size()];
		ByteBuffer buf = ByteBuffer.wrap(b);
		fc.read(buf);
		fc.close();
		fc = null;
		return buf.array();
	}

	public byte[] getChunk() throws IOException {
		return fileContents;

	}

}
