package org.opendedup.sdfs.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class WriteHashCmd implements IOCmd {
	byte[] hash;
	byte[] aContents;
	int position;
	int len;
	boolean written = false;
	boolean compress = false;

	public WriteHashCmd(byte[] hash, byte[] aContents, int len, boolean compress)
			throws IOException {
		this.hash = hash;
		this.compress = compress;
		if (compress) {
			throw new IOException("not implemented");
			/*
			 * try { byte[] compB = CompressionUtils.compress(aContents); if
			 * (compB.length <= aContents.length) { this.aContents = compB;
			 * this.len = this.aContents.length; } else { this.compress = false;
			 * this.aContents = aContents; this.len = len; } } catch
			 * (IOException e) { // TODO Auto-generated catch block
			 * e.printStackTrace(); this.aContents = aContents; this.len = len;
			 * this.compress = false; }
			 */
		} else {
			this.aContents = aContents;
			this.len = len;
		}

	}

	@Override
	public void executeCmd(DataInputStream is, DataOutputStream os)
			throws IOException {
		if (compress)
			os.write(NetworkCMDS.WRITE_COMPRESSED_CMD);
		else
			os.write(NetworkCMDS.WRITE_HASH_CMD);
		os.writeShort(hash.length);
		os.write(hash);
		os.writeInt(len);
		os.write(aContents);
		os.flush();
		this.written = is.readBoolean();
		aContents = null;
	}

	public boolean wasWritten() {
		return this.written;
	}

	@Override
	public byte getCmdID() {
		return NetworkCMDS.WRITE_HASH_CMD;
	}

}
