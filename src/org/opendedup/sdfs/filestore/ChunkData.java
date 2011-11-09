package org.opendedup.sdfs.filestore;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.bouncycastle.util.Arrays;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.HashChunkService;
import org.opendedup.util.HashFunctions;
import org.opendedup.util.SDFSLogger;
import org.opendedup.util.StringUtils;

/**
 * 
 * @author sam silverberg Chunk block meta data is as follows: [mark of deletion
 *         (1 byte)|hash lenth(2 bytes)|hash(32 bytes)|date added (8 bytes)|date
 *         last claimed (8 bytes)| number of times claimed (8 bytes)|chunk len
 *         (4 bytes)|chunk position (8 bytes)]
 * 
 */

public class ChunkData {
	public static final int RAWDL = 1 + 2 + 32 + 8 + 8 + 8 + 4 + 8;
	public static final int CLAIMED_OFFSET = 1 + 2 + 32 + 8;
	private static byte[] BLANKCM = new byte[RAWDL];
	private boolean mDelete = false;
	private short hashLen = 0;
	private byte[] hash = null;
	private long added = 0;
	private long lastClaimed = 0;
	private long numClaimed = 0;
	private int cLen = 0;
	private long cPos = 0;
	private byte[] chunk = null;

	private static byte[] blankHash = null;;

	static {
		Arrays.fill(BLANKCM, (byte) 0);
		try {
			blankHash = HashFunctions
					.getTigerHashBytes(new byte[Main.chunkStorePageSize]);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public ChunkData(long cPos) {
		this.cPos = cPos;
		this.mDelete = true;
	}

	public ChunkData(byte[] rawData) {
		ByteBuffer buf = ByteBuffer.wrap(rawData);
		byte del = buf.get();
		if (del == 0)
			this.mDelete = false;
		else
			this.mDelete = true;
		this.hashLen = buf.getShort();
		this.hash = new byte[hashLen];
		buf.get(hash);
		buf.get(new byte[32 - hashLen]);
		added = buf.getLong();
		lastClaimed = buf.getLong();
		this.numClaimed = buf.getLong();
		cLen = buf.getInt();
		cPos = buf.getLong();
	}

	public ChunkData(byte[] hash, int chunkLen, byte[] chunk) {
		long tm = System.currentTimeMillis();
		this.added = tm;
		this.lastClaimed = tm;
		this.numClaimed = 1;
		this.mDelete = false;
		this.hashLen = (short) hash.length;
		this.hash = hash;
		this.cLen = chunkLen;
		this.cPos = -1;
		this.chunk = chunk;
	}

	public ChunkData(byte[] hash, long pos) {
		long tm = System.currentTimeMillis();
		this.added = tm;
		this.lastClaimed = tm;
		this.numClaimed = 1;
		this.mDelete = false;
		this.hashLen = (short) hash.length;
		this.hash = hash;
		this.cLen = Main.CHUNK_LENGTH;
		this.cPos = pos;
		this.chunk = null;
	}

	public ByteBuffer getMetaDataBytes() {
		ByteBuffer buf = ByteBuffer.wrap(new byte[RAWDL]);
		if(this.mDelete)
			buf.put((byte) 1);
		else
			buf.put((byte) 0);
		buf.putShort(this.hashLen);
		buf.put(hash);
		buf.put(new byte[32 - this.hashLen]);
		buf.putLong(this.added);
		buf.putLong(this.lastClaimed);
		buf.putLong(this.numClaimed);
		buf.putInt(this.cLen);
		buf.putLong(cPos);
		buf.position(0);
		return buf;
	}

	public void persistData(boolean clear) throws IOException {
		if (this.chunk != null) {
			if (cPos == -1) {
				this.cPos = HashChunkService.getChuckStore()
						.reserveWritePosition(cLen);
			}
			if (this.mDelete) {
				chunk = new byte[cLen];
				HashChunkService.getChuckStore()
				.deleteChunk(hash, cPos,cLen);
			}else {
				HashChunkService.getChuckStore()
					.writeChunk(hash, chunk, cLen, cPos);
			}
			if (clear)
				this.chunk = null;
		}
	}

	public boolean ismDelete() {
		return mDelete;
	}

	public void setmDelete(boolean mDelete) {
		this.mDelete = mDelete;
		if (this.mDelete && Main.AWSChunkStore) {
			try {
				HashChunkService.getChuckStore().deleteChunk(this.hash, 0, 0);
			} catch (IOException e) {
				SDFSLogger.getLog().error(
						"Unable to remove hash ["
								+ StringUtils.getHexString(this.hash) + "]", e);
			}
		}
	}

	public long getLastClaimed() {
		return lastClaimed;
	}

	public void setLastClaimed(long lastClaimed) {
		this.lastClaimed = lastClaimed;
	}

	public long getNumClaimed() {
		return this.numClaimed;
	}

	public void updateNumClaimed(int num) {
		this.numClaimed += num;
	}

	public long getcPos() {
		return cPos;
	}

	public void setcPos(long cPos) {
		this.cPos = cPos;
	}

	public short getHashLen() {
		return hashLen;
	}

	public byte[] getHash() {
		return hash;
	}

	public static byte[] getChunk(byte[] hash, long pos) throws IOException {
		try {
			return HashChunkService.getChuckStore().getChunk(hash, pos,
					Main.chunkStorePageSize);
		} catch (IOException e) {
			if (Arrays.areEqual(hash, blankHash))
				return new byte[Main.chunkStorePageSize];
			else
				throw e;
		}

	}

	public byte[] getData() throws IOException {
		if (this.chunk == null) {
			return HashChunkService.getChuckStore().getChunk(hash, this.cPos,
					this.cLen);
		} else
			return chunk;
	}

	public long getAdded() {
		return added;
	}

	public int getcLen() {
		return cLen;
	}
}
