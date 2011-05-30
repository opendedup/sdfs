package org.opendedup.sdfs.io;

/**
 * 
 * @author annesam Lock Object for DedupFiles. Locks do not actually prevent
 *         writing to locked space but rather reserve space so that other write
 *         threads know an area is locked from writing.
 */
public class DedupFileLock {

	private DedupFileChannel channel;
	private long position;
	private long size;
	private boolean shared;
	private boolean valid;

	/**
	 * Instantiates a DedupFileLock
	 * 
	 * @param ch
	 *            DedupFileChannel associated with this lock.
	 * @param position
	 *            the position where the lock start position is located.
	 * @param len
	 *            the length of the lock
	 * @param shared
	 *            if the lock is shared for reading or not.
	 */
	public DedupFileLock(DedupFileChannel ch, long position, long len,
			boolean shared) {
		this.channel = ch;
		this.position = position;
		this.size = len;
		this.shared = shared;
		this.valid = true;

	}

	/**
	 * 
	 * @return the channel associated with this lock
	 */
	public DedupFileChannel channel() {
		return this.channel;
	}

	/**
	 * 
	 * @return the size or length of the lock
	 */
	public long size() {
		return this.size;
	}

	/**
	 * 
	 * @return If the lock is shared or not
	 */
	public boolean isShared() {
		return this.shared;
	}

	/**
	 * 
	 * @return true if the lock is still valid
	 */
	public boolean isValid() {
		return this.valid;
	}

	/**
	 * sets the lock it valid = false.
	 */
	public void release() {
		this.valid = false;
	}

	/**
	 * checks to see if two locks overlap
	 * 
	 * @param pos
	 *            the proposed position of the lock
	 * @param sz
	 *            the size of the proposed lock
	 * @return true if it overlaps
	 */
	public boolean overLaps(long pos, long sz) {
		long endPos = this.position + this.size;
		long pEndPos = pos + sz;
		if (pos >= this.position && pos <= endPos)
			return true;
		if (pEndPos >= this.position && pEndPos <= endPos)
			return true;
		return false;
	}

}
