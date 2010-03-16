package org.opendedup.sdfs.filestore;

public class ChunkEvent {
	private byte[] hash;
	private long oldLocation;
	private long newLocation;
	private int length;
	private AbstractChunkStore source;

	public ChunkEvent(byte[] hash, long oldLocation, long newLocation,
			int length, AbstractChunkStore source) {
		this.setHash(hash);
		this.setOldLocation(oldLocation);
		this.setNewLocation(newLocation);
		this.setLength(length);
		this.setSource(source);

	}

	private void setHash(byte[] hash) {
		this.hash = hash;
	}

	public byte[] getHash() {
		return hash;
	}

	private void setNewLocation(long newLocation) {
		this.newLocation = newLocation;
	}

	public long getNewLocation() {
		return newLocation;
	}

	private void setOldLocation(long oldLocation) {
		this.oldLocation = oldLocation;
	}

	public long getOldLocation() {
		return oldLocation;
	}

	private void setLength(int length) {
		this.length = length;
	}

	public int getLength() {
		return length;
	}

	private void setSource(AbstractChunkStore source) {
		this.source = source;
	}

	public AbstractChunkStore getSource() {
		return source;
	}

}
