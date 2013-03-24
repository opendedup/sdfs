package org.opendedup.sdfs.io;

public interface DedupFileListener {
	void onCreate(DedupFile file);
	void onCopyTo(String dst, DedupFile file);
	void onAddLock(long position,long len, boolean shared,DedupFileLock lock,DedupFile file);
	void onCreateBlankFile(long size, DedupFile file);
	void onDelete(DedupFile file);
	void onForceClose(DedupFile file);
	void onRemoveHash(long position,DedupFile file);
	void onRemoveLock(DedupFileLock lock,DedupFile file);
	void onSnapShot(MetaDataDedupFile mf,DedupFile file);
	void onSync(DedupFile file);
	void onTruncate(long length,DedupFile file);
	void onUpdateMap (DedupChunkInterface writeBuffer, byte[] hash,
			boolean doop,DedupFile file);
}
