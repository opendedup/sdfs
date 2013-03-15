package org.opendedup.collections;

public interface LongByteArrayMapListener {
	void onCopyEvent(String target,LongByteArrayMap src);
	void onPutEvent(long position,byte [] data,LongByteArrayMap src);
	void onRemoveEvent(long position,LongByteArrayMap src);
	void onTruncateEvent(long position,LongByteArrayMap src);
	void onVanishEvent(LongByteArrayMap src);
	void onSync(LongByteArrayMap src);
}
