package org.opendedup.collections;

public interface LongByteArrayMapListener {
	void onCopyEvent(String target, DataMapInterface src);

	void onPutEvent(long position, byte[] data, DataMapInterface src);

	void onRemoveEvent(long position, DataMapInterface src);

	void onTruncateEvent(long position, DataMapInterface src);

	void onVanishEvent(DataMapInterface src);

	void onSync(DataMapInterface src);
}
