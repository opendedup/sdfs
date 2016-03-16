package org.opendedup.sdfs.filestore.cloud;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

public class Serialization {
	/**
	 * Stores the contents of a map in an output stream, as part of
	 * serialization. It does not support concurrent maps whose content may
	 * change while the method is running.
	 *
	 * <p>
	 * The serialized output consists of the number of entries, first key, first
	 * value, second key, second value, and so on.
	 */
	static <K, V> void writeMap(Map<K, V> map, ObjectOutputStream stream)
			throws IOException {
		stream.writeInt(map.size());
		for (Map.Entry<K, V> entry : map.entrySet()) {
			stream.writeObject(entry.getKey());
			stream.writeObject(entry.getValue());
		}
		stream.close();
	}

	/**
	 * Populates a map by reading an input stream, as part of deserialization.
	 * See {@link #writeMap} for the data format.
	 */
	static <K, V> void populateMap(Map<K, V> map, ObjectInputStream stream)
			throws IOException, ClassNotFoundException {
		int size = stream.readInt();
		populateMap(map, stream, size);
	}

	/**
	 * Populates a map by reading an input stream, as part of deserialization.
	 * See {@link #writeMap} for the data format. The size is determined by a
	 * prior call to {@link #readCount}.
	 */
	static <K, V> void populateMap(Map<K, V> map, ObjectInputStream stream,
			int size) throws IOException, ClassNotFoundException {
		for (int i = 0; i < size; i++) {
			@SuppressWarnings("unchecked")
			// reading data stored by writeMap
			K key = (K) stream.readObject();
			@SuppressWarnings("unchecked")
			// reading data stored by writeMap
			V value = (V) stream.readObject();
			map.put(key, value);
		}
		stream.close();
	}

}
