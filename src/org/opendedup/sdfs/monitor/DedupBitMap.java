package org.opendedup.sdfs.monitor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;

import org.opendedup.sdfs.Main;

public class DedupBitMap {
	BitSet set = new BitSet();

	public void setDup(long pos) {
		int bpos = (int) (pos / Main.CHUNK_LENGTH);
		set.clear(bpos);
	}

	public void setUnique(long pos) {
		int bpos = (int) (pos / Main.CHUNK_LENGTH);
		set.set(bpos);
	}

	public long getUniqueSz() {
		return set.cardinality() * Main.CHUNK_LENGTH;
	}

	public void serialize(String path) throws IOException {
		String file = path + File.separator + "dedupmap.bst";
		FileOutputStream out = new FileOutputStream(file);
		ObjectOutputStream obj = new ObjectOutputStream(out);
		obj.writeObject(this);
		obj.close();
	}

	public static DedupBitMap marshall(String path) throws IOException,
			ClassNotFoundException {
		String file = path + File.separator + "dedupmap.bst";
		FileInputStream in = new FileInputStream(file);
		ObjectInputStream obj = new ObjectInputStream(in);
		DedupBitMap map = (DedupBitMap) obj.readObject();
		obj.close();
		return map;
	}

}
