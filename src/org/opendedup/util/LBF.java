package org.opendedup.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.locks.ReentrantLock;

import org.opendedup.collections.BloomFileByteArrayLongMap.KeyBlob;

import com.google.common.hash.BloomFilter;

public class LBF implements Serializable {
	private static final long serialVersionUID = 1L;
	public transient BloomFilter<KeyBlob> bfs = null;
	public transient ReentrantLock l = new ReentrantLock();

	LBF(BloomFilter<KeyBlob> bf) {
		this.bfs = bf;
	}

	@SuppressWarnings("unchecked")
	LBF(File f) throws IOException, ClassNotFoundException {
		if (!f.exists()) {
			throw new IOException("bf does not exist " + f.getPath());
		}
		FileInputStream fin = new FileInputStream(f);
		ObjectInputStream oon = new ObjectInputStream(fin);
		BloomFilter<KeyBlob> kb = (BloomFilter<KeyBlob>) oon.readObject();
		bfs = kb;
		oon.close();
		f.delete();
	}

	public boolean mightContain(KeyBlob kb) {
		l.lock();
		try {
			return bfs.mightContain(kb);
		} finally {
			l.unlock();
		}
	}

	public void put(KeyBlob kb) {
		l.lock();
		try {
			bfs.put(kb);
		} finally {
			l.unlock();
		}
	}
	
	public void save(File f) throws IOException {
		FileOutputStream fout = new FileOutputStream(f);
		ObjectOutputStream oon = new ObjectOutputStream(fout);
		oon.writeObject(this.bfs);
		oon.flush();
		oon.close();
		fout.flush();
		fout.close();
	}

}
