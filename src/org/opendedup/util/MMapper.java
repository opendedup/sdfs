package org.opendedup.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import sun.nio.ch.FileChannelImpl;
import sun.misc.Unsafe;

public class MMapper {

	private static final Unsafe unsafe;
	private static final Method mmap;
	private static final Method unmmap;
	private static final int BYTE_ARRAY_OFFSET;

	private long addr, size;
	private final String loc;
	private FileChannel ch = null;
	private RandomAccessFile backingFile = null;
	static {
		try {
			Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
			singleoneInstanceField.setAccessible(true);
			unsafe = (Unsafe) singleoneInstanceField.get(null);

			mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
			unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
			BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
		} catch (Exception e){
			throw new RuntimeException(e);
		}
	}

	//Bundle reflection calls to get access to the given method
	private static Method getMethod(Class<?> cls, String name, Class<?>... params) throws Exception {
		Method m = cls.getDeclaredMethod(name, params);
		m.setAccessible(true);
		return m;
	}

	//Round to next 4096 bytes
	private static long roundTo4096(long i) {
		return (i + 0xfffL) & ~0xfffL;
	}

	//Given that the location and size have been set, map that location
	//for the given length and set this.addr to the returned offset
	private void mapAndSetOffset() throws Exception{
		backingFile = new RandomAccessFile(this.loc, "rw");
		backingFile.setLength(this.size);
		ch = FileChannelImpl.open(backingFile.getFD(), loc, true, true,true, null);
		this.addr = (long) mmap.invoke(ch, 1, 0L, this.size);

		
	}

	public MMapper(final String loc, long len) throws Exception {
		this.loc = loc;
		this.size = roundTo4096(len);
		mapAndSetOffset();
	}

	//Callers should synchronize to avoid calls in the middle of this, but
	//it is undesirable to synchronize w/ all access methods.
	public void remap(long nLen) throws Exception{
		this.close();
		this.size = roundTo4096(nLen);
		mapAndSetOffset();
	}

	public int getInt(long pos){
		return unsafe.getInt(pos + addr);
	}

	public long getLong(long pos){
		return unsafe.getLong(pos + addr);
	}

	public void putInt(long pos, int val){
		unsafe.putInt(pos + addr, val);
	}

	public void putLong(long pos, long val){
		unsafe.putLong(pos + addr, val);
	}

	//May want to have offset & length within data as well, for both of these
	public void getBytes(long pos, byte[] data){
		unsafe.copyMemory(null, pos + addr, data, BYTE_ARRAY_OFFSET, data.length);
	}

	public void setBytes(long pos, byte[] data){
		unsafe.copyMemory(data, BYTE_ARRAY_OFFSET, null, pos + addr, data.length);
	}
	
	public void close () throws Exception {
		ch.close();
		//backingFile.close();
		unmmap.invoke(null, addr, this.size);
	}
	
	public void force() throws IOException {
		ch.force(true);
	}
	
	public static void main(String [] args) throws Exception {
		MMapper mm = new MMapper("c:\\temp\\test.txt",1024);
		mm.setBytes(0, "testme".getBytes());
		mm.close();
		Thread.sleep(10000);
		File f = new File("c:\\temp\\test.txt");
		System.out.println(f.delete());
	}
}
