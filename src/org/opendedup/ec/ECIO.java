package org.opendedup.ec;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

public class ECIO {
	private int dataBuffers;
	private int parityBuffers;
	private RawErasureEncoder encoder;
	private RawErasureDecoder decoder;
	private HashFunction hf = Hashing.sipHash24();

	public static void main(String[] args) throws IOException {
		ECIO ec = new ECIO(3, 2);
		File [] fls =ec.encode(new File("c:/temp/pingme.txt"),new File("c:/temp/"));
		fls[1]=null;
		fls[3]=null;
		ec.decode(fls, new File("c:/temp/test2.txt"));
		
	}

	public ECIO(int dataBuffers, int parityBuffers) {
		this.dataBuffers = dataBuffers;
		this.parityBuffers = parityBuffers;
		ErasureCoderOptions coderOptions = new ErasureCoderOptions(dataBuffers, parityBuffers);
		try {
			encoder = new NativeRSRawErasureCoderFactory().createEncoder(coderOptions);
			decoder = new NativeRSRawErasureCoderFactory().createDecoder(coderOptions);

		} catch (ExceptionInInitializerError e) {
			encoder = new RSRawErasureCoderFactory().createEncoder(coderOptions);
			decoder = new RSRawErasureCoderFactory().createDecoder(coderOptions);
		}
	}

	public ByteBuffer[] encode(byte[] data) {
		ByteBuffer bk = ByteBuffer.allocate(data.length + 8 + 4);
		bk.putInt(data.length);
		bk.putLong(hf.hashBytes(data).asLong());
		bk.put(data);
		bk.position(0);
		int bl = roundUp(bk.capacity(), this.dataBuffers);
		byte[][] cks = new byte[this.dataBuffers][bl];
		byte[][] pb = new byte[this.parityBuffers][bl];
		for (int i = 0; i < cks.length; i++) {
			if (bk.remaining() < cks[i].length) {

				bk.get(cks[i], 0, bk.remaining());
			} else {
				bk.get(cks[i]);
			}
		}
		encoder.encode(cks, pb);
		ByteBuffer[] dBuffers = new ByteBuffer[this.dataBuffers + this.parityBuffers];
		for (int i = 0; i < this.dataBuffers; i++) {
			dBuffers[i] = ByteBuffer.wrap(cks[i]);
		}
		for (int i = this.dataBuffers; i < dBuffers.length; i++) {
			dBuffers[i] = ByteBuffer.wrap(pb[i - this.dataBuffers]);
		}
		return dBuffers;
	}

	public File[] encode(File data,File destDir) throws IOException {
		//System.out.println(data.length());
		byte[] fdata = Files.toByteArray(data);
		ByteBuffer[] bk = encode(fdata);
		File[] fls = new File[bk.length];
		for (int i = 0; i < bk.length; i++) {
			File f = new File(destDir, data.getName() + "." + i);
			@SuppressWarnings("resource")
			FileChannel ch = new FileOutputStream(f, false).getChannel();
			bk[i].position(0);
			ch.write(bk[i]);
			ch.close();
			fls[i] = f;
		}
		return fls;
	}

	public void decode(File[] input, File output) throws IOException {
		ByteBuffer[] bf = new ByteBuffer[input.length];
		for (int i = 0; i < input.length; i++) {
			File f = input[i];
			if(f !=null && f.exists() && f.length() > 0) {
				byte[] data = Files.toByteArray(f);
				bf[i] = ByteBuffer.wrap(data);
			} else {
				bf[i] = null;
			}
			//System.out.println(data.length +  " " + f.length());
		}
		byte[] b = this.decode(bf);
		FileOutputStream out = new FileOutputStream(output, false);
		out.write(b);
		out.flush();
		out.close();
	}

	public byte[] decode(ByteBuffer[] data) throws IOException {
		int dl = 0;
		ArrayList<Integer> missing = new ArrayList<Integer>();
		for (int i = 0; i < this.dataBuffers; i++) {
			ByteBuffer bk = data[i];
			if (bk == null) {
				missing.add(i);
			}
			else if (bk.capacity() > dl)
				dl = bk.capacity();
		}
		if (missing.size() > 0) {
			int[] pb = new int[missing.size()];

			for (int i = 0; i < missing.size(); i++) {
				pb[i] = missing.get(i);
			}
			ByteBuffer[] ob = new ByteBuffer[pb.length];
			for (int i = 0; i < pb.length; i++) {
				ob[i] = ByteBuffer.allocate(dl);
			}
			this.decoder.decode(data, pb, ob);
			for (int i = 0; i < pb.length; i++) {
				data[pb[i]] = ob[i];
			}
		}
		//System.out.println(dl + " " + this.dataBuffers);
		ByteBuffer _im = ByteBuffer.allocate(dl * this.dataBuffers);
		for (int i = 0; i < this.dataBuffers; i++) {
			ByteBuffer bk = data[i];
			bk.position(0);
			_im.put(bk);
		}
		_im.position(0);
		int sz = _im.getInt();
		long hv =_im.getLong();
		byte[] out = new byte[sz];
		//System.out.println(out.length);
		
		_im.get(out);
		if(hf.hashBytes(out).asLong() != hv)
			throw new IOException("Erasure decoding mismatch expected hv=" + hv + " actual hv=" + hf.hashBytes(out).asLong());
		return out;
	}

	public static int roundUp(int num, int divisor) {
		return (num + divisor - 1) / divisor;
	}

	public int getDataBuffers() {
		return this.dataBuffers;
	}

	public int getParityBuffers() {
		return this.parityBuffers;
	}

}
