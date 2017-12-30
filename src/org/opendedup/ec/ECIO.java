package org.opendedup.ec;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;

import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RSRawErasureCoderFactory;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureEncoder;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;


public class ECIO {
	private int dataBuffers;
	private int parityBuffers;
	private RawErasureEncoder encoder;
	private RawErasureDecoder decoder;
	HashFunction hf = Hashing.sipHash24();
	private static final int MAX_SIZE=256*1024*1024; //256MB Max Size
	
	public static void main(String [] args) {
		ECIO  ec = new ECIO(3,1);
		Random rnd = new Random();
		byte [] k = new byte[32*1024];
		rnd.nextBytes(k);
		ByteBuffer [] zk = ec.encode(k);
		ec.decode(zk);
	}
	
	public ECIO(int dataBuffers,int parityBuffers) {
		this.dataBuffers = dataBuffers;
		this.parityBuffers = parityBuffers;
		ErasureCoderOptions coderOptions = new ErasureCoderOptions(
				dataBuffers, parityBuffers);
		try {
			encoder = new NativeRSRawErasureCoderFactory().createEncoder(coderOptions);
			decoder =  new NativeRSRawErasureCoderFactory().createDecoder(coderOptions);
		   
		}catch(ExceptionInInitializerError e) {
			encoder = new RSRawErasureCoderFactory().createEncoder(coderOptions);
			decoder =  new RSRawErasureCoderFactory().createDecoder(coderOptions);
		}
	}
	
	public ByteBuffer[] encode(byte [] data) {
		ByteBuffer bk = ByteBuffer.allocate(data.length + 8+ 4);
		bk.putInt(data.length);
		bk.putLong(hf.hashBytes(data).asLong());
		bk.put(data);
		int bl = roundUp(data.length,this.dataBuffers);
		byte [][] cks = new byte[this.dataBuffers][bl];
		byte [][] pb = new byte[this.parityBuffers][bl];
		for(int i = 0;i<cks.length;i++) {
			if(bk.remaining() < cks[i].length) {
				
				bk.get(cks[i], 0, bk.remaining());
			} else {
				bk.get(cks[i]);
			}
		}
		encoder.encode(cks, pb);
		ByteBuffer[] dBuffers = new ByteBuffer[this.dataBuffers + this.parityBuffers];
		for(int i = 0;i<this.dataBuffers;i++) {
			dBuffers[i] = ByteBuffer.wrap(cks[i]);
		}
		for(int i = this.dataBuffers;i<dBuffers.length;i++) {
			dBuffers[i] = ByteBuffer.wrap(pb[i-this.dataBuffers]);
		}
		return dBuffers;
	}
	
	public byte [] decode(ByteBuffer[] data) {
		int dl = 0;
		for(ByteBuffer bk : data) {
			if(bk.capacity() > dl)
				dl = bk.capacity();
		}
		int [] pb = new int[0];
		
		ByteBuffer[] ob = new ByteBuffer[this.dataBuffers];
		for(int i = 0;i<this.dataBuffers;i++) {
			ob[i]= ByteBuffer.allocate(dl);
		}
		this.decoder.decode(data, pb, ob);
		ByteBuffer _im = ByteBuffer.allocate(ob[0].capacity()*ob.length);
		for(ByteBuffer bk : ob) {
			_im.put(bk);
		}
		_im.position(0);
		int sz = _im.getInt();
		long hv = _im.getLong();
		byte [] out = new byte[sz];
		_im.get(out);
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
