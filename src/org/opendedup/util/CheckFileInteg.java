package org.opendedup.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.hadoop.util.StringUtils;
import org.opendedup.hashing.VariableSipHashEngine;

public class CheckFileInteg {

	public static void main(String args[]) throws NoSuchAlgorithmException {
		String fn = "z:/" + args[0];
		String nf = "c:/temp/" + args[0];
		BufferedReader reader;
		long pos = 0;
		try {
			reader = new BufferedReader(new FileReader(
					nf));
			String line = reader.readLine();
			RandomAccessFile r = new RandomAccessFile(fn,"r");
			VariableSipHashEngine eng = new VariableSipHashEngine();
			int ln = 0;
			
			while (line != null) {
				String [] st  = line.split(",");
				pos = Long.parseLong(st[0]);
				int cap = Integer.parseInt(st[1]);
				byte [] hash = StringUtils.hexStringToByte(st[2]);
				byte [] b = new byte [cap];
				r.seek(pos);
				r.readFully(b);
				byte [] k = eng.getHash(b);
				if(!Arrays.equals(k, hash)) {
					System.out.println(ln + " mismatch at " +pos + " len " + cap + " expected " +StringUtils.byteToHexString(hash) + " got " + StringUtils.byteToHexString(k));
				}
				ln++;
				line = reader.readLine();
			}
			reader.close();
			r.close();
		} catch (IOException e) {
			System.out.println("unable to read at " + pos);
			e.printStackTrace();
		}
	}
}
