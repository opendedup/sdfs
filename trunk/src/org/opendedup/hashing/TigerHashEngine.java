package org.opendedup.hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.opendedup.sdfs.Main;

public class TigerHashEngine implements AbstractHashEngine {
	static {
		Security.addProvider(new BouncyCastleProvider());
	}

	MessageDigest hc = null;

	public TigerHashEngine() throws NoSuchAlgorithmException,
			NoSuchProviderException {
		hc = MessageDigest.getInstance("Tiger", "BC");
	}

	@Override
	public byte[] getHash(byte[] data) {
		byte[] hash = hc.digest(data);
		hc.reset();
		return hash;
	}

	public static int getHashLenth() {
		// TODO Auto-generated method stub
		return 24;
	}

	@Override
	public void destroy() {
		hc.reset();
		hc = null;
	}

	@Override
	public boolean isVariableLength() {
		// TODO Auto-generated method stub
		return false;
	}
	
	@Override
	public int getMaxLen() {
		// TODO Auto-generated method stub
		return Main.CHUNK_LENGTH;
	}
}
