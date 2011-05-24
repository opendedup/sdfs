package org.opendedup.util;

import java.io.IOException;

import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.GoogleChunkStore;

public class GSTest {
	public static void main(String [] args) throws IOException {
		Main.awsAccessKey = args[0];
		Main.awsSecretKey = args[1];
		Main.awsBucket = args[2];
		Main.chunkStoreEncryptionEnabled = false;
		Main.awsCompress = false;
		GoogleChunkStore store = new GoogleChunkStore(Main.awsBucket);
		store.writeBlankChunk("test.txt", "this is a test".getBytes());
	}

}
