package org.opendedup.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class DeleteDir {

	public static boolean deleteDirectory(File path) throws IOException {
		if (path.exists()) {
			File[] files = path.listFiles();
			for (int i = 0; i < files.length; i++) {
				if(Files.isSymbolicLink(files[i].toPath())) {
					Files.deleteIfExists(files[i].toPath());
				}
				else if (files[i].isDirectory()) {
					deleteDirectory(files[i]);
				} else {
					Files.deleteIfExists(files[i].toPath());
				}
			}
			return (path.delete());
		} else {
			return true;
		}

	}
}
