package org.opendedup.util;

import java.io.File;

public class DeleteDir {

	public static boolean deleteDirectory(File path) {
		if (path.exists()) {
			File[] files = path.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					deleteDirectory(files[i]);
				} else {
					files[i].delete();
				}
			}
			return (path.delete());
		} else {
			return true;
		}

	}
}
