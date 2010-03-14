package org.opendedup.util;


import java.io.File;
import java.io.IOException;
public class TestFile {
	public static void main(String [] args) throws IOException {
		String os_arch = System.getProperty("os.arch");
		System.out.println(os_arch);
	}

}
