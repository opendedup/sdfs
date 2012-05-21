package org.opendedup.util;

import java.io.IOException;

public class TestFile {
	public static void main(String[] args) throws IOException {
		String tst = "/sys/kernel/config/target/iscsi/iqn.2008-06.org.opendedup.iscsi:n7YOQDe9TQKD/tpgt_1/attrib/demo_mode_write_protect";
		tst = tst.replaceAll("\\:", "\\\\:");
		System.out.println(tst);
	}

}
