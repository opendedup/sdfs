package org.opendedup.sdfs.mgmt.cli;

import java.util.Formatter;

import org.opendedup.rabin.utils.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessSetCache {
	public static void runCmd(String ssz) {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			long sz = StringUtils.parseSize(ssz);
			formatter.format("file=%s&cmd=setcachesz&sz=%d", "null",sz);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				if (root.getAttribute("status").equals("failed")) {
					System.out.println(root.getAttribute("msg"));
					System.exit(-1);
				}
				String status = root.getAttribute("status");
				String msg = root.getAttribute("msg");
				System.out.printf("Set Cache [%s] returned [%s]\n", status, msg);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
