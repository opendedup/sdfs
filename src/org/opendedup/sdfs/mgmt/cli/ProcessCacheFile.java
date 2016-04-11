package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessCacheFile {
	public static void runCmd(String file) {
		try {
			System.out.printf("PreFetching File [%s] \n", file);
			file = URLEncoder.encode(file, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=cachefile", file);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			System.out.printf(
					"Checking out a read only copy of [%s] returned [%s]\n",
					status, msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
