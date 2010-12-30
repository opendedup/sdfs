package org.opendedup.sdfs.mgmt.cli;


import java.util.Formatter;

import org.opendedup.util.StorageUnit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessDSEInfo {
	public static void runCmd() {
		try {
			
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=dse-info", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			Element dse = (Element) root.getElementsByTagName("dse")
					.item(0);
			long maxSz = Long.parseLong(dse.getAttribute("max-size"));
			long currentSz = Long.parseLong(dse.getAttribute("current-size"));
			int pageSize = Integer.parseInt(dse.getAttribute("page-size"));
			System.out.printf("DSE Max Size : %s\n",
					StorageUnit.of(maxSz).format(maxSz));
			System.out.printf("DSE Current Size : %s\n",
					StorageUnit.of(currentSz).format(currentSz));
			System.out.printf("DSE Page Size : %s\n",
					StorageUnit.of(pageSize).format(pageSize));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void main(String []args) {
		runCmd();
	}

}
