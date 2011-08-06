package org.opendedup.sdfs.mgmt.cli;

import java.text.DecimalFormat;
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
			Element dse = (Element) root.getElementsByTagName("dse").item(0);
			long maxSz = Long.parseLong(dse.getAttribute("max-size"));
			long currentSz = Long.parseLong(dse.getAttribute("current-size"));
			long freeBlocks = Long.parseLong(dse.getAttribute("free-blocks"));
			int pageSize = Integer.parseInt(dse.getAttribute("page-size"));
			double pFull = 0.00;
			if (currentSz > 0) {
				pFull = (((double) currentSz / (double) maxSz) * 100);
				DecimalFormat twoDForm = new DecimalFormat("#.##");
				pFull = Double.valueOf(twoDForm.format(pFull));
			}
			System.out.printf("DSE Max Size : %s\n", StorageUnit.of(maxSz)
					.format(maxSz));
			System.out.printf("DSE Current Size : %s\n",
					StorageUnit.of(currentSz).format(currentSz));
			System.out.printf("DSE Percent Full : %s%%\n", pFull);
			System.out.printf("DSE Page Size : %s\n", pageSize);
			System.out.printf("DSE Blocks Available for Reuse : %s\n",
					freeBlocks);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		runCmd();
	}

}
