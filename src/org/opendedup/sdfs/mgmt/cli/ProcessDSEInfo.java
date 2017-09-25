package org.opendedup.sdfs.mgmt.cli;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Formatter;
import java.util.Locale;

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
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				Element dse = (Element) root.getElementsByTagName("dse")
						.item(0);
				long entries = -1;
				if(dse.hasAttribute("entries"))
					entries = Long.parseLong(dse.getAttribute("entries"));
				long maxSz = Long.parseLong(dse.getAttribute("max-size"));
				long maxCacheSz = Long.parseLong(dse
						.getAttribute("max-cache-size"));
				long cacheSz = Long.parseLong(dse.getAttribute("cache-size"));
				long rsp = Long.parseLong(dse.getAttribute("read-speed")) * 1024;
				long wsp = Long.parseLong(dse.getAttribute("write-speed")) * 1024;
				long currentSz = Long.parseLong(dse
						.getAttribute("current-size"));
				long compressedSz = Long.parseLong(dse
						.getAttribute("compressed-size"));
				long freeBlocks = Long.parseLong(dse
						.getAttribute("free-blocks"));
				int pageSize = Integer.parseInt(dse.getAttribute("page-size"));
				int port = Integer.parseInt(dse.getAttribute("listen-port"));
				String host = dse.getAttribute("listen-hostname");
				double pFull = 0.00;
				if (currentSz > 0) {
					pFull = (((double) currentSz / (double) maxSz) * 100);
					DecimalFormat twoDForm = (DecimalFormat) NumberFormat
							.getNumberInstance(Locale.US);
					twoDForm.applyPattern("#.##");
					pFull = Double.valueOf(twoDForm.format(pFull));
				}
				System.out.printf("DSE Max Size : %s\n", StorageUnit.of(maxSz)
						.format(maxSz));
				System.out.printf("DSE Current Size : %s\n",
						StorageUnit.of(currentSz).format(currentSz));
				System.out.printf("DSE Compressed Size : %s\n",
						StorageUnit.of(compressedSz).format(compressedSz));
				System.out.printf("DSE Percent Full : %s%%\n", pFull);
				System.out.printf("DSE Page Size : %s\n", pageSize);
				System.out.printf("DSE Blocks Available for Reuse : %s\n",
						freeBlocks);
				if(entries > 0) {
					System.out.printf("Total DSE Blocks : %s\n",
							entries);
					long avgbs = currentSz/entries;
					System.out.printf("Average DSE Block Size : %s\n",
							avgbs);
				}
				System.out.printf("DSE Current Cache Size : %s\n", StorageUnit
						.of(cacheSz).format(cacheSz));
				System.out.printf("DSE Max Cache Size : %s\n",
						StorageUnit.of(maxCacheSz).format(maxCacheSz));
				System.out.printf("Trottled Read Speed : %s/s\n", StorageUnit
						.of(rsp).format(rsp));
				System.out.printf("Trottled Write Speed : %s/s\n", StorageUnit
						.of(wsp).format(wsp));
				if(dse.hasAttribute("ecryption-key"))
					System.out.printf("Encryption Key : %s\n",dse.getAttribute("ecryption-key"));
				if(dse.hasAttribute("ecryption-iv"))
					System.out.printf("Encryption IV : %s\n",dse.getAttribute("ecryption-iv"));
				if(dse.hasAttribute("cloud-access-key"))
					System.out.printf("Cloud Access Key : %s\n",dse.getAttribute("cloud-access-key"));
				if(dse.hasAttribute("cloud-secret-key"))
					System.out.printf("Cloud Secret Key : %s\n",dse.getAttribute("cloud-secret-key"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		runCmd();
	}

}
