package org.opendedup.sdfs.mgmt.cli;

import java.text.DecimalFormat;
import java.util.Formatter;

import org.opendedup.util.StorageUnit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessVolumeInfo {
	public static void runCmd() {
		try {

			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=volume-info", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				Element dse = (Element) root.getElementsByTagName("volume")
						.item(0);
				long capacitySz = Long.parseLong(dse.getAttribute("capacity"));
				double maxPFull = Double.parseDouble(dse
						.getAttribute("maximum-percentage-full"));
				long currentSz = Long.parseLong(dse
						.getAttribute("current-size"));
				long dedupSz = Long.parseLong(dse
						.getAttribute("duplicate-bytes"));
				long compSz = Long.parseLong(dse
						.getAttribute("dse-comp-size"));
				long dseSz = Long.parseLong(dse.getAttribute("dse-size"));
				System.out.printf("Volume Capacity : %s\n",
						StorageUnit.of(capacitySz).format(capacitySz));
				System.out.printf("Volume Current Logical Size : %s\n",
						StorageUnit.of(currentSz).format(currentSz));
				if (maxPFull < 0)
					System.out.printf("Volume Max Percentage Full : %s\n",
							"Unlimited");
				else
					System.out.printf("Volume Max Percentage Full : %s%%\n",
							maxPFull * 100);
				System.out.printf("Volume Duplicate Data Written : %s\n",
						StorageUnit.of(dedupSz).format(dedupSz));
				System.out.printf("Unique Blocks Stored: %s\n",
						StorageUnit.of(dseSz).format(dseSz));
				System.out.printf("Unique Blocks Stored after Compression : %s\n",
						StorageUnit.of(compSz).format(compSz));
				System.out.printf("Cluster Block Copies : %s\n",
						dse.getAttribute("cluster-block-copies"));
				if (dseSz == 0 || capacitySz == 0) {
					System.out
							.printf("Volume Virtual Dedup Rate (Unique Blocks Stored/Current Size) : %d%%\n",
									0);
				} else {
					double dedupRate = (((double) dseSz / (double) capacitySz) * 100);
					DecimalFormat twoDForm = new DecimalFormat("#.##");
					dedupRate = Double.valueOf(twoDForm.format(dedupRate));
					System.out
							.printf("Volume Virtual Dedup Rate (Unique Blocks Stored/Current Size) : %s%%\n",
									Double.toString(dedupRate));
				}
				if (compSz == 0 || currentSz == 0) {
					System.out
							.printf("Actual Storage Savings (Compressed Unique Blocks Stored/Current Size): %d%%\n",
									0);
				} else {
					double dedupRate = (1 - ((double) compSz / (double) currentSz)) * 100;
					DecimalFormat twoDForm = new DecimalFormat("#.##");
					dedupRate = Double.valueOf(twoDForm.format(dedupRate));
					System.out
							.printf("Volume Actual Storage Savings (Compressed Unique Blocks Stored/Current Size) : %s%%\n",
									Double.toString(dedupRate));
				}
				if (compSz == 0 || dseSz == 0) {
					System.out
							.printf("Compression Rate: %d%%\n",
									0);
				} else {
					double compRate = (1 - ((double) compSz / (double) dseSz)) * 100;
					DecimalFormat twoDForm = new DecimalFormat("#.##");
					compRate = Double.valueOf(twoDForm.format(compRate));
					System.out
							.printf("Compression Rate: %s%%\n",
									Double.toString(compRate));
				}
				formatter.close();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		runCmd();
	}

}
