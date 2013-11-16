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
				long aWriteBites = Long.parseLong(dse
						.getAttribute("write-bytes"));
				double readBytes = Double.parseDouble(dse
						.getAttribute("read-bytes"));
				long dseSz = Long.parseLong(dse.getAttribute("dse-size"));
				long totalBytes = dedupSz + aWriteBites;
				System.out.printf("Volume Capacity : %s\n",
						StorageUnit.of(capacitySz).format(capacitySz));
				System.out.printf("Volume Current Size : %s\n",
						StorageUnit.of(currentSz).format(currentSz));
				if (maxPFull < 0)
					System.out.printf("Volume Max Percentage Full : %s\n",
							"Unlimited");
				else
					System.out.printf("Volume Max Percentage Full : %s%%\n",
							maxPFull * 100);
				System.out.printf("Volume Duplicate Data Written : %s\n",
						StorageUnit.of(dedupSz).format(dedupSz));
				System.out.printf("Volume Unique Data Written: %s\n",
						StorageUnit.of(aWriteBites).format(aWriteBites));
				System.out.printf("Volume Data Read : %s\n",
						StorageUnit.of(readBytes).format(readBytes));
				System.out.printf("Cluster Block Copies : %s\n",
						dse.getAttribute("cluster-block-copies"));
				if (dedupSz == 0 || aWriteBites == 0) {
					System.out
							.printf("Volume Virtual Dedup Rate (Dup/Total Bytes Written) : %d%%\n",
									0);
				} else {
					double dedupRate = (((double) dedupSz / (double) totalBytes) * 100);
					DecimalFormat twoDForm = new DecimalFormat("#.##");
					dedupRate = Double.valueOf(twoDForm.format(dedupRate));
					System.out
							.printf("Volume Virtual Dedup Rate (Dup/Total Bytes Written) : %s%%\n",
									Double.toString(dedupRate));
				}
				if (totalBytes == 0 || dseSz == 0) {
					System.out
							.printf("Volume Real Dedup Rate (DSE Size/Total Bytes Written) : %d%%\n",
									0);
				} else {
					double dedupRate = (1 - ((double) dseSz / (double) totalBytes)) * 100;
					DecimalFormat twoDForm = new DecimalFormat("#.##");
					dedupRate = Double.valueOf(twoDForm.format(dedupRate));
					System.out
							.printf("Volume Real Dedup Rate (DSE Size/Total Bytes Written) : %s%%\n",
									Double.toString(dedupRate));
				}
				if (currentSz == 0 || aWriteBites == 0) {
					System.out
							.printf("Actual Storage Savings (Unique Blocks Stored/Current Size): %d%%\n",
									0);
				} else {
					double dedupRate = (1 - ((double) dseSz / (double) currentSz)) * 100;
					DecimalFormat twoDForm = new DecimalFormat("#.##");
					dedupRate = Double.valueOf(twoDForm.format(dedupRate));
					System.out
							.printf("Volume Actual Storage Savings (Unique Blocks Stored/Current Size) : %s%%\n",
									Double.toString(dedupRate));
				}
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
