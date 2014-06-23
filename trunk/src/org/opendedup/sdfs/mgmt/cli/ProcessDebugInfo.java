package org.opendedup.sdfs.mgmt.cli;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Formatter;
import java.util.Locale;

import org.opendedup.util.OSValidator;
import org.opendedup.util.StorageUnit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessDebugInfo {
	public static void runCmd() {
		try {

			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=debug-info", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				Element debug = (Element) root.getElementsByTagName("debug")
						.item(0);
				System.out.printf("Active SDFS Threads : %s\n",
						debug.getAttribute("active-threads"));
				if (OSValidator.isUnix()) {
					DecimalFormat zDForm = (DecimalFormat)NumberFormat.getNumberInstance(Locale.US);
					zDForm.applyPattern("#");
					double tcp = Double.parseDouble(debug
							.getAttribute("total-cpu-load")) * 100;
					double pcp = Double.parseDouble(debug
							.getAttribute("sdfs-cpu-load")) * 100;
					System.out.printf("CPU Load : %s\n", zDForm.format(tcp)
							+ "%");
					System.out.printf("SDFS CPU Load : %s\n",
							zDForm.format(pcp) + "%");

					System.out.printf("Total Memory : %s\n",
							formatData(debug.getAttribute("total-memory")));
					System.out.printf("Free Memory : %s\n",
							formatData(debug.getAttribute("free-memory")));
				}
				System.out.printf("Free Disk Space for Chunk Store: %s\n",
						formatData(debug.getAttribute("free-space")));
				System.out.printf("Total Space for Chunk Store: %s\n",
						formatData(debug.getAttribute("total-space")));
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static String formatData(String data) {
		return StorageUnit.of(Long.parseLong(data))
				.format(Long.parseLong(data));
	}

	public static void main(String[] args) {
		runCmd();
	}

}
