package org.opendedup.sdfs.mgmt.cli;

import java.net.URLEncoder;
import java.text.DecimalFormat;
import java.util.Formatter;

import org.opendedup.util.StorageUnit;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessFileInfo {
	public static void runCmd(String file) {
		try {
			file = URLEncoder.encode(file, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=info", file);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				Element files = (Element) root.getElementsByTagName("files")
						.item(0);
				for (int i = 0; i < files.getElementsByTagName("file-info")
						.getLength(); i++) {
					Element fileEl = (Element) files.getElementsByTagName(
							"file-info").item(i);
					System.out.println("#");
					System.out.printf("file name : %s\n",
							fileEl.getAttribute("file-name"));
					System.out.printf("sdfs path : %s\n",
							fileEl.getAttribute("sdfs-path"));
					if (fileEl.hasAttribute("symlink")) {
						System.out.printf("symlink : %s\n",
								fileEl.getAttribute("symlink"));
						System.out.printf("symlink path: %s\n",
								fileEl.getAttribute("symlink-path"));
					}
					System.out.printf("file type : %s\n",
							fileEl.getAttribute("type"));
					if (fileEl.getAttribute("type").equalsIgnoreCase("file")) {
						Element ioEl = (Element) fileEl.getElementsByTagName(
								"io-info").item(0);
						System.out.printf("dedup file : %s\n",
								fileEl.getAttribute("dedup"));
						System.out.printf("map file guid : %s\n",
								fileEl.getAttribute("dedup-map-guid"));
						System.out.printf("file open : %s\n",
								fileEl.getAttribute("open"));
						System.out.printf("real bytes written : %s\n",
								ioEl.getAttribute("actual-bytes-written"));
						System.out
								.printf("format real data written : %s\n",
										StorageUnit
												.of(Long.parseLong(ioEl
														.getAttribute("actual-bytes-written")))
												.format(Long.parseLong(ioEl
														.getAttribute("actual-bytes-written"))));
						System.out.printf("virtual bytes written : %s\n",
								ioEl.getAttribute("virtual-bytes-written"));
						System.out
								.printf("format virtual data written : %s\n",
										StorageUnit
												.of(Long.parseLong(ioEl
														.getAttribute("virtual-bytes-written")))
												.format(Long.parseLong(ioEl
														.getAttribute("virtual-bytes-written"))));
						System.out.printf("duplicate data bytes: %s\n",
								ioEl.getAttribute("duplicate-blocks"));
						System.out
								.printf("format duplicate data : %s\n",
										StorageUnit
												.of(Long.parseLong(ioEl
														.getAttribute("duplicate-blocks")))
												.format(Long.parseLong(ioEl
														.getAttribute("duplicate-blocks"))));
						System.out.printf("bytes read : %s\n",
								ioEl.getAttribute("bytes-read"));
						System.out.printf(
								"format data read: %s\n",
								StorageUnit.of(
										Long.parseLong(ioEl
												.getAttribute("bytes-read")))
										.format(Long.parseLong(ioEl
												.getAttribute("bytes-read"))));

						long realBytes = Long.parseLong(ioEl
								.getAttribute("actual-bytes-written"));
						long dedupBytes = Long.parseLong(ioEl
								.getAttribute("duplicate-blocks"));
						if (dedupBytes == 0 || realBytes == 0) {

							System.out.printf("dedup rate : %d%%\n", 0);
						} else {
							double dedupRate = (((double) dedupBytes / (double) (dedupBytes + realBytes)) * 100);
							DecimalFormat twoDForm = new DecimalFormat("#.##");
							dedupRate = Double.valueOf(twoDForm
									.format(dedupRate));

							System.out.printf("dedup rate : %s%%\n",
									Double.toString(dedupRate));

						}
					}
				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		runCmd("/");
	}

}
