package org.opendedup.sdfs.mgmt.cli;

import java.text.DecimalFormat;
import java.util.Formatter;

import org.opendedup.util.StorageUnit;
import org.opendedup.util.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.bethecoder.ascii_table.ASCIITable;
import com.bethecoder.ascii_table.ASCIITableHeader;

public class ProcessClusterVolumesList {
	public static void runCmd() {
		try {
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=cluster-volumes", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			formatter.close();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				ASCIITableHeader[] headerObjs = {
						new ASCIITableHeader("Volume Name",
								ASCIITable.ALIGN_LEFT),
						new ASCIITableHeader("Mounted", ASCIITable.ALIGN_LEFT),
						new ASCIITableHeader("Host", ASCIITable.ALIGN_LEFT),
						new ASCIITableHeader("Current Size (GB)",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Max Size (GB)",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Percent Full",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Unique Data (GB)",
								ASCIITable.ALIGN_RIGHT),

						new ASCIITableHeader("Duplicate (GB)",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Read (GB)",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Redundancy",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("Rack Aware",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("DSE as Cap",
								ASCIITable.ALIGN_RIGHT),
						new ASCIITableHeader("DSE as Size",
								ASCIITable.ALIGN_RIGHT)

				};
				Element dses = (Element) root.getElementsByTagName(
						"remote-volumes").item(0);
				NodeList nl = dses.getElementsByTagName("volume");
				String[][] data = new String[nl.getLength()][headerObjs.length];
				for (int i = 0; i < nl.getLength(); i++) {

					Element dse = (Element) nl.item(i);
					if (dse.getAttribute("down").equalsIgnoreCase("false")) {
						String csz = StorageUnit.GIGABYTE.format(Long
								.parseLong(dse.getAttribute("current-size")));
						String msz = StorageUnit.GIGABYTE.format(StringUtils
								.parseSize(dse.getAttribute("capacity")));
						double pf = ((double) Long.parseLong(dse
								.getAttribute("current-size")) / (double) StringUtils
								.parseSize(dse.getAttribute("capacity"))) * 100;
						DecimalFormat twoDForm = new DecimalFormat("#.##");
						String cpf = Double.toString(Double.valueOf(twoDForm
								.format(pf)));
						String wb = StorageUnit.GIGABYTE.format(Long
								.parseLong(dse.getAttribute("write-bytes")));
						String db = StorageUnit.GIGABYTE
								.format(Long.parseLong(dse
										.getAttribute("duplicate-bytes")));
						String rb = StorageUnit.GIGABYTE.format(Double
								.parseDouble(dse.getAttribute("read-bytes")));
						String cc = Integer.toString((Integer.parseInt(dse
								.getAttribute("cluster-block-copies")) - 1));
						String[] row = { dse.getAttribute("name"), "true",
								dse.getAttribute("host"), csz, msz, cpf + " %",
								wb, db, rb, cc,
								dse.getAttribute("cluster-rack-aware"),
								dse.getAttribute("use-dse-capacity"),
								dse.getAttribute("use-dse-size") };
						data[i] = row;
					} else {
						String[] row = { dse.getAttribute("name"), "false", "",
								"", "", "", "", "", "", "", "", "", "" };
						data[i] = row;
					}

				}
				ASCIITable.getInstance().printTable(headerObjs, data);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		runCmd();
	}

}
