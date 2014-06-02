package org.opendedup.sdfs.mgmt.cli;

import java.util.Date;
import java.util.Formatter;

import org.quartz.CronExpression;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.bethecoder.ascii_table.ASCIITable;
import com.bethecoder.ascii_table.ASCIITableHeader;

public class ProcessGetGCSchedule {
	public static void runCmd() {
		try {

			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=get-gc-schedule", "null");
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			formatter.close();
			Element root = doc.getDocumentElement();
			if (root.getAttribute("status").equals("failed"))
				System.out.println(root.getAttribute("msg"));
			else {
				Element se = (Element) root.getElementsByTagName("schedule")
						.item(0);
				String schedule = se.getAttribute("schedule");
				System.out.printf("Cron Schedule is : %s\n", schedule);
				CronExpression cex = new CronExpression(schedule);
				ASCIITableHeader[] headerObjs = { new ASCIITableHeader(
						"Next 5 Runs", ASCIITable.ALIGN_LEFT) };
				String[][] data = new String[5][1];
				Date d = cex.getNextValidTimeAfter(new Date());
				for (int i = 0; i < 5; i++) {
					data[i][0] = d.toString();
					d = cex.getNextValidTimeAfter(d);

				}
				ASCIITable.getInstance().printTable(headerObjs, data);
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
