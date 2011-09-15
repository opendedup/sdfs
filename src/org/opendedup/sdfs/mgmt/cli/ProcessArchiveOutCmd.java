package org.opendedup.sdfs.mgmt.cli;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.Formatter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class ProcessArchiveOutCmd {
	public static void runCmd(String file) {
		try {
			System.out.printf("archive a copy of [%s] \n",
					file);
			file = URLEncoder.encode(file, "UTF-8");
			StringBuilder sb = new StringBuilder();
			Formatter formatter = new Formatter(sb);
			formatter.format("file=%s&cmd=archiveout&options=iloveanne", file);
			Document doc = MgmtServerConnection.getResponse(sb.toString());
			Element root = doc.getDocumentElement();
			String status = root.getAttribute("status");
			String msg = root.getAttribute("msg");
			System.out.println("getting " + msg);
			InputStream in = MgmtServerConnection.connectAndGet("", msg);
			FileOutputStream out = new FileOutputStream(msg);
			byte[] buf = new byte[32768];
			int len;
			while ((len = in.read(buf)) > 0) {
				System.out.print("#");
				out.write(buf, 0, len);
			}
			in.close();
			out.close();
			System.out.println();
			System.out.printf("Copy Out [%s] returned [%s]\n", status, msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
