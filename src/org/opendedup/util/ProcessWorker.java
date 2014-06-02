package org.opendedup.util;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.opendedup.logging.SDFSLogger;

public class ProcessWorker extends Thread {

	public static int runProcess(String[] pstr, int timeout)
			throws TimeoutException, InterruptedException, IOException {
		String cmdStr = "";
		for (String st : pstr) {
			cmdStr = cmdStr + " " + st;
		}
		SDFSLogger.getLog().info("Executing [" + cmdStr + "]");
		Process p = Runtime.getRuntime().exec(pstr);
		ProcessWorker worker = new ProcessWorker(p);
		worker.start();
		try {
			worker.join(timeout);
			if (worker.exit != null) {
				SDFSLogger.getLog().info(
						"[" + cmdStr + "] returned " + worker.exit);
				return worker.exit;
			} else
				throw new TimeoutException();
		} catch (InterruptedException ex) {
			worker.interrupt();
			Thread.currentThread().interrupt();
			throw ex;
		} finally {
			p.destroy();
		}
	}

	private final Process process;
	private Integer exit;

	private ProcessWorker(Process process) {
		this.process = process;
	}

	public void run() {
		try {
			exit = process.waitFor();
		} catch (InterruptedException ignore) {
			return;
		}
	}

}
