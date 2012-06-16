package org.opendedup.util;

import java.io.PrintStream;

public class CommandLineProgressBar {
	private final String info;
	private final PrintStream out;
	private final float onePercent;
	private long currentPercent = -1;

	public CommandLineProgressBar(String info, long totalCount, PrintStream out) {
		this.info = info;
		this.out = out;
		this.onePercent = Float.valueOf(100F)
				/ Float.valueOf(String.valueOf(totalCount));
		this.update(0);
	}

	public void update(long currentCount) {
		int percent = (int) (onePercent * currentCount);
		if (percent != currentPercent) {
			this.currentPercent = percent;
			printProgBar(this.currentPercent);
		}
	}

	public void finish() {
		printProgBar(100);
	}

	private void printProgBar(long percent) {
		StringBuilder bar = new StringBuilder(info + " |");

		for (int i = 0; i < 50; i++) {
			if (i < (percent / 2)) {
				bar.append(")");
			} else if (i == (percent / 2)) {
				bar.append("]");
			} else {
				bar.append(" ");
			}
		}
		bar.append("| " + percent + "% ");
		out.print("\r" + bar.toString());
		if (percent == 100) {
			out.println("\r\n");
			out.flush();
		}
	}
}