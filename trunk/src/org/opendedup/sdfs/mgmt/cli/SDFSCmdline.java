package org.opendedup.sdfs.mgmt.cli;


import java.util.Formatter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
public class SDFSCmdline {
	public static void parseCmdLine(String[] args) throws Exception {
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption("help") || args.length == 0) {
			printHelp(options);
			System.exit(1);
		}
		if(cmd.hasOption("file-info")) {
			
		}
	}
	@SuppressWarnings("static-access")
	public static Options buildOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("help").withDescription(
				"Display these options.").hasArg(false).create());
		options
				.addOption(OptionBuilder
						.withLongOpt("file-info")
						.withDescription(
								"Returns io file attributes such as dedup rate and file io statistics. " +
								"\n e.g. --file-info --file-path=<path to file or folder>")
						.hasArg(false).create());
		options
		.addOption(OptionBuilder
				.withLongOpt("snapshot")
				.withDescription(
						"Creates a snapshot for a particular file or folder.\n e.g. --snapshot " +
						"--file-path=<source-file> --snapshot-path=<snapshot-destination> ")
				.hasArg(false).create());
		options
		.addOption(OptionBuilder
				.withLongOpt("snapshot")
				.withDescription(
						"Creates a snapshot for a particular file or folder.\n e.g. --snapshot " +
						"--file-path=<source-file> --snapshot-path=<snapshot-destination> ")
				.hasArg(false).create());
		options
		.addOption(OptionBuilder
				.withLongOpt("flush-file-buffers")
				.withDescription(
						"Flushes to buffer of a praticular file.\n e.g. --flush-file-buffers " +
						"--file-path=<file to flush>")
				.hasArg(false).create());
		options
		.addOption(OptionBuilder
				.withLongOpt("flush-all-buffers")
				.withDescription(
						"Flushes all buffers within an SDFS file system.\n e.g. --flush-file-buffers " +
						"--file-path=<file to flush>")
				.hasArg(false).create());
		options
		.addOption(OptionBuilder
				.withLongOpt("dedup-file")
				.withDescription(
						"Flushes to buffer of a praticular file.\n e.g. --flush-file-buffers " +
						"--file-path=<file to flush>")
				.hasArg(false).create());
		options
		.addOption(OptionBuilder
				.withLongOpt("file-path")
				.withDescription(
						"The relative path to the file or folder to take action on.\n e.g. --file-path=readme.txt or --file-path=file\\file.txt")
						.hasArg()
				.withArgName("RELATIVE PATH").create());
		options
		.addOption(OptionBuilder
				.withLongOpt("snapshot-path")
				.withDescription(
						"The relative path to the destination of the snapshot.\n e.g. --snapshot-path=snap-readme.txt or --snapshot-path=file\\snap-file.txt")
						.hasArg()
				.withArgName("RELATIVE PATH").create());
		
		return options;
	}
	
	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(175);
		formatter
				.printHelp(
						"sdfs.cmd <options>",
						options);
	}
	
	public static void main(String [] args) throws Exception {
		BasicConfigurator.configure();
		StringBuilder sb = new StringBuilder();
		Formatter formatter = new Formatter(sb);
	    System.out.printf ("%-10.10s %-10.10s %-10.10s\n", "one", "two", "three");
	    System.out.printf ("%-10.10s %-10.10s %-10.10s\n", "one", "two", "three");
	    System.out.printf ("%-10.10s %-10.10s %-10.10s\n", "one", "two", "three");
	    formatter.format("%-10.10s %-10.10s %-10.10s", "one", "two", "three\n");
	    formatter.format("%-10.10s %-10.10s %-10.10s", "one", "two", "three\n");
	    formatter.format("%-10.10s %-10.10s %-10.10s", "four", "five", "six\n");
	    formatter.format("%-10.10s %-10.10s %-10.10s", "one", "two", "three\n");
	    formatter.format("%-10.10s %-10.10s %-10.10s", "four", "five", "six\n");
	    System.out.println(sb);
		parseCmdLine(args);
		
	    // Send all output to the Appendable object sb
	    
	}
}
