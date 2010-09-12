package org.opendedup.sdfs.mgmt.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
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
			if(cmd.hasOption("file-path")) {
				ProcessFileInfo.runCmd(cmd.getOptionValue("file-path"));
				
			}else{
				System.out.println("file info request failed. --file-path option is required");
			}
			System.exit(0);
		}
		if(cmd.hasOption("snapshot")) {
			if(cmd.hasOption("file-path") && cmd.hasOption("snapshot-path")) {
				ProcessSnapshotCmd.runCmd(cmd.getOptionValue("file-path"),cmd.getOptionValue("snapshot-path"));
			}else{
				System.out.println("snapshot request failed. --file-path and --snapshot-path options are required");
			}
			System.exit(0);
		}
		if(cmd.hasOption("flush-file-buffers")) {
			if(cmd.hasOption("file-path")) {
				ProcessFlushBuffersCmd.runCmd("file",cmd.getOptionValue("file-path"));
			}else{
				System.out.println("flush file request failed. --file-path");
			}
			System.exit(0);
		}
		if(cmd.hasOption("flush-all-buffers")) {
				ProcessFlushBuffersCmd.runCmd("all","/");
		}
		if(cmd.hasOption("dedup-file")) {
			if(cmd.hasOption("file-path")) {
				ProcessFlushBuffersCmd.runCmd(cmd.getOptionValue("file-path"),cmd.getOptionValue("dedup-file"));
			}else{
				System.out.println("dedup file request failed. --file-path");
			}
			System.exit(0);
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
						"Deduplicates all file blocks if set to true, otherwise it will only dedup blocks that are" +
						" already stored in the DSE.\n e.g. --dedup-file=true " +
						"--file-path=<file to flush>")
				.withArgName("true|false").create());
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
		Logger.getRootLogger().setLevel(Level.ERROR);
		try {
		parseCmdLine(args);
		}catch(Exception e) {
			System.out.println("Error : It does not appear the SDFS volume is mounted or listening on tcp port 6642");
		}
	    
	}
}
