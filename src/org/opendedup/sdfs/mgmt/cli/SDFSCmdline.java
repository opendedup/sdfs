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
		if(cmd.hasOption("username"))
			MgmtServerConnection.userName = cmd.getOptionValue("username");
		if(cmd.hasOption("password"))
			MgmtServerConnection.password = cmd.getOptionValue("password");
		if(cmd.hasOption("port"))
			MgmtServerConnection.port = Integer.parseInt(cmd.getOptionValue("port"));
		
		if (cmd.hasOption("file-info")) {
			if (cmd.hasOption("file-path")) {
				ProcessFileInfo.runCmd(cmd.getOptionValue("file-path"));

			} else {
				System.out
						.println("file info request failed. --file-path option is required");
			}
			System.exit(0);
		}
		if (cmd.hasOption("dse-info")) {
			ProcessDSEInfo.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("volume-info")) {
			ProcessVolumeInfo.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("debug-info")) {
			ProcessDebugInfo.runCmd();
			System.exit(0);
		}

		if (cmd.hasOption("snapshot")) {
			if (cmd.hasOption("file-path") && cmd.hasOption("snapshot-path")) {
				ProcessSnapshotCmd.runCmd(cmd.getOptionValue("file-path"),
						cmd.getOptionValue("snapshot-path"));
			} else {
				System.out
						.println("snapshot request failed. --file-path and --snapshot-path options are required");
			}
			System.exit(0);
		}
		if (cmd.hasOption("flush-file-buffers")) {
			if (cmd.hasOption("file-path")) {
				ProcessFlushBuffersCmd.runCmd("file",
						cmd.getOptionValue("file-path"));
			} else {
				System.out.println("flush file request failed. --file-path");
			}
			System.exit(0);
		}
		if (cmd.hasOption("flush-all-buffers")) {
			ProcessFlushBuffersCmd.runCmd("all", "/");
		}
		if (cmd.hasOption("dedup-file")) {
			if (cmd.hasOption("file-path")
					&& cmd.getOptionValue("dedup-file") != null) {
				ProcessDedupAllCmd.runCmd(cmd.getOptionValue("file-path"),
						cmd.getOptionValue("dedup-file"));
			} else {
				System.out
						.println("dedup file request failed. --dedup-all=(true|false) --file-path=(path to file)");
			}
			System.exit(0);
		}
		if (cmd.hasOption("cleanstore")) {
			ProcessCleanStore.runCmd(Integer.parseInt(cmd
					.getOptionValue("cleanstore")));
			System.exit(0);
		}
		if (cmd.hasOption("change-password")) {
			ProcessSetPasswordCmd.runCmd(cmd
					.getOptionValue("change-password"));
			System.exit(0);
		}
		if (cmd.hasOption("expandvolume")) {
			ProcessXpandVolumeCmd.runCmd(cmd.getOptionValue("expandvolume"));
			System.exit(0);
		}
		
			
		

	}

	@SuppressWarnings("static-access")
	public static Options buildOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("help")
				.withDescription("Display these options.").hasArg(false)
				.create());
		options.addOption(OptionBuilder.withLongOpt("username")
				.withDescription("User name to authenticate to SDFS CLI Interface for volume.").hasArg(true)
				.create());
		options.addOption(OptionBuilder.withLongOpt("expandvolume")
				.withDescription("Expand the volume, online, to a size in MB,GB, or TB \n e.g expandvolume=100GB. \nValues can be in MB,GB,TB.").hasArg(true)
				.create());
		options.addOption(OptionBuilder.withLongOpt("change-password")
				.withDescription("Change the administrative password.").hasArg(true)
				.create());
		options.addOption(OptionBuilder.withLongOpt("password")
				.withDescription("password to authenticate to SDFS CLI Interface for volume.").hasArg(true)
				.create());
		options.addOption(OptionBuilder.withLongOpt("port")
				.withDescription("SDFS CLI Interface tcp listening port for volume.").hasArg(true)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("file-info")
				.withDescription(
						"Returns io file attributes such as dedup rate and file io statistics. "
								+ "\n e.g. --file-info --file-path=<path to file or folder>")
				.hasArg(false).create());
		options.addOption(OptionBuilder
				.withLongOpt("dse-info")
				.withDescription(
						"Returns Dedup Storage Engine Statitics. "
								+ "\n e.g. --dse-info").hasArg(false).create());
		options.addOption(OptionBuilder
				.withLongOpt("debug-info")
				.withDescription(
						"Returns Debug Information. " + "\n e.g. --debug-info")
				.hasArg(false).create());
		options.addOption(OptionBuilder
				.withLongOpt("volume-info")
				.withDescription(
						"Returns SDFS Volume Statitics. "
								+ "\n e.g. --volume-info").hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("snapshot")
				.withDescription(
						"Creates a snapshot for a particular file or folder.\n e.g. --snapshot "
								+ "--file-path=<source-file> --snapshot-path=<snapshot-destination> ")
				.hasArg(false).create());
		options.addOption(OptionBuilder
				.withLongOpt("flush-file-buffers")
				.withDescription(
						"Flushes to buffer of a praticular file.\n e.g. --flush-file-buffers "
								+ "--file-path=<file to flush>").hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("flush-all-buffers")
				.withDescription(
						"Flushes all buffers within an SDFS file system.\n e.g. --flush-file-buffers "
								+ "--file-path=<file to flush>").hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("dedup-file")
				.withDescription(
						"Deduplicates all file blocks if set to true, otherwise it will only dedup blocks that are"
								+ " already stored in the DSE.\n e.g. --dedup-file=true "
								+ "--file-path=<file to flush>").hasArg()
				.withArgName("true|false").create());
		options.addOption(OptionBuilder
				.withLongOpt("file-path")
				.withDescription(
						"The relative path to the file or folder to take action on.\n e.g. --file-path=readme.txt or --file-path=file\\file.txt")
				.hasArg().withArgName("RELATIVE PATH").create());
		options.addOption(OptionBuilder
				.withLongOpt("snapshot-path")
				.withDescription(
						"The relative path to the destination of the snapshot.\n e.g. --snapshot-path=snap-readme.txt or --snapshot-path=file\\snap-file.txt")
				.hasArg().withArgName("RELATIVE PATH").create());
		options.addOption(OptionBuilder
				.withLongOpt("cleanstore")
				.withDescription(
						"Clean the dedup storage engine of data that is older than defined minutes and is unclaimed by current files. This command only works"
								+ "if the dedup storage engine is local and not in network mode")
				.hasArg().withArgName("minutes").create());
		return options;
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(175);
		formatter.printHelp("sdfs.cmd <options>", options);
	}

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.ERROR);
		try {
			parseCmdLine(args);
		} catch (org.apache.commons.cli.UnrecognizedOptionException e) {
			System.out.println(e.getMessage());
			printHelp(buildOptions());
		} catch (Exception e) {
			System.out
					.println("Error : It does not appear the SDFS volume is mounted or listening on tcp port 6642");
		}

	}
}
