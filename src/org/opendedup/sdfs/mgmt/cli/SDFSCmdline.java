package org.opendedup.sdfs.mgmt.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.opendedup.logging.SDFSLogger;

public class SDFSCmdline {
	public static void parseCmdLine(String[] args) throws Exception {
		PosixParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		if (cmd.hasOption("help") || args.length == 0) {
			printHelp(options);
			System.exit(1);
		}
		boolean quiet = false;
		if (cmd.hasOption("debug"))
			SDFSLogger.setLevel(0);
		if (cmd.hasOption("nossl"))
			MgmtServerConnection.useSSL = false;
		if (cmd.hasOption("server"))
			MgmtServerConnection.server = cmd.getOptionValue("server");
		if (cmd.hasOption("password"))
			MgmtServerConnection.password = cmd.getOptionValue("password");
		if (cmd.hasOption("port"))
			MgmtServerConnection.port = Integer.parseInt(cmd
					.getOptionValue("port"));

		if (cmd.hasOption("file-info")) {
			if (cmd.hasOption("file-path")) {
				ProcessFileInfo.runCmd(cmd.getOptionValue("file-path"));

			} else {
				SDFSLogger
						.getBasicLog()
						.warn("file info request failed. --file-path option is required");
			}
			System.exit(0);
		}
		if (cmd.hasOption("dse-info")) {
			ProcessDSEInfo.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("cluster-dse-info")) {
			ProcessClusterDSEInfo.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("cluster-volumes")) {
			ProcessClusterVolumesList.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("cluster-volume-remove")) {
			ProcessClusterVolumeRemove.runCmd(cmd
					.getOptionValue("cluster-volume-remove"));
			System.exit(0);
		}
		if (cmd.hasOption("cluster-volume-add")) {
			ProcessClusterVolumeAdd.runCmd(cmd
					.getOptionValue("cluster-volume-add"));
			System.exit(0);
		}
		if (cmd.hasOption("cluster-make-gc-master")) {
			ProcessClusterPromoteToGC.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("cluster-get-gc-master")) {
			ProcessGetGCMaster.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("set-gc-schedule")) {
			ProcessSetGCSchedule.runCmd(cmd.getOptionValue("set-gc-schedule"));
			System.exit(0);
		}
		if (cmd.hasOption("get-gc-schedule")) {
			ProcessGetGCSchedule.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("volume-info")) {
			ProcessVolumeInfo.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("cluster-redundancy-check")) {
			ProcessClusterRedundancyCheck.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("debug-info")) {
			ProcessDebugInfo.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("perfmon-on")) {
			ProcessSetPerfmonCmd.runCmd(cmd.getOptionValue("perfmon-on"));
		}
		if (cmd.hasOption("import-archive")) {
			String server = cmd.getOptionValue("replication-master");
			String password = cmd.getOptionValue("replication-master-password");
			int port = 6442;
			int maxSz = -1;
			if (cmd.hasOption("replication-master-port"))
				port = Integer.parseInt(cmd
						.getOptionValue("replication-master-port"));
			if (cmd.hasOption("replication-batch-size"))
				maxSz = Integer.parseInt(cmd
						.getOptionValue("replication-batch-size"));
			ProcessImportArchiveCmd.runCmd(
					cmd.getOptionValue("import-archive"),
					cmd.getOptionValue("file-path"), server, password, port,
					quiet, maxSz);
		}
		if (cmd.hasOption("archive-out")) {
			ProcessArchiveOutCmd.runCmd(cmd.getOptionValue("archive-out"), ".");
		}

		if (cmd.hasOption("snapshot")) {
			if (cmd.hasOption("file-path") && cmd.hasOption("snapshot-path")) {
				ProcessSnapshotCmd.runCmd(cmd.getOptionValue("file-path"),
						cmd.getOptionValue("snapshot-path"));
			} else {
				SDFSLogger
						.getBasicLog()
						.warn("snapshot request failed. --file-path and --snapshot-path options are required");
			}
			System.exit(0);
		}
		if (cmd.hasOption("flush-file-buffers")) {
			if (cmd.hasOption("file-path")) {
				ProcessFlushBuffersCmd.runCmd("file",
						cmd.getOptionValue("file-path"));
			} else {
				SDFSLogger.getBasicLog().warn(
						"flush file request failed. --file-path");
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
				SDFSLogger
						.getBasicLog()
						.warn("dedup file request failed. --dedup-all=(true|false) --file-path=(path to file)");
			}
			System.exit(0);
		}
		if (cmd.hasOption("cleanstore")) {
			ProcessCleanStore.runCmd(Integer.parseInt(cmd
					.getOptionValue("cleanstore")));
			System.exit(0);
		}
		if (cmd.hasOption("fdisk")) {
			ProcessFdisk.runCmd(cmd.getOptionValue("fdisk"));
			System.exit(0);
		}
		if (cmd.hasOption("change-password")) {
			ProcessSetPasswordCmd.runCmd(cmd.getOptionValue("change-password"));
			System.exit(0);
		}
		if (cmd.hasOption("expandvolume")) {
			ProcessXpandVolumeCmd.runCmd(cmd.getOptionValue("expandvolume"));
			System.exit(0);
		}
		if (cmd.hasOption("partition-add")) {
			String[] vals = cmd.getOptionValues("partition-add");
			if (vals.length != 3) {
				System.err
						.println("device-name size start-on-vol-startup are required options");
				System.exit(-1);
			}
			ProcessBlockDeviceAdd.runCmd(vals[0], vals[1],
					Boolean.parseBoolean(vals[2]));
			System.exit(0);
		}

		if (cmd.hasOption("partition-update")) {
			String[] vals = cmd.getOptionValues("partition-update");
			if (vals.length != 3) {
				System.err
						.println("device-name <size|autostart> <value> are required options");
				System.exit(-1);
			}
			ProcessBlockDeviceUpdate.runCmd(vals[0], vals[1], vals[2]);
			System.exit(0);
		}
		
		if (cmd.hasOption("copy-extents")) {
			String[] vals = cmd.getOptionValues("copy-extents");
			if (vals.length != 3) {
				System.err
						.println("copy-extents <source-start> <len> <destination-start>");
				System.exit(-1);
			}
			String sfile = cmd.getOptionValue("file-path");
			String dfile = cmd.getOptionValue("snapshot-path");
			ProcessCopyExtents.runCmd(sfile,dfile,Long.parseLong(vals[0]), Long.parseLong(vals[1]), Long.parseLong(vals[2]));
			System.exit(0);
		}
		

		if (cmd.hasOption("partition-rm")) {
			String val = cmd.getOptionValue("partition-rm");
			ProcessBlockDeviceRm.runCmd(val);
			System.exit(0);
		}
		if (cmd.hasOption("partition-start")) {
			String val = cmd.getOptionValue("partition-start");
			ProcessBlockDeviceStart.runCmd(val);
			System.exit(0);
		}
		if (cmd.hasOption("partition-stop")) {
			String val = cmd.getOptionValue("partition-stop");
			ProcessBlockDeviceStop.runCmd(val);
			System.exit(0);
		}
		if (cmd.hasOption("partition-list")) {
			ProcessBlockDeviceList.runCmd();
			System.exit(0);
		}
		if (cmd.hasOption("shutdown")) {
			ProcessShutdown.runCmd();
			System.exit(0);
		}
	}

	@SuppressWarnings("static-access")
	public static Options buildOptions() {
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt("help")
				.withDescription("Display these options.").hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("server")
				.withDescription(
						"SDFS host location that is the target of this cli command.")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("expandvolume")
				.withDescription(
						"Expand the local volume, online, to a size in MB,GB, or TB \n e.g expandvolume=100GB. \nValues can be in MB,GB,TB.")
				.hasArg(true).create());
		options.addOption(OptionBuilder.withLongOpt("change-password")
				.withDescription("Change the administrative password.")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("password")
				.withDescription(
						"password to authenticate to SDFS CLI Interface for volume.")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("port")
				.withDescription(
						"SDFS CLI Interface tcp listening port for volume.")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("perfmon-on")
				.withDescription(
						"Turn on or off the volume performance monitor.")
				.hasArg(true).withArgName("true|false").create());
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
				.withLongOpt("cluster-dse-info")
				.withDescription(
						"Returns Dedup Storage Engine Statitics for all Storage Nodes in the cluster. "
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
				.withLongOpt("cluster-volumes")
				.withDescription(
						"Returns A List of SDFS Volumes in the cluster. "
								+ "\n e.g. --cluster-volumes").hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("fdisk")
				.withDescription(
						"Runs fdisk on volume.")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-volume-remove")
				.withDescription(
						"Removes an unassociated volume in the cluster. "
								+ "\n e.g. --cluster-volume-remove <vol-name>")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-volume-add")
				.withDescription(
						"Adds an unassociated volume in the cluster. "
								+ "\n e.g. --cluster-volume-add <vol-name>")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-make-gc-master")
				.withDescription(
						"Makes this host the current Garbage Collection Coordinator. "
								+ "\n e.g. --cluster-make-gc-master")
				.hasArg(false).create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-get-gc-master")
				.withDescription(
						"Returns the current Garbage Collection Coordinator. ")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("shutdown")
				.withDescription("Shuts down the volume").hasArg(false)
				.create());
		options.addOption(OptionBuilder.withLongOpt("copy-extents")
				.withDescription("Copies Extent from one file to another").hasArgs(3)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("set-gc-schedule")
				.withDescription(
						"Sets the cron schedule for the GC Master. Schedule must be in quotes \n e.g. --set-gc-schedule=\"0 59 23 * * ?\"")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("get-gc-schedule")
				.withDescription("Returns the cron schedule for the GC Master.")
				.hasArg(false).create());
		options.addOption(OptionBuilder
				.withLongOpt("snapshot")
				.withDescription(
						"Creates a snapshot for a particular file or folder.\n e.g. --snapshot "
								+ "--file-path=<source-file> --snapshot-path=<snapshot-destination> ")
				.hasArg(false).create());
		options.addOption(OptionBuilder.withLongOpt("debug")
				.withDescription("makes output more verbose").hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("cluster-redundancy-check")
				.withDescription(
						"makes sure that the storage cluster maintains the required number of copies for each block of data")
				.hasArg(false).create());
		options.addOption(OptionBuilder
				.withLongOpt("archive-out")
				.withDescription(
						"Creates an archive tar for a particular file or folder and outputs the location.\n e.g. --archive-out "
								+ "<source-file> ").hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("import-archive")
				.withDescription(
						"Imports an archive created using archive out.\n e.g. --import-archive <archive created with archive-out> "
								+ "--file-path=<relative-folder-destination> --replication-master=<server-ip> --replication-master-password=<server-password>")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("replication-master")
				.withDescription(
						"The server associated with the archive imported "
								+ "--replication-master=<server-ip> ")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("replication-batch-size")
				.withDescription(
						"The size,in MB, of the batch that the relication client will request from the replication master. If ignored or set to <1 it will default "
								+ "to the what ever is on the replication client volume as the default. This is currently 30 MB. This will default to \"-1\" "
								+ "--replication-batch-size=<size in MB> ")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("replication-master-port")
				.withDescription(
						"The server port associated with the archive imported. This will default to \"6442\" "
								+ "--replication-master-port=<tcp port> ")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("replication-master-password")
				.withDescription(
						"The server password associated with the archive imported "
								+ "--replication-master-password=<server-password> ")
				.hasArg(true).create());
		options.addOption(OptionBuilder
				.withLongOpt("flush-file-buffers")
				.withDescription(
						"Flushes to buffer of a praticular file.\n e.g. --flush-file-buffers "
								+ "--file-path=<file to flush>").hasArg(false)
				.create());
		options.addOption(OptionBuilder
				.withLongOpt("nossl")
				.withDescription(
						"If set, tries to connect to volume without ssl")
				.hasArg(false).create());
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
		options.addOption(OptionBuilder
				.withLongOpt("partition-add")
				.withDescription(
						"Creates a partition inside this volume. This option has three aguements: device-name size(MB|GB|TB) start-on-volume-startup(true|false) \n e.g. --createdev new-dev 100GB true")
				.hasArgs(3)
				.withArgName("device-name size start-on-vol-startup").create());
		options.addOption(OptionBuilder
				.withLongOpt("partition-rm")
				.withDescription(
						"Removes a partition from the volume.This will delete the block device and de-reference all data in the volume.")
				.hasArg().withArgName("device-name").create());
		options.addOption(OptionBuilder
				.withLongOpt("partition-stop")
				.withDescription("Stops an active partition within the volume.")
				.hasArg().withArgName("device-name").create());
		options.addOption(OptionBuilder
				.withLongOpt("partition-start")
				.withDescription(
						"Starts an inactive partition within the volume.")
				.hasArg().withArgName("device-name").create());
		options.addOption(OptionBuilder.withLongOpt("partition-list")
				.withDescription("Lists all block devices within the volume.")
				.create());
		return options;
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(175);
		formatter.printHelp("sdfs.cmd <options>", options);
	}

	public static void main(String[] args) throws Exception {
		LogManager.getRootLogger().setLevel(Level.INFO);
		BasicConfigurator.configure();
		try {
			parseCmdLine(args);
		} catch (org.apache.commons.cli.UnrecognizedOptionException e) {
			SDFSLogger.getBasicLog().warn(e.toString());
			printHelp(buildOptions());
		} catch (Exception e) {
			SDFSLogger.getBasicLog().error("An error occured", e);
			System.exit(-1);
		}
	}
}
