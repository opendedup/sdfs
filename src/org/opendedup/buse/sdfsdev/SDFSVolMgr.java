package org.opendedup.buse.sdfsdev;

import java.io.File;

import java.util.ArrayList;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.SDFSService;
import org.opendedup.util.OSValidator;

public class SDFSVolMgr {

	public static Options buildOptions() {
		Options options = new Options();
		options.addOption(
				"osdev",
				true,
				"The os device to map to. \n e.g. /dev/nbd0");
		options.addOption(
				"blockdev",
				true,
				"The sdfs block device to map to. \n e.g. dev0");
		options.addOption("d", false, "verbose debug output");
		options.addOption("v", true, "sdfs volume to mount \ne.g. dedup");
		options.addOption("vc", true,
				"sdfs volume configuration file to mount \ne.g. /etc/sdfs/dedup-volume-cfg.xml");
		options.addOption("c", false,
				"sdfs volume will be compacted and then exit");
		options.addOption(
				"forcecompact",
				false,
				"sdfs volume will be compacted even if it is missing blocks. This option is used in conjunction with -c");
		options.addOption(
				"rv",
				true,
				"comma separated list of remote volumes that should also be accounted for when doing garbage collection. "
						+ "If not entered the volume will attempt to identify other volumes in the cluster.");
		options.addOption("h", false, "displays available options");
		options.addOption("nossl", false,
				"If set ssl will not be used sdfscli traffic.");
		return options;
	}

	public static void main(String[] args) throws ParseException {
		checkJavaVersion();
		String volumeConfigFile = null;
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		boolean useSSL = true;
		CommandLine cmd = parser.parse(options, args);
		ArrayList<String> volumes = new ArrayList<String>();
		if (cmd.hasOption("h")) {
			printHelp(options);
			System.exit(1);
		}
		if (cmd.hasOption("rv")) {
			StringTokenizer st = new StringTokenizer(cmd.getOptionValue("rv"),
					",");
			while (st.hasMoreTokens()) {
				volumes.add(st.nextToken());
			}
		}
		String volname = "SDFS";
		if (cmd.hasOption("c")) {
			Main.runCompact = true;
			if (cmd.hasOption("forcecompact"))
				Main.forceCompact = true;
		}
		if (cmd.hasOption("v")) {
			File f = new File("/etc/sdfs/" + cmd.getOptionValue("v").trim()
					+ "-volume-cfg.xml");
			volname = f.getName();
			if (!f.exists()) {
				System.out.println("Volume configuration file " + f.getPath()
						+ " does not exist");
				System.exit(-1);
			}
			volumeConfigFile = f.getPath();
		} else if (cmd.hasOption("vc")) {
			File f = new File(cmd.getOptionValue("vc").trim());
			volname = f.getName();
			if (!f.exists()) {
				System.out.println("Volume configuration file " + f.getPath()
						+ " does not exist");
				System.exit(-1);
			}
			volumeConfigFile = f.getPath();
		} else {
			File f = new File("/etc/sdfs/" + args[0].trim() + "-volume-cfg.xml");
			volname = f.getName();
			if (!f.exists()) {
				System.out.println("Volume configuration file " + f.getPath()
						+ " does not exist");
				System.exit(-1);
			}
			volumeConfigFile = f.getPath();
		}
		if (cmd.hasOption("nossl")) {
			useSSL = false;
		}

		if (volumeConfigFile == null) {
			System.out
					.println("error : volume or path to volume configuration file not defined");
			printHelp(options);
			System.exit(-1);
		}
		if (OSValidator.isUnix())
			Main.logPath = "/var/log/sdfs/" + volname + ".log";
		if (OSValidator.isWindows())
			Main.logPath = Main.volume.getPath() + "\\log\\"
					+ Main.volume.getName() + ".log";
		Main.blockDev = true;
		SDFSService sdfsService = new SDFSService(volumeConfigFile, volumes);
		if (cmd.hasOption("d")) {
			SDFSLogger.setLevel(0);
		}
		try {
			sdfsService.start(useSSL);
		} catch (Throwable e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("Exiting because " + e1.toString());
			System.exit(-1);
		}
		
		try {
			SDFSBlockDev dev = new SDFSBlockDev(cmd.getOptionValue("blockdev"),cmd.getOptionValue("osdev"),Main.volume.getPath(),Main.volume.getCapacity());
			ShutdownHook shutdownHook = new ShutdownHook(sdfsService,
					cmd.getOptionValue("m"),dev);
			Runtime.getRuntime().addShutdownHook(shutdownHook);
			dev.startBlockDev();
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter
				.printHelp(
						"mksdfsdev -osdev <device name e.g. /dev/nbd0> -blockdev <sdfs blockdevice to map to> "
								+ "-[v|vc] <volume name to mount | path to volume config file> -p <TCP Management Port> -rv <comma separated list of remote volumes> ",
						options);
	}

	private static void checkJavaVersion() {
		Properties sProp = java.lang.System.getProperties();
		String sVersion = sProp.getProperty("java.version");
		sVersion = sVersion.substring(0, 3);
		Float f = Float.valueOf(sVersion);
		if (f.floatValue() < (float) 1.7) {
			System.out.println("Java version must be 1.7 or newer");
			System.out
					.println("To get Java 7 go to https://jdk7.dev.java.net/");
			System.exit(-1);
		}
	}
}
