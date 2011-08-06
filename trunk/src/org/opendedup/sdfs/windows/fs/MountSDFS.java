package org.opendedup.sdfs.windows.fs;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.SDFSService;
import org.opendedup.util.OSValidator;

public class MountSDFS {

	public static Options buildOptions() {
		Options options = new Options();
		options.addOption("m", true,
				"the drive letter for SDFS file system \n e.g. \'S\'");
		options.addOption("r", true,
				"path to chunkstore routing file. \n Will default to: \n"
						+ OSValidator.getConfigPath() + "routing-config.xml");
		options.addOption("v", true, "sdfs volume to mount \ne.g. dedup");
		options.addOption(
				"vc",
				true,
				"sdfs volume configuration file to mount \ne.g. "
						+ OSValidator.getConfigPath() + "dedup-volume-cfg.xml");
		options.addOption("h", false, "display available options");
		return options;
	}

	public static void main(String[] args) throws ParseException {
		checkJavaVersion();
		String volumeConfigFile = null;
		String routingConfigFile = null;
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		CommandLine cmd = parser.parse(options, args);
		ArrayList<String> fal = new ArrayList<String>();
		fal.add("-f");
		if (cmd.hasOption("h")) {
			printHelp(options);
			System.exit(1);
		}

		if (!cmd.hasOption("m")) {
			printHelp(options);
			System.exit(0);
		} else {
			fal.add(cmd.getOptionValue("m"));
			Main.volumeMountPoint = cmd.getOptionValue("m");
		}
		if (cmd.hasOption("r")) {
			File f = new File(cmd.getOptionValue("r").trim());
			if (!f.exists()) {
				System.out.println("Routing configuration file " + f.getPath()
						+ " does not exist");
				System.exit(-1);
			}
			routingConfigFile = f.getPath();
		}

		if (cmd.hasOption("v")) {
			File f = new File(OSValidator.getConfigPath()
					+ cmd.getOptionValue("v").trim() + "-volume-cfg.xml");
			if (!f.exists()) {
				System.out.println("Volume configuration file " + f.getPath()
						+ " does not exist");
				System.exit(-1);
			}
			volumeConfigFile = f.getPath();
		} else if (cmd.hasOption("vc")) {
			File f = new File(cmd.getOptionValue("vc").trim());
			if (!f.exists()) {
				System.out.println("Volume configuration file " + f.getPath()
						+ " does not exist");
				System.exit(-1);
			}
			volumeConfigFile = f.getPath();
		} else {
			File f = new File(OSValidator.getConfigPath() + args[0].trim()
					+ "-volume-cfg.xml");
			if (!f.exists()) {
				System.out.println("Volume configuration file " + f.getPath()
						+ " does not exist");
				System.exit(-1);
			}
			volumeConfigFile = f.getPath();
		}

		if (volumeConfigFile == null) {
			System.out
					.println("error : volume or path to volume configuration file not defined");
			printHelp(options);
			System.exit(-1);
		}

		SDFSService sdfsService = new SDFSService(volumeConfigFile,
				routingConfigFile);

		try {
			sdfsService.start();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("Exiting because " + e1.toString());
			System.exit(-1);
		}
		ShutdownHook shutdownHook = new ShutdownHook(sdfsService,
				cmd.getOptionValue("m"));
		Runtime.getRuntime().addShutdownHook(shutdownHook);
		try {
			String[] sFal = new String[fal.size()];
			fal.toArray(sFal);
			for (int i = 0; i < sFal.length; i++) {
				System.out.println(sFal[i]);
			}
			WinSDFS sdfs = new WinSDFS();
			sdfs.mount(cmd.getOptionValue("m"), Main.volume.getPath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter
				.printHelp(
						"mount.sdfs -f -o <fuse options> -m <mount point> "
								+ "-r <path to chunk store routing file> -[v|vc] <volume name to mount | path to volume config file> ",
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
