package fuse.SDFS;

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
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.servers.SDFSService;
import org.opendedup.util.OSValidator;

import fuse.FuseMount;

public class MountSDFS implements Daemon, Runnable {
	private static final Log log = LogFactory.getLog(SDFSFileSystem.class);
	private static String[] sFal = null;
	private static SDFSService sdfsService;
	private static String mountOptions;
	protected static ShutdownHook shutdownHook = null;
	private static String password = null;
	private static boolean nm;

	public static Options buildOptions() {
		Options options = new Options();
		options.addOption(
				"o",
				true,
				"fuse mount options.\nWill default to: \ndirect_io,big_writes,allow_other,fsname=SDFS");
		options.addOption("cfr", false,
				"Restores files from cloud storage if the backend cloud store supports it");
		options.addOption("d", false, "debug output");
		options.addOption("s", false, "Run single threaded");
		options.addOption("p", true, "port to use for sdfs cli");
		options.addOption("cc", false, "Runs Consistency Check");
		options.addOption("e", true, "password to decrypt config");
		options.addOption("nm", false, "disable drive mount");
		options.addOption("m", true,
				"mount point for SDFS file system \n e.g. /media/dedup");
		options.addOption("v", true, "sdfs volume to mount \ne.g. dedup");
		options.addOption("vc", true,
				"sdfs volume configuration file to mount \ne.g. /etc/sdfs/dedup-volume-cfg.xml");
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
		BasicConfigurator.configure();
		setup(args);
		try {

			FuseMount.mount(sFal, new SDFSFileSystem(Main.volume.getPath(),
					Main.volumeMountPoint), log);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void printHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter
				.printHelp(
						"mount.sdfs -o <fuse options> -m <mount point> "
								+ "-r <path to chunk store routing file> -[v|vc] <volume name to mount | path to volume config file> -p <TCP Management Port> -rv <comma separated list of remote volumes> ",
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

	private static void setup(String[] args) throws ParseException {
		checkJavaVersion();
		int port = -1;
		String volumeConfigFile = null;
		CommandLineParser parser = new PosixParser();
		Options options = buildOptions();
		boolean useSSL = true;
		CommandLine cmd = parser.parse(options, args);
		ArrayList<String> fal = new ArrayList<String>();
		ArrayList<String> volumes = new ArrayList<String>();
		fal.add("-f");
		if (cmd.hasOption("h")) {
			printHelp(options);
			System.exit(1);
		}
		if (cmd.hasOption("nm")) {
			nm = true;
		}
		if (cmd.hasOption("cc")) {
			Main.runConsistancyCheck = true;
		}
		if (cmd.hasOption("d")) {
			fal.add("-d");
		}
		if (cmd.hasOption("e")) {
			password = cmd.getOptionValue("e");
		}
		if (cmd.hasOption("s")) {
			fal.add("-s");
		}
		if (cmd.hasOption("p")) {
			port = Integer.parseInt(cmd.getOptionValue("p"));
		}
		if (cmd.hasOption("rv")) {
			StringTokenizer st = new StringTokenizer(cmd.getOptionValue("rv"),
					",");
			while (st.hasMoreTokens()) {
				volumes.add(st.nextToken());
			}
		}
		if (!cmd.hasOption("m")) {
			fal.add(args[1]);
			Main.volumeMountPoint = args[1];
		} else {
			fal.add(cmd.getOptionValue("m"));
			Main.volumeMountPoint = cmd.getOptionValue("m");
		}

		String volname = "SDFS";
		if (cmd.hasOption("c")) {
			Main.runCompact = true;
			if (cmd.hasOption("forcecompact"))
				Main.forceCompact = true;
		}
		if (cmd.hasOption("cfr")) {
			Main.syncDL = true;
			Main.runConsistancyCheck = true;
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
		sdfsService = new SDFSService(volumeConfigFile, volumes);
		if (cmd.hasOption("d")) {
			SDFSLogger.setLevel(0);
		}
		try {
			sdfsService.start(useSSL, port,password);
		} catch (Throwable e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("Exiting because " + e1.toString());
			System.exit(-1);
		}
		shutdownHook = new ShutdownHook(sdfsService,
				cmd.getOptionValue("m"));
		mountOptions = cmd.getOptionValue("m");
		Runtime.getRuntime().addShutdownHook(shutdownHook);
		if (cmd.hasOption("o")) {
			fal.add("-o");
			fal.add("modules=iconv,from_code=UTF-8,to_code=UTF-8,direct_io,allow_other,nonempty,big_writes,allow_other,fsname=sdfs:"
					+ volumeConfigFile
					+ ":"
					+ Main.sdfsCliPort
					+ ","
					+ cmd.getOptionValue("o"));
		} else {
			fal.add("-o");
			fal.add("modules=iconv,from_code=UTF-8,to_code=UTF-8,direct_io,allow_other,nonempty,big_writes,allow_other,fsname=sdfs:"
					+ volumeConfigFile + ":" + Main.sdfsCliPort);
		}
		sFal = new String[fal.size()];
		fal.toArray(sFal);
		for (int i = 0; i < sFal.length; i++) {
			SDFSLogger.getLog().info("Mount Option : " + sFal[i]);
		}
	}

	@Override
	public void destroy() {
		sdfsService = null;
		mountOptions = null;
	}

	@Override
	public void init(DaemonContext arg0) throws DaemonInitException, Exception {
		setup(arg0.getArguments());
	}

	@Override
	public void start() throws Exception {
		if(!nm) {
		MountSDFS sd = new MountSDFS();
		Thread th = new Thread(sd);
		th.start();
		}
	}

	@Override
	public void stop() throws Exception {
		SDFSLogger.getLog().info("Please Wait while shutting down SDFS");
		SDFSLogger.getLog().info("Data Can be lost if this is interrupted");
		sdfsService.stop();
		SDFSLogger.getLog().info("All Data Flushed");
		try {
			Process p = Runtime.getRuntime().exec("umount " + mountOptions);
			p.waitFor();
		} catch (Exception e) {
		}
		SDFSLogger.getLog().info("SDFS Shut Down Cleanly");
	}

	@Override
	public void run() {
		try {
			FuseMount.mount(sFal, new SDFSFileSystem(Main.volume.getPath(),
					Main.volumeMountPoint), log);
			if(shutdownHook != null)
				shutdownHook.shutdown();
			System.exit(0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
