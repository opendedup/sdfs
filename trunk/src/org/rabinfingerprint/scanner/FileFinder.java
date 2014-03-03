package org.rabinfingerprint.scanner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FileFinder {

	public static interface FileVisitor {
		public void visitFile(File file);
		public void visitDirectory(File file);
	}

	public static void visitFilesRecursively(FileVisitor visitor, File directory, String filePattern, String directoryPattern, boolean recursively) {
		if (!directory.isDirectory()) directory = directory.getParentFile();
		if (!directory.getName().matches(directoryPattern)) return;
		visitor.visitDirectory(directory);
		final ArrayList<File> childDirectories = new ArrayList<File>();
		for (File file : directory.listFiles()) {
			if (file.isDirectory()) {
				childDirectories.add(file);
			} else if (file.isFile()) {
				if (!file.getName().matches(filePattern)) continue;
				visitor.visitFile(file);
			}
		}
		if (recursively) {
			for (File childDirectory : childDirectories) {
				visitFilesRecursively(visitor, childDirectory, filePattern, directoryPattern, recursively);
			}
		}
	}
	
	public static void visitDirectoriesRecursively(FileVisitor visitor, File directory, String pattern, boolean recursively)  {
		if (!directory.isDirectory()) directory = directory.getParentFile();
		if (!directory.getName().matches(pattern)) return;
		visitor.visitDirectory(directory);
		final ArrayList<File> childDirectories = new ArrayList<File>();
		for (File file : directory.listFiles()) {
			if (file.isDirectory()) {
				childDirectories.add(file);
			}
		}
		if (recursively) {
			for (File childDirectory : childDirectories) {
				visitDirectoriesRecursively(visitor, childDirectory, pattern, recursively);
			}
		}
		
	}

	public static List<File> getDirectoriesMatching(String basePath, String pattern, boolean recursively)  {
		File dir = new File(basePath);
		final List<File> dirs = new ArrayList<File>();
		if (!dir.exists()) return dirs;
		visitDirectoriesRecursively(new FileVisitor() {
			public void visitDirectory(File d) {
				dirs.add(d);
			}

			public void visitFile(File f) {
				// only finding directories
			}
		}, dir, pattern, recursively);
		return dirs;
	}

	public static List<File> getFilesMatching(String basePath, String filePattern) {
		return getFilesMatching(basePath, filePattern, "[^.].*", true);
	}
	
	public static List<File> getFilesMatching(String basePath, String filePattern, String directoryPattern, boolean recursively) {
		File file = new File(basePath);
		final List<File> files = new ArrayList<File>();
		if (!file.exists()) return files;
		visitFilesRecursively(new FileVisitor() {
			public void visitDirectory(File d) {
				// only finding files
			}

			public void visitFile(File f) {
				files.add(f);
			}
		}, file, filePattern, directoryPattern, recursively);
		return files;
	}
	
	public static String getExtensionPatterns(List<String> exts) {
		final StringBuilder sb = new StringBuilder();
		sb.append("(");
		for (String ext : exts) {
			sb.append(".*\\." + ext + "|");
		}
		sb.append(")");
		return sb.toString();
	}

	public static void main(String[] args) throws Exception {
		final String curDir = System.getProperty("user.dir");
		final List<File> files = getFilesMatching(curDir, ".*\\.java", "[^.].*", true);
		for (File file : files) {
			System.out.println("Found " + file.getName()); // (authorized)
		}
	}
}
