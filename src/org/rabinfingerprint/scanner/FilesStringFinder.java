package org.rabinfingerprint.scanner;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.rabinfingerprint.scanner.StringFinder.StringMatch;
import org.rabinfingerprint.scanner.StringFinder.StringMatchVisitor;
import org.rabinfingerprint.scanner.StringFinder.StringMatcher;

public class FilesStringFinder {

	public static void find(
			final String baseDirectory,
			final String filePattern,
			final String targetString,
			final StringMatchVisitor visitor)
			throws IOException {
		final List<File> files = FileFinder.getFilesMatching(baseDirectory, filePattern);
		find(files, targetString, visitor);
	}

	public static void find(
			final List<File> files,
			final String targetString,
			final StringMatchVisitor visitor)
			throws IOException {
		
		// create finder
		final StringFinder scanner = new StringFinder(targetString);
		
		// find
		for (final File file : files) {
			final String str = IOUtils.readEntireFile(file);
			final StringMatcher sm = scanner.matcher(str);
			LineNumberIndex index = null;
			
			// find matching strings
			while (sm.find()) {
				if (index == null) index = new LineNumberIndex(str);
				final int off = sm.getStart();
				final int lineOffset = index.getLineNumber(off);
				final String line = index.getLine(off);
				final StringMatch match = new StringMatch(file, line, lineOffset, off);
				visitor.found(match);
			}
		}
	}
	
	public static void findThreaded(
			final int threadCount,
			final List<File> files,
			final String targetString,
			final StringMatchVisitor visitor) {
		
		// create finder
		final StringFinder scanner = new StringFinder(targetString);
		
		// create thread pool
		final ThreadPoolExecutor executor = new ThreadPoolExecutor(threadCount, threadCount, 200, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		
		// find
		for (final File file : files) {
			executor.submit(new Runnable() {
				public void run() {
					try {
						final String str = IOUtils.readEntireFile(file);
						final StringMatcher sm = scanner.matcher(str);
						LineNumberIndex index = null;

						// find matching strings
						while (sm.find()) {
							if (index == null) index = new LineNumberIndex(str);
							final int off = sm.getStart();
							final int lineOffset = index.getLineNumber(off);
							final String line = index.getLine(off);
							final StringMatch match = new StringMatch(file, line, lineOffset, off);
							visitor.found(match);
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			});
		}
		
		try {
			executor.shutdown();
			executor.awaitTermination(10*60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
		final String curDir = System.getProperty("user.dir");
		final String filePattern = ".*\\.java";
		final String targetString = "asdf fdsa asdf";

		System.out.println(String.format("Searching for \"%s\"", args[0]));
		find(curDir, filePattern, targetString, new StringMatchVisitor() {
			public void found(final StringMatch match) {
				System.out.println(String.format("%s line %d (%s)", match.getFile().getName(), match.getLineOffset(), match.getFile().getAbsolutePath()));
			}
		});
	}
}
