package org.rabinfingerprint.scanner;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class LineNumberIndex {
	public static final String LINE_SEPARATOR = "\r\n|[\n\r\u2028\u2029\u0085]";
	public static final String LINE = ".*(?:" + LINE_SEPARATOR + ")|.+$";
	public static final String TERMINAL_LINE = ";\\s*((//|/\\*).*)?$";
	public static final Pattern LINE_SEPARATOR_PATTERN = Pattern.compile(LINE_SEPARATOR);
	public static final Pattern LINE_PATTERN = Pattern.compile(LINE);
	public static final Pattern TERMINAL_LINE_PATTERN = Pattern.compile(TERMINAL_LINE);

	private final TreeMap<Long, Long> index;
	private final String fileString;

	public LineNumberIndex(String fileString) {
		// build line number index
		this.index = new TreeMap<Long, Long>();
		this.fileString = fileString;
		final TokenReader lineMatcher = new TokenReader(LINE_PATTERN, fileString);
		long lines = 1;
		long chars = 0;
		index.put(chars, lines);
		String str = null;
		while ((str = lineMatcher.get()) != null) {
			lines += 1;
			chars += str.length();
			index.put(chars, lines);
		}
	}

	public int getLineNumber(long characterOffset) {
		final SortedMap<Long, Long> head = index.headMap(characterOffset + 1);
		if(head.isEmpty()) return -1;
		return head.get(head.lastKey()).intValue();
	}

	public String getLine(long characterOffset) {
		final SortedMap<Long, Long> head = index.headMap(characterOffset + 1);
		final SortedMap<Long, Long> tail = index.tailMap(characterOffset);
		int i0 = (int) ((head.isEmpty()) ? 0 : head.lastKey());
		int i1 = (int) ((tail.isEmpty()) ? fileString.length() : tail.firstKey());
		return fileString.substring(i0, i1);
	}
}
