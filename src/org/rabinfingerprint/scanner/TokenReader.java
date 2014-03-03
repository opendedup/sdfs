package org.rabinfingerprint.scanner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TokenReader {
	private int offset;
	private int count;
	private String string;
	private Matcher matcher;

	public static final String LINE_SEPARATOR_PATTERN = "\r\n|[\n\r\u2028\u2029\u0085]";
	public static final String LINE_PATTERN = ".*(?:" + LINE_SEPARATOR_PATTERN + ")|.+$";

	public TokenReader(String string) {
		this(Pattern.compile(LINE_PATTERN), string);
	}

	public TokenReader(Pattern pattern, String string) {
		this.offset = 0;
		this.count = 0;
		this.string = string;
		this.matcher = pattern.matcher(string);
	}

	public String get() {
		if (!matcher.find()) return null;
		offset = matcher.start();
		count += 1;
		final String s = matcher.group(0);
		matcher.region(matcher.end(), string.length());
		return s;
	}

	public int getOffset() {
		return offset;
	}

	public int getCount() {
		return count;
	}

}
