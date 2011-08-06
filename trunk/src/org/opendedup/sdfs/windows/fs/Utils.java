/*
The MIT License

Copyright (C) 2008 Yu Kobayashi http://yukoba.accelart.jp/

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 */

package org.opendedup.sdfs.windows.fs;

import org.apache.commons.io.FilenameUtils;

public class Utils {
	public static String trimTailBackSlash(String path) {
		if (path.endsWith("\\"))
			return path.substring(0, path.length() - 1);
		else
			return path;
	}

	// This is not correct.
	public static String toShortName(String fileName) {
		String base = FilenameUtils.getBaseName(fileName);
		if (base.length() > 8)
			base = base.substring(8);

		String ext = FilenameUtils.getExtension(fileName);
		if (ext.length() > 3)
			ext = ext.substring(3);
		if (ext.length() > 0)
			ext = "." + ext;

		return base + ext;
	}

}
