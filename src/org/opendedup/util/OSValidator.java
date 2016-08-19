/*******************************************************************************
 * Copyright (C) 2016 Sam Silverberg sam.silverberg@gmail.com	
 *
 * This file is part of OpenDedupe SDFS.
 *
 * OpenDedupe SDFS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * OpenDedupe SDFS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/
package org.opendedup.util;

import java.io.File;

import org.opendedup.sdfs.windows.utils.WinRegistry;

public class OSValidator {

	public static void main(String[] args) {
		if (isWindows()) {
			System.out.println("This is Windows " + getProgramBasePath() + " "
					+ getConfigPath());
		} else if (isMac()) {
			System.out.println("This is Mac " + getProgramBasePath() + " "
					+ getConfigPath());
		} else if (isUnix()) {
			System.out.println("This is Unix or Linux " + getProgramBasePath()
					+ " " + getConfigPath());
		} else {
			System.out.println("Your OS is not support!!");
		}
		System.out.println(Runtime.getRuntime().maxMemory());
		System.out.println(Runtime.getRuntime().availableProcessors());
	}

	public static boolean isWindows() {

		String os = System.getProperty("os.name").toLowerCase();
		// windows
		return (os.indexOf("win") >= 0);

	}

	public static boolean isMac() {

		String os = System.getProperty("os.name").toLowerCase();
		// Mac
		return (os.indexOf("mac") >= 0);

	}

	public static boolean isUnix() {

		String os = System.getProperty("os.name").toLowerCase();
		// linux or unix
		return (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0);

	}

	public static String getProgramBasePath() {
		if (isUnix() || isMac())
			return "/opt/sdfs/";
		else {
			try {
				return WinRegistry.readString(WinRegistry.HKEY_LOCAL_MACHINE,
						"SOFTWARE\\Wow6432Node\\SDFS", "path") + File.separator;
			} catch (Exception e) {
				return System.getenv("programfiles") + File.separator + "sdfs"
						+ File.separator;
			}
		}
	}

	public static String getConfigPath() {
		if (isUnix() || isMac())
			return "/etc/sdfs/";
		else
			try {
				return WinRegistry.readString(WinRegistry.HKEY_LOCAL_MACHINE,
						"SOFTWARE\\Wow6432Node\\SDFS", "path")
						+ File.separator
						+ "etc" + File.separator;
			} catch (Exception e) {
				return System.getenv("programfiles") + File.separator + "sdfs"
						+ File.separator + "etc" + File.separator;
			}
	}
}
