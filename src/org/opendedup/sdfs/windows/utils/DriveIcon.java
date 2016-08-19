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
package org.opendedup.sdfs.windows.utils;

import java.io.File;
import java.lang.reflect.InvocationTargetException;

public class DriveIcon {

	public static void addIcon(String drive) throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {
		drive = drive.toUpperCase();
		String drivekey = "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\DriveIcons\\"
				+ drive;
		String iconkey = "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\DriveIcons\\"
				+ drive + "\\DefaultIcon";
		String deskey = "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\DriveIcons\\"
				+ drive + "\\DefaultLabel";
		String iconpath = WinRegistry.readString(
				WinRegistry.HKEY_LOCAL_MACHINE, "SOFTWARE\\Wow6432Node\\SDFS",
				"path")
				+ File.separator + "sdfs.ico";
		WinRegistry.createKey(WinRegistry.HKEY_LOCAL_MACHINE, drivekey);
		WinRegistry.createKey(WinRegistry.HKEY_LOCAL_MACHINE, iconkey);
		WinRegistry.createKey(WinRegistry.HKEY_LOCAL_MACHINE, deskey);
		WinRegistry.writeStringValue(WinRegistry.HKEY_LOCAL_MACHINE, iconkey,
				"", iconpath);
		WinRegistry.writeStringValue(WinRegistry.HKEY_LOCAL_MACHINE, deskey,
				"", "SDFS Deduplicated Volume");
	}

	public static void deleteIcon(String drive)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		drive = drive.toUpperCase();
		String drivekey = "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\DriveIcons\\"
				+ drive;
		String iconkey = "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\DriveIcons\\"
				+ drive + "\\DefaultIcon";
		String deskey = "SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\DriveIcons\\"
				+ drive + "\\DefaultLabel";
		WinRegistry.deleteValue(WinRegistry.HKEY_LOCAL_MACHINE, iconkey, "");
		WinRegistry.deleteValue(WinRegistry.HKEY_LOCAL_MACHINE, deskey, "");
		WinRegistry.deleteKey(WinRegistry.HKEY_LOCAL_MACHINE, iconkey);
		WinRegistry.deleteKey(WinRegistry.HKEY_LOCAL_MACHINE, deskey);
		WinRegistry.deleteKey(WinRegistry.HKEY_LOCAL_MACHINE, drivekey);
	}

}
