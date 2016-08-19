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
package org.opendedup.sdfs.filestore.cloud.utils;

import java.io.File;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opendedup.util.OSValidator;

import org.opendedup.sdfs.filestore.cloud.utils.EncyptUtils;

public class FileUtils {
	// private static final String STATTR="DSHSTR";
	private static final String BATTR = "DSHBAR";
	private static final String IATTR = "DSHINT";
	private static final String LATTR = "DSHLNG";

	public static HashMap<String, String> getFileMetaData(File file,
			boolean encrypt) throws IOException {
		HashMap<String, String> md = new HashMap<String, String>();
		if (OSValidator.isUnix()) {
			boolean isSymbolicLink = Files.isSymbolicLink(file.toPath());
			if (isSymbolicLink)
				return md;
			else {
				Path p = file.toPath();
				int uid = (Integer) Files.getAttribute(p, "unix:uid");
				int gid = (Integer) Files.getAttribute(p, "unix:gid");
				int mode = (Integer) Files.getAttribute(p, "unix:mode");
				long mtime = file.lastModified();
				md.put(IATTR + "uid", EncyptUtils.encInt(uid, encrypt));
				md.put(IATTR + "gid", EncyptUtils.encInt(gid, encrypt));
				md.put(IATTR + "mode", EncyptUtils.encInt(mode, encrypt));
				md.put(LATTR + "mtime", EncyptUtils.encLong(mtime, encrypt));
				UserDefinedFileAttributeView view = Files.getFileAttributeView(
						p, UserDefinedFileAttributeView.class);
				List<String> l = view.list();
				for (String s : l) {
					byte[] b = new byte[view.size(s)];
					ByteBuffer bf = ByteBuffer.wrap(b);
					view.read(s, bf);
					md.put(BATTR + s, EncyptUtils.encBar(b, encrypt));
				}
				return md;
			}
		} else {
			return md;
		}

	}

	public static boolean fileValid(File f, byte[] hash) throws IOException {
		try (FileInputStream inputStream = new FileInputStream(f)) {
			MessageDigest digest = MessageDigest.getInstance("MD5");

			byte[] bytesBuffer = new byte[1024];
			int bytesRead = -1;

			while ((bytesRead = inputStream.read(bytesBuffer)) != -1) {
				digest.update(bytesBuffer, 0, bytesRead);
			}
			byte[] b = digest.digest();
			return Arrays.equals(b, hash);

			// initialize blob properties and assign md5 content
			// generated.

		} catch (Exception ex) {
			throw new IOException("Could not generate hash from file "
					+ f.getPath(), ex);
		}
	}

	public static void setFileMetaData(File f, Map<String, String> md,
			boolean encrypt) throws IOException {
		if (OSValidator.isUnix()) {
			boolean isSymbolicLink = Files.isSymbolicLink(f.toPath());
			if (isSymbolicLink)
				return;
			else {
				Set<String> keys = md.keySet();
				Path p = f.toPath();
				UserDefinedFileAttributeView view = Files.getFileAttributeView(
						p, UserDefinedFileAttributeView.class);
				for (String s : keys) {
					if (s.startsWith(BATTR)) {
						byte[] av = EncyptUtils.decBar(md.get(s), encrypt);
						view.write(s.substring(BATTR.length()),
								ByteBuffer.wrap(av));
					} else if (s.startsWith(IATTR)) {
						String nm = s.substring(IATTR.length());
						if (nm.equalsIgnoreCase("uid"))
							Files.setAttribute(
									p,
									"unix:uid",
									Integer.valueOf(EncyptUtils.decInt(
											md.get(s), encrypt)),
									LinkOption.NOFOLLOW_LINKS);
						if (nm.equalsIgnoreCase("gid"))
							Files.setAttribute(
									p,
									"unix:gid",
									Integer.valueOf(EncyptUtils.decInt(
											md.get(s), encrypt)),
									LinkOption.NOFOLLOW_LINKS);
						if (nm.equalsIgnoreCase("mode"))
							Files.setAttribute(
									p,
									"unix:mode",
									Integer.valueOf(EncyptUtils.decInt(
											md.get(s), encrypt)),
									LinkOption.NOFOLLOW_LINKS);

					} else if (s.startsWith(LATTR)) {
						String nm = s.substring(LATTR.length());
						if (nm.equalsIgnoreCase("mtime")) {
							f.setLastModified(EncyptUtils.decLong(md.get(s),
									encrypt));
						}
					}
				}
			}
		}
	}
}
