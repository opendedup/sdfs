package fuse.SDFS.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class FileUtils {

	public static void main(String[] args) throws IOException {
		File f = new File(args[0]);
		Path p = f.toPath();
		PosixFileAttributeView view = Files
				.getFileAttributeView(p, PosixFileAttributeView.class);
		PosixFileAttributes attrs1 = view
				.readAttributes();
		Set<PosixFilePermission> perms = attrs1.permissions();
		Iterator<PosixFilePermission> iter = perms.iterator();
		String s = "---------";
		char[] smak = s.toCharArray();
		while (iter.hasNext()) {
			PosixFilePermission perm = iter.next();
			if (perm.toString().equalsIgnoreCase("OWNER_READ"))
				smak[0] = 'r';
			if (perm.toString().equalsIgnoreCase("OWNER_WRITE"))
				smak[1] = 'w';

			if (perm.toString().equalsIgnoreCase("OWNER_EXECUTE"))
				smak[2] = 'x';

			if (perm.toString().equalsIgnoreCase("GROUP_READ"))
				smak[3] = 'r';

			if (perm.toString().equalsIgnoreCase("GROUP_WRITE"))
				smak[4] = 'w';

			if (perm.toString().equalsIgnoreCase("GROUP_EXECUTE"))
				smak[5] = 'x';

			if (perm.toString().equalsIgnoreCase("OTHERS_READ"))
				smak[6] = 'r';

			if (perm.toString().equalsIgnoreCase("OTHERS_WRITE"))
				smak[7] = 'w';

			if (perm.toString().equalsIgnoreCase("OTHERS_EXECUTE"))
				smak[8] = 'x';

		}
		UserPrincipalLookupService look = FileSystems.getDefault()
				.getUserPrincipalLookupService();
		System.out.println(look.lookupPrincipalByName("0").getName());
		System.out.println(String.copyValueOf(smak));
		System.out.println(parseFilePermissions(700));
		String mode = PosixFilePermissions.toString(attrs1.permissions());
		String mm = parseFilePermissions(mode.substring(0, 3)) + ""
				+ parseFilePermissions(mode.substring(3, 6)) + ""
				+ parseFilePermissions(mode.substring(6, 9));
		System.out.format("%s %s %s %s", mode, attrs1.owner(), attrs1.group(),
				Integer.parseInt(mm));

	}

	public static int parseFilePermissions(String mode) {
		if (mode.equalsIgnoreCase("---"))
			return 0;
		if (mode.equalsIgnoreCase("--x"))
			return 1;
		if (mode.equalsIgnoreCase("-w-"))
			return 2;
		if (mode.equalsIgnoreCase("-wx"))
			return 3;
		if (mode.equalsIgnoreCase("r--"))
			return 4;
		if (mode.equalsIgnoreCase("r-x"))
			return 5;
		if (mode.equalsIgnoreCase("rw-"))
			return 6;
		if (mode.equalsIgnoreCase("rwx"))
			return 7;
		else
			return 0;

	}

	public static String parseFilePermissions(int mode) {
		String mm = Integer.toString(mode);
		char[] c = mm.toCharArray();
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < c.length; i++) {
			System.out.println("parsing " + c[i]);
			if (c[i] == '0')
				b.append("---");
			if (c[i] == '1')
				b.append("--x");
			if (c[i] == '2')
				b.append("-w-");
			if (c[i] == '3')
				b.append("-wx");
			if (c[i] == '4')
				b.append("r--");
			if (c[i] == '5')
				b.append("r-x");
			if (c[i] == '6')
				b.append("rw-");
			if (c[i] == '7')
				b.append("rwx");
		}
		return b.toString();

	}

	public static void setPermissions(String path, int permissions)
			throws IOException {
		HashSet<PosixFilePermission> set = new HashSet<PosixFilePermission>();
		char[] smak = parseFilePermissions(permissions).toCharArray();
		if (smak[0] == 'r')
			set.add(PosixFilePermission.OWNER_READ);
		if (smak[1] == 'w')
			set.add(PosixFilePermission.OWNER_WRITE);
		if (smak[2] == 'x')
			set.add(PosixFilePermission.OWNER_EXECUTE);
		if (smak[3] == 'r')
			set.add(PosixFilePermission.GROUP_READ);
		if (smak[4] == 'w')
			set.add(PosixFilePermission.GROUP_WRITE);
		if (smak[5] == 'x')
			set.add(PosixFilePermission.GROUP_EXECUTE);
		if (smak[6] == 'r')
			set.add(PosixFilePermission.OTHERS_READ);
		if (smak[7] == 'w')
			set.add(PosixFilePermission.OTHERS_WRITE);
		if (smak[8] == 'x')
			set.add(PosixFilePermission.OTHERS_EXECUTE);
		File f = new File(path);
		Path p = f.toPath();
		PosixFileAttributeView view = Files
				.getFileAttributeView(p, PosixFileAttributeView.class);
		view.setPermissions(set);
	}

	public static int getFilePermissions(String path) throws IOException {
		File f = new File(path);
		Path p = f.toPath();
		PosixFileAttributeView view = Files
				.getFileAttributeView(p, PosixFileAttributeView.class);

		PosixFileAttributes attrs1 = view
				.readAttributes();
		String mode = PosixFilePermissions.toString(attrs1.permissions());
		String mm = parseFilePermissions(mode.substring(0, 3)) + ""
				+ parseFilePermissions(mode.substring(3, 6)) + ""
				+ parseFilePermissions(mode.substring(6, 9));
		return Integer.parseInt(mm);
	}

}
