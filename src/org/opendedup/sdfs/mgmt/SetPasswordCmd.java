package org.opendedup.sdfs.mgmt;


import java.io.IOException;

import org.opendedup.hashing.HashFunctions;
import org.opendedup.sdfs.Main;
import org.opendedup.util.SDFSLogger;

public class SetPasswordCmd implements XtendedCmd {

	@Override
	public String getResult(String oldUserName,String newPassword) throws IOException {
		return setPassword(newPassword);
	}

	private String setPassword(String newPassword)
			throws IOException {
		
		
		String oldSalt = Main.sdfsCliSalt;
		String oldPassword = Main.sdfsCliPassword;	
		try {
			
			String salt = HashFunctions.getRandomString(6);
			String password = HashFunctions.getSHAHash(newPassword.getBytes(), salt.getBytes());
			Main.sdfsCliPassword = password;
			Main.sdfsCliSalt = salt;
			return "password changed";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"password could not be changed" + e.toString(), e);
			Main.sdfsCliPassword = oldPassword;
			Main.sdfsCliSalt = oldSalt;
			throw new IOException(
					"password could not be changed");
		}
	}

}
