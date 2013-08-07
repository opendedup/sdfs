package org.opendedup.sdfs.mgmt;


import java.io.IOException;

import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class SetPasswordCmd implements XtendedCmd {

	@Override
	public String getResult(String oldUserName,String newPassword) throws IOException {
		return setPassword(newPassword);
	}

	private String setPassword(String newPassword)
			throws IOException {
		
		
		String oldSalt = Main.sdfsPasswordSalt;
		String oldPassword = Main.sdfsPassword;	
		try {
			
			String salt = HashFunctions.getRandomString(6);
			String password = HashFunctions.getSHAHash(newPassword.getBytes(), salt.getBytes());
			Main.sdfsPassword = password;
			Main.sdfsPasswordSalt = salt;
			return "password changed";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"password could not be changed" + e.toString(), e);
			Main.sdfsPassword = oldPassword;
			Main.sdfsPasswordSalt = oldSalt;
			throw new IOException(
					"password could not be changed");
		}
	}

}
