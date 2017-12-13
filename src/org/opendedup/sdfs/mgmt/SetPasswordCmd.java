package org.opendedup.sdfs.mgmt;

import java.io.IOException;

import org.opendedup.hashing.HashFunctions;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.util.EncryptUtils;

import com.google.common.io.BaseEncoding;

public class SetPasswordCmd implements XtendedCmd {

	@Override
	public String getResult(String oldUserName, String newPassword)
			throws IOException {
		return setPassword(newPassword);
	}

	private String setPassword(String newPassword) throws IOException {

		String oldSalt = Main.sdfsPasswordSalt;
		String oldPassword = Main.sdfsPassword;
		String oeCloudSecretKey = Main.eCloudSecretKey;
		try {
			
			String salt = HashFunctions.getRandomString(6);
			
			String password = HashFunctions.getSHAHash(newPassword.getBytes(),
					salt.getBytes());
			Main.sdfsPassword = password;
			Main.sdfsPasswordSalt = salt;
			
			 if(Main.eCloudSecretKey != null) {
				byte [] ec = EncryptUtils.encryptCBC(Main.cloudSecretKey.getBytes(), newPassword, Main.chunkStoreEncryptionIV);
				Main.eCloudSecretKey = BaseEncoding.base64Url().encode(ec);
			}
			Main.volume.writeUpdate();
			return "password changed";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"password could not be changed" + e.toString(), e);
			Main.sdfsPassword = oldPassword;
			Main.sdfsPasswordSalt = oldSalt;
			Main.eCloudSecretKey = oeCloudSecretKey;
			throw new IOException("password could not be changed");
		}
	}

}
