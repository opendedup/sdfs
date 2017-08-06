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
		String oeChunkStoreEncryptionKey = Main.eChunkStoreEncryptionKey;
		try {
			
			String salt = HashFunctions.getRandomString(6);
			
			String password = HashFunctions.getSHAHash(newPassword.getBytes(),
					salt.getBytes());
			Main.sdfsPassword = password;
			Main.sdfsPasswordSalt = salt;
			
			if(Main.eChunkStoreEncryptionKey != null) {
				byte [] ec = EncryptUtils.encryptCBC(Main.chunkStoreEncryptionKey.getBytes(), newPassword, Main.chunkStoreEncryptionIV);
				Main.eChunkStoreEncryptionKey = BaseEncoding.base64Url().encode(ec);
			} if(Main.eCloudSecretKey != null) {
				byte [] ec = EncryptUtils.encryptCBC(Main.eCloudSecretKey.getBytes(), newPassword, Main.chunkStoreEncryptionIV);
				Main.eCloudSecretKey = BaseEncoding.base64Url().encode(ec);
			}
			Main.volume.writeUpdate();
			return "password changed";
		} catch (Exception e) {
			SDFSLogger.getLog().error(
					"password could not be changed" + e.toString(), e);
			Main.sdfsPassword = oldPassword;
			Main.sdfsPasswordSalt = oldSalt;
			Main.eChunkStoreEncryptionKey = oeChunkStoreEncryptionKey;
			Main.eCloudSecretKey = oeCloudSecretKey;
			throw new IOException("password could not be changed");
		}
	}

}
