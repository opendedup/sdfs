package org.opendedup.util;

import java.security.Security;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * Basic symmetric encryption example
 */
public class MainClass {
  public static void main(String[] args) throws Exception {
    Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());        
    byte[] input = " www.java2s.com ".getBytes();
    byte[] keyBytes = HashFunctions.getMD5ByteHash("Password".getBytes());

    SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");
    Cipher cipher = Cipher.getInstance("AES/ECB/NoPadding", "BC");
    System.out.println("input text : " + new String(input));

    // encryption pass

    byte[] cipherText = new byte[input.length];
    cipher.init(Cipher.ENCRYPT_MODE, key);
    int ctLength = cipher.update(input, 0, input.length, cipherText, 0);
    ctLength += cipher.doFinal(cipherText, ctLength);
    System.out.println("cipher text: " + new String(cipherText) + " bytes: " + ctLength);

    // decryption pass

    byte[] plainText = new byte[ctLength];
    cipher.init(Cipher.DECRYPT_MODE, key);
    int ptLength = cipher.update(cipherText, 0, ctLength, plainText, 0);
    ptLength += cipher.doFinal(plainText, ptLength);
    System.out.println("plain text : " + new String(plainText) + " bytes: " + ptLength);
  }
}