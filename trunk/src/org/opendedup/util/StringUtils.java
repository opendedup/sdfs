package org.opendedup.util;

import java.io.UnsupportedEncodingException;

public class StringUtils {
    
  static final byte[] HEX_CHAR_TABLE = {
    (byte)'0', (byte)'1', (byte)'2', (byte)'3',
    (byte)'4', (byte)'5', (byte)'6', (byte)'7',
    (byte)'8', (byte)'9', (byte)'a', (byte)'b',
    (byte)'c', (byte)'d', (byte)'e', (byte)'f'
  };    

  public static String getHexString(byte[] raw) 
  {
    String hexStr= null;
	  byte[] hex = new byte[2 * raw.length];
    int index = 0;

    for (byte b : raw) {
      int v = b & 0xFF;
      hex[index++] = HEX_CHAR_TABLE[v >>> 4];
      hex[index++] = HEX_CHAR_TABLE[v & 0xF];
    }
    try {
		hexStr = new String(hex, "ASCII");
	} catch (UnsupportedEncodingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    return hexStr;
  }
  
  public static byte [] getHexBytes(String hex) {
	  byte[] bts = new byte[hex.length() / 2];
	  for (int i = 0; i < bts.length; i++) {
		  bts[i] = (byte) Integer.parseInt(hex.substring(2*i, 2*i+2), 16);
	  }
	  return bts;
  }

  public static void main(String args[]) throws Exception{
    byte[] byteArray = {
      (byte)255, (byte)254, (byte)253, 
      (byte)252, (byte)251, (byte)250
    };
    String bla = StringUtils.getHexString(byteArray);
    System.out.println(bla);
    byte [] b = StringUtils.getHexBytes(bla);
    bla = StringUtils.getHexString(byteArray);
    System.out.println(bla);
    /*
     * output :
     *   fffefdfcfbfa
     */
    
  }
}

