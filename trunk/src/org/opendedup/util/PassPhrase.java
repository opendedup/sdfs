package org.opendedup.util;

import java.security.SecureRandom;

/*
 * Copyright (c) Ian F. Darwin, http://www.darwinsys.com/, 1996-2002.
 * All rights reserved. Software written by Ian F. Darwin and others.
 * $Id: LICENSE,v 1.8 2004/02/09 03:33:38 ian Exp $
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS''
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * Java, the Duke mascot, and all variants of Sun's Java "steaming coffee
 * cup" logo are trademarks of Sun Microsystems. Sun's, and James Gosling's,
 * pioneering role in inventing and promulgating (and standardizing) the Java 
 * language and environment is gratefully acknowledged.
 * 
 * The pioneering role of Dennis Ritchie and Bjarne Stroustrup, of AT&T, for
 * inventing predecessor languages C and C++ is also gratefully acknowledged.
 */

/**
 * Cheap, lightweight, low-security password generator. See also:
 * java.security.*;
 */
public class PassPhrase {
	/** Minimum length for a decent password */
	public static final int MIN_LENGTH = 18;

	/** The random number generator. */
	protected static java.security.SecureRandom r = new java.security.SecureRandom();

	/*
	 * Set of characters that is valid. Must be printable, memorable, and "won't
	 * break HTML" (i.e., not ' <', '>', '&', '=', ...). or break shell commands
	 * (i.e., not ' <', '>', '$', '!', ...). I, L and O are good to leave out,
	 * as are numeric zero and one.
	 */
	protected static char[] goodChar = { 'a', 'b', 'c', 'd', 'e', 'f', 'g',
			'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
			'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K',
			'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
			'2', '3', '4', '5', '6', '7', '8', '9', '+', '-', '_', '1', 'l',
			'=' };

	/* Generate a Password object with a random password. */
	public static String getNext() {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < MIN_LENGTH; i++) {
			sb.append(goodChar[r.nextInt(goodChar.length)]);
		}
		return sb.toString();
	}
	
	public static String getIV() {
		SecureRandom rnd = new SecureRandom();
		byte [] b = new byte[16];
		rnd.nextBytes(b);
		return StringUtils.getHexString(b);
	}

	public static void main(String[] argv) {
		for (int i = 0; i < 20; i++) {
			System.out.println(PassPhrase.getNext());
		}
	}
}
