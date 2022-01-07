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

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;

import org.opendedup.logging.SDFSLogger;

public class WinRegistry {

    public static final int KEY_SZ = 0;
    public static final int KEY_DWORD = 1;
    private static final String REG_QUERY = "reg query ";
    private static final String REG_STR = "REG_SZ";
    private static final String REG_DWORD = "REG_DWORD";

    static String readRegistryRegQuery(String key) throws IOException {
        String useKey = REG_QUERY + key;
        int keyType = -1;
        // Run reg query, then read output with StreamReader (internal class)
        Process process = Runtime.getRuntime().exec(useKey);
        StreamReader reader = new StreamReader(process.getInputStream());

        reader.start();
        try {
            process.waitFor();
            reader.join();
        } catch (InterruptedException e) {
            SDFSLogger.getLog().error(e.getMessage());
            Thread.currentThread().interrupt();
        }

        // Parse out the value

        String result = reader.getResult();
        int p = -1;
        if (result.contains(REG_STR)) {
            p = result.indexOf(REG_STR);
            keyType = KEY_SZ;
        } else if (result.contains(REG_DWORD)) {
            p = result.indexOf(REG_DWORD);
            keyType = KEY_DWORD;
        }
        if (p == -1) {
            return null;
        }

        switch (keyType) {
            case KEY_SZ:
                return result.substring(p + REG_STR.length()).trim();
            case KEY_DWORD:
                String temp = result.substring(p + REG_DWORD.length()).trim();
                return Integer.toString((Integer.parseInt(temp.substring("0x".length()), 16)));
            default:
                return "";
        }

    }

    static class StreamReader extends Thread {

        private final InputStream mIs;
        private final StringWriter mSw;

        public StreamReader(InputStream is) {
            this.mIs = is;
            mSw = new StringWriter();
        }

        public void run() {
            try {
                int c;
                while ((c = mIs.read()) != -1)
                    mSw.write(c);
            } catch (IOException e) {
                SDFSLogger.getLog().error(e.getMessage());
            }
        }

        public String getResult() {
            return mSw.toString();
        }
    }

    public static String readRegistry(String location, String key) throws IOException {
        String pathkey = "\"" +
                location +
                "\" /v " +
                key;
        return readRegistryRegQuery(pathkey);
    }

    public static void main(String[] args) throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        // System.out.println(WinRegistry.readString(HKEY_LOCAL_MACHINE,
        // "SOFTWARE\\Wow6432Node\\SDFS", "path"));
        System.out.println("Running Winregistry class");
    }

}
