package org.opendedup.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;

public class ProcessWorker {

	public static int runProcess(String[] pstr, int timeout)
			throws TimeoutException, InterruptedException, IOException {
		
		String cmdStr = "";
		for (String st : pstr) {
			cmdStr = cmdStr + " " + st;
		}
		SDFSLogger.getLog().debug("Executing [" + cmdStr + "]");
		Process p = null;
		try {
			p = Runtime.getRuntime().exec(pstr, null, new File(Main.volume.getPath()));
			ReadStream s1 = new ReadStream("stdin", p.getInputStream ());
			ReadStream s2 = new ReadStream("stderr", p.getErrorStream ());
			s1.start ();
			s2.start ();
		}catch(Throwable e) {
			SDFSLogger.getLog().error("unable to execute " + cmdStr,e);
			throw new IOException(e);
		}
		long now = System.currentTimeMillis();
	    long finish = now + timeout;
	    while ( isAlive( p ) && ( System.currentTimeMillis() < finish ) )
	    {
	        Thread.sleep( 10 );
	    }
	    if ( isAlive( p ) )
	    {
	        throw new TimeoutException( "Process timeout out after " + timeout + " ms" );
	    }
	    return p.exitValue();
	}
	
	public static int runProcess(String pstr)
			throws TimeoutException, InterruptedException, IOException {
		
		
		SDFSLogger.getLog().debug("Executing [" + pstr + "]");
		Process p = null;
		try {
			p = Runtime.getRuntime().exec(pstr, null, new File(Main.volume.getPath()));
			ReadStream s1 = new ReadStream("stdin", p.getInputStream ());
			ReadStream s2 = new ReadStream("stderr", p.getErrorStream ());
			s1.start ();
			s2.start ();
		}catch(Throwable e) {
			SDFSLogger.getLog().error("unable to execute " + pstr,e);
			throw new IOException(e);
		}
		p.waitFor();
	    return p.exitValue();
	}
	
	public static boolean isAlive( Process p ) {
	    try
	    {	
	        p.exitValue();
	        return false;
	    } catch (IllegalThreadStateException e) {
	        return true;
	    }
	}
	
	private static class ReadStream implements Runnable {
	    String name;
	    InputStream is;
	    Thread thread;      
	    public ReadStream(String name, InputStream is) {
	        this.name = name;
	        this.is = is;
	    }       
	    public void start () {
	        thread = new Thread (this);
	        thread.start ();
	    }       
	    public void run () {
	        try {
	            InputStreamReader isr = new InputStreamReader (is);
	            BufferedReader br = new BufferedReader (isr);   
	            while (true) {
	                String s = br.readLine ();
	                if (s == null) break;
	                SDFSLogger.getLog().info("[" + name + "] " + s);
	            }
	            is.close ();    
	        } catch (Exception ex) {
	        	SDFSLogger.getLog().debug("Problem reading stream " + name + "... :" , ex);
	        }
	    }
	}
	
	

	

}
