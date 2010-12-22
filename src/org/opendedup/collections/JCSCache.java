package org.opendedup.collections;

import java.util.LinkedHashMap;

import java.util.Map;


import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.CompositeCacheAttributes;
import org.apache.jcs.engine.ElementAttributes;
import org.apache.jcs.engine.behavior.ICompositeCacheAttributes;
import org.apache.jcs.engine.control.event.behavior.IElementEvent;
import org.apache.jcs.engine.control.event.behavior.IElementEventHandler;


public class JCSCache implements IElementEventHandler{
	private int maxSize = 1024;
	private JCS jcs = null;
	public JCSCache(int maxSize,String uuid)  throws CacheException {
		this.maxSize = maxSize;
		ICompositeCacheAttributes cattr = new CompositeCacheAttributes();
        cattr.setMaxObjects( maxSize );
        cattr.setUseDisk(false);
        
        ElementAttributes attr = new ElementAttributes();
        attr.setIsEternal(false);
        attr.setIsRemote(false);
        attr.setIsSpool(false);
        
        jcs = JCS.getInstance( uuid, cattr );
        jcs.setDefaultElementAttributes(attr);
	}
	
	public int getMaxSize () {
		return this.maxSize;
	}

	@Override
	public void handleElementEvent(IElementEvent arg0) {
		System.out.println("Event occured " + arg0.getElementEvent());
		
	}
	
	public void put(Object key,Object value) throws CacheException {
		this.jcs.put(key, value);
	}
	
	public Object get(Object key) {
		return this.jcs.get(key);
	}
	
	public void remove(Object key) throws CacheException {
		this.jcs.remove(key);
	}
	
	public boolean containsKey(Object key) {
		return(this.jcs.get(key) != null);
	}
	
	public void doTest(int tries)
    throws Exception
{
		/** jcs / hashtable */
	    float ratioPut = 0;

	    /** jcs / hashtable */
	    float ratioGet = 0;

	    /** ration goal */
	    float target = 3.50f;

    int loops = 20;
    // run settings
    long start = 0;
    long end = 0;
    long time = 0;
    float tPer = 0;

    long putTotalJCS = 0;
    long getTotalJCS = 0;
    long putTotalHashtable = 0;
    long getTotalHashtable = 0;

    try
    {

        //JCS.setConfigFilename( "/cache.ccf" );

        for ( int j = 0; j < loops; j++ )
        {

            String name = "JCS      ";
            

            // /////////////////////////////////////////////////////////////
            name = "Hashtable";
            @SuppressWarnings("serial")
			LinkedHashMap<String,String> cache2 = new LinkedHashMap<String,String>(tries/2, 1, true){
        		protected boolean removeEldestEntry(
        				Map.Entry<String,String> eldest) {
        			System.out.println("called");
        			return false;
        		}
        	};
            start = System.currentTimeMillis();
            
            for ( int i = 0; i < tries; i++ )
            {
                cache2.put( "key:" + i, "data:"+i);
            }
            end = System.currentTimeMillis();
            time = end - start;
            putTotalHashtable += time;
            tPer = Float.intBitsToFloat( (int) time ) / Float.intBitsToFloat( tries );
            System.out.println( name + " put time for " + tries + " = " + time + "; millis per = " + tPer );

            start = System.currentTimeMillis();
            for ( int i = 0; i < tries; i++ )
            {
                cache2.get( "key:" + i );
            }
            end = System.currentTimeMillis();
            time = end - start;
            getTotalHashtable += time;
            tPer = Float.intBitsToFloat( (int) time ) / Float.intBitsToFloat( tries );
            System.out.println( name + " get time for " + tries + " = " + time + "; millis per = " + tPer );

            System.out.println( "\n" );
        }

    }
    catch ( Exception e )
    {
        e.printStackTrace( System.out );
        System.out.println( e );
    }

    long putAvJCS = putTotalJCS / loops;
    long getAvJCS = getTotalJCS / loops;
    long putAvHashtable = putTotalHashtable / loops;
    long getAvHashtable = getTotalHashtable / loops;

    System.out.println( "Finished " + loops + " loops of " + tries + " gets and puts" );

    System.out.println( "\n" );
    System.out.println( "Put average for JCS       = " + putAvJCS );
    System.out.println( "Put average for Hashtable = " + putAvHashtable );
    ratioPut = Float.intBitsToFloat( (int) putAvJCS ) / Float.intBitsToFloat( (int) putAvHashtable );
    System.out.println( "JCS puts took " + ratioPut + " times the Hashtable, the goal is <" + target + "x" );

    System.out.println( "\n" );
    System.out.println( "Get average for JCS       = " + getAvJCS );
    System.out.println( "Get average for Hashtable = " + getAvHashtable );
    ratioGet = Float.intBitsToFloat( (int) getAvJCS ) / Float.intBitsToFloat( (int) getAvHashtable );
    System.out.println( "JCS gets took " + ratioGet + " times the Hashtable, the goal is <" + target + "x" );

}
	
	public static void main(String [] args) throws Exception {
		JCSCache cache = new JCSCache(1000000, "test");
		cache.doTest(50000);
	}

}
