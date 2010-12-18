package org.opendedup.collections;

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


    int loops = 20;
    // run settings
    long start = 0;
    long end = 0;
    long time = 0;
    float tPer = 0;

    long putTotalJCS = 0;
    long getTotalJCS = 0;

    String jcsDisplayName = "JCS";

    try
    {
        for ( int j = 0; j < loops; j++ )
        {

            jcsDisplayName = "JCS      ";
            start = System.currentTimeMillis();
            for ( int i = 0; i < tries; i++ )
            {
                jcs.put( "key:" + i, "data" + i );
            }
            end = System.currentTimeMillis();
            time = end - start;
            putTotalJCS += time;
            tPer = Float.intBitsToFloat( (int) time ) / Float.intBitsToFloat( tries );
            System.out
                .println( jcsDisplayName + " put time for " + tries + " = " + time + "; millis per = " + tPer );

            start = System.currentTimeMillis();
            for ( int i = 0; i < tries; i++ )
            {
                jcs.getCacheElement( "key:" + i );
            }
            end = System.currentTimeMillis();
            time = end - start;
            getTotalJCS += time;
            tPer = Float.intBitsToFloat( (int) time ) / Float.intBitsToFloat( tries );
            System.out
                .println( jcsDisplayName + " get time for " + tries + " = " + time + "; millis per = " + tPer );

           

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

    System.out.println( "Finished " + loops + " loops of " + tries + " gets and puts" );

    System.out.println( "\n" );
    System.out.println( "Put average for " + jcsDisplayName + "  = " + putAvJCS );
    

    System.out.println( "\n" );
    System.out.println( "Get average for  " + jcsDisplayName + "  = " + getAvJCS );
    

}
	
	public static void main(String [] args) throws Exception {
		JCSCache cache = new JCSCache(1000000, "test");
		cache.doTest(500);
	}

}
