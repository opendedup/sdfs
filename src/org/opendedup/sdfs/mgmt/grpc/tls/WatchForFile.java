package org.opendedup.sdfs.mgmt.grpc.tls;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;

import org.apache.log4j.Logger;
import org.opendedup.logging.SDFSLogger;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;

public class WatchForFile implements Runnable {
    DynamicTrustManager tm = null;
    static SslContextBuilder ssl = null;

    public WatchForFile(DynamicTrustManager tm) {
        this.tm = tm;

    }

    public void run()
    {
    	Logger logger = SDFSLogger.getLog();
        Path myDir= tm.trustedCertificatesDir.toPath();
          try 
          {
              Boolean isFolder = (Boolean) Files.getAttribute(myDir,"basic:isDirectory", NOFOLLOW_LINKS);
              if (!isFolder)
              {
                  throw new IllegalArgumentException("Path: " + myDir + " is not a folder");
              }
          }
          catch (IOException ioe)
          {
              ioe.printStackTrace();
          }

          logger.info("Watching path: " + myDir);

        try {
           WatchService watcher = myDir.getFileSystem().newWatchService();
           myDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE,StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);

           while (true) {
        	   WatchKey watckKey = watcher.take();

               List<WatchEvent<?>> events = watckKey.pollEvents();

               for (WatchEvent<?> event : events) {
                    if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                    	logger.info("Created: " + event.kind().toString());
                    	tm.loadTrustManager(ClientAuth.REQUIRE);
                    	//ssl.trustManager(tm).build();
                    }
                    if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
                    	logger.info("Delete: " + event.context().toString());
                    	tm.loadTrustManager(ClientAuth.REQUIRE);
                    	//ssl.trustManager(tm).build();
                    }
                    if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                    	logger.info("Modify: " + event.context().toString());
                    	tm.loadTrustManager(ClientAuth.REQUIRE);
                    	//ssl.trustManager(tm).build();
                    }
                }
               
               if (!watckKey.reset()) {
                   break; // loop
               }
           }
        }
        catch (Exception e) 
        {
        	logger.info("Error: " + e.toString());
        }
    }
}
