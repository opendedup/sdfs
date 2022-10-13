package org.opendedup.sdfs.mgmt.grpc.replication;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import org.opendedup.collections.RocksDBMap.ProcessPriorityThreadFactory;
import org.opendedup.grpc.FileInfo.FileInfoRequest;
import org.opendedup.grpc.FileInfo.FileInfoResponse;
import org.opendedup.grpc.FileInfo.FileMessageResponse;
import org.opendedup.grpc.FileInfo.errorCodes;

import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.notification.ReplicationImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;

public class DownloadAll implements Runnable {
    ReplicationClient client;
    private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
    SDFSEvent evt;

    public DownloadAll(ReplicationClient client, SDFSEvent evt) {
        this.client = client;
        this.client.sequence = 0;
        this.evt = evt;
    }

    public void replicationSinkAll() throws IOException {
        SDFSLogger.getLog().info("downloading all files");
        Iterator<FileMessageResponse> fi = client.ioBlockingStub.getaAllFileInfo(
                FileInfoRequest.newBuilder().setPvolumeID(client.volumeid).setFileName(".").build());
        BlockingQueue<Runnable> aworksQueue = new ArrayBlockingQueue<Runnable>(2);
        ThreadPoolExecutor arExecutor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads + 1,
                10, TimeUnit.SECONDS, aworksQueue, new ProcessPriorityThreadFactory(Thread.NORM_PRIORITY),
                executionHandler);
        while (fi.hasNext()) {

            FileMessageResponse rs = fi.next();

            if (rs.getErrorCode() != errorCodes.NOERR) {
                SDFSLogger.getLog().warn("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                throw new IOException("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
            }
            FileInfoResponse file = rs.getResponseList().get(0);
            ImportFile impf = null;
            synchronized (client.activeImports) {
                if (!client.activeImports.contains(file.getFilePath())) {
                    client.activeImports.add(file.getFilePath());
                    String pt = Main.volume.getPath() + File.separator + file.getFilePath();
                    File _f = new File(pt);
                    if (_f.exists()) {
                        if (MetaFileStore.getMF(_f).lastModified() != file.getMtime()) {
                            ReplicationImportEvent evt = new ReplicationImportEvent(file.getFilePath(),
                                    file.getFilePath(),
                                    client.url, client.volumeid, client.mtls, false);
                            impf = new ImportFile(file.getFilePath(), file.getFilePath(), client, evt,true);
                        } else {
                            ReplicationImportEvent evt = new ReplicationImportEvent(file.getFilePath(),
                                    file.getFilePath(),
                                    client.url, client.volumeid, client.mtls, false);
                            evt.endEvent("File Already Exists and looks like the same " + file.getFilePath());
                        }
                    } else {
                        ReplicationImportEvent evt = new ReplicationImportEvent(file.getFilePath(),
                                file.getFilePath(),
                                client.url, client.volumeid, client.mtls, false);
                        impf = new ImportFile(file.getFilePath(), file.getFilePath(), client, evt,true);
                    }
                }
            }
            if (impf != null) {
                arExecutor.execute(impf);
            }

        }
        arExecutor.shutdown();
        try {
            while (!arExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                SDFSLogger.getLog().debug("Awaiting replication to finish.");
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        evt.endEvent("Download All Replication ended Successfully");
    }

    @Override
    public void run() {
        int retries = 0;
        for (;;) {
            try {
                replicationSinkAll();
                break;
            } catch (Exception e) {
                retries++;
                SDFSLogger.getLog().warn("Downloadall Replication failed", e);
                if (retries > 10) {
                    SDFSLogger.getLog().warn("Download all replication failed. Giving up");
                    evt.endEvent("Download all replication failed. Giving up");
                    break;
                } else {
                    try {
                        Thread.sleep(60 * 1000 * 5);
                    } catch (InterruptedException e1) {
                        break;
                    }
                }

            }
        }
        client.downloadThread = null;
    }
}
