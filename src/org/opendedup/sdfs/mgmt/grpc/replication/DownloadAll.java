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
import org.opendedup.sdfs.mgmt.grpc.replication.ReplicationClient.ReplicationConnection;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.notification.ReplicationImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;

public class DownloadAll implements Runnable {
    ReplicationClient client;
    private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
    SDFSEvent evt;
    ThreadPoolExecutor arExecutor;
    ReplicationConnection rc = null;
    boolean closed = false;

    public DownloadAll(ReplicationClient client, SDFSEvent evt) {
        this.client = client;
        this.client.setSequence(0);
        this.evt = evt;
    }

    public void replicationSinkAll() throws Exception, DownloadCanceledException {
        rc = client.getReplicationConnection();
        try {
            if (client.removed) {
                throw new DownloadCanceledException();
            }
            SDFSLogger.getLog().info("downloading all files");
            Iterator<FileMessageResponse> fi = rc.getIoBlockingStub().getaAllFileInfo(
                    FileInfoRequest.newBuilder().setPvolumeID(client.volumeid).setFileName(".").build());
            BlockingQueue<Runnable> aworksQueue = new ArrayBlockingQueue<Runnable>(2);
            arExecutor = new ThreadPoolExecutor(1, 3,
                    10, TimeUnit.SECONDS, aworksQueue, new ProcessPriorityThreadFactory(Thread.NORM_PRIORITY),
                    executionHandler);
            while (fi.hasNext()) {
                if (client.removed) {
                    if (this.arExecutor != null) {
                        arExecutor.shutdown();
                    }
                    throw new DownloadCanceledException();

                }

                FileMessageResponse rs = fi.next();

                if (rs.getErrorCode() != errorCodes.NOERR) {
                    SDFSLogger.getLog().warn("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                    throw new IOException("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                }
                FileInfoResponse file = rs.getResponseList().get(0);
                ImportFile impf = null;
                String pt = Main.volume.getPath() + File.separator + file.getFilePath();
                File _f = new File(pt);
                if (_f.exists()) {
                    if (MetaFileStore.getMF(_f).lastModified() != file.getMtime()) {
                        ReplicationImportEvent evt = new ReplicationImportEvent(file.getFilePath(),
                                file.getFilePath(),
                                client.url, client.volumeid, client.mtls, false,0,0,0,true,false);
                        evt.persistEvent();
                        impf = new ImportFile(client, evt);
                    } else {
                        ReplicationImportEvent evt = new ReplicationImportEvent(file.getFilePath(),
                                file.getFilePath(),
                                client.url, client.volumeid, client.mtls, false,0,0,0,true,false);
                        evt.endEvent("File Already Exists and looks like the same " + file.getFilePath());
                    }
                } else {
                    ReplicationImportEvent evt = new ReplicationImportEvent(file.getFilePath(),
                            file.getFilePath(),
                            client.url, client.volumeid, client.mtls, false,0,0,0,true,false);
                    evt.persistEvent();
                    impf = new ImportFile(client, evt);
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
        } finally {
            try {
                client.closeReplicationConnection(rc);
            } catch (Exception e) {

            }
        }
    }

    public void close() {
        this.closed = true;
    }

    @Override
    public void run() {
        try {
            replicationSinkAll();
        } catch (DownloadCanceledException e) {
            SDFSLogger.getLog().warn("Download canceled");
            evt.endEvent("Download canceled", SDFSEvent.WARN);
        } catch (Exception e) {
            SDFSLogger.getLog().warn("Downloadall Replication failed", e);
            evt.endEvent("Download canceled", SDFSEvent.WARN);

        }

    }

    private static class DownloadCanceledException extends Exception {

    }
}
