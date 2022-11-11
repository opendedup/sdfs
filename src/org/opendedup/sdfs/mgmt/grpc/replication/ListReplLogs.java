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
import org.opendedup.grpc.FileInfo.errorCodes;
import org.opendedup.grpc.Storage.VolumeEvent;
import org.opendedup.grpc.Storage.VolumeEventListenRequest;
import org.opendedup.grpc.Storage.actionType;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.Main;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.mgmt.grpc.FileIOServiceImpl;
import org.opendedup.sdfs.mgmt.grpc.replication.ReplicationClient.ReplicationConnection;
import org.opendedup.sdfs.notification.ReplicationImportEvent;
import org.opendedup.sdfs.notification.SDFSEvent;

public class ListReplLogs implements Runnable {
    ReplicationClient client;
    private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
    ThreadPoolExecutor arExecutor = null;
    ReplicationConnection rc = null;
    boolean closed = false;

    public ListReplLogs(ReplicationClient client) {
        this.client = client;
    }

    public void list() throws Exception, ListCanceledException {
        rc = client.getReplicationConnection();
        try {
            SDFSLogger.getLog().info("listing replication logs");
            if (client.removed) {
                throw new ListCanceledException();
            }
            Iterator<VolumeEvent> fi = rc.getStorageBlockingStub().listReplLogs(VolumeEventListenRequest.newBuilder()
                    .setPvolumeID(this.client.volumeid).setStartSequence(this.client.getSeq()).build());
            BlockingQueue<Runnable> aworksQueue = new ArrayBlockingQueue<Runnable>(2);
            arExecutor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads + 1,
                    10, TimeUnit.SECONDS, aworksQueue, new ProcessPriorityThreadFactory(Thread.NORM_PRIORITY),
                    executionHandler);
            long seq = 0;
            while (fi.hasNext()) {
                if (client.removed) {
                    arExecutor.shutdown();
                    throw new ListCanceledException();
                }
                VolumeEvent rs = fi.next();
                if (rs.getErrorCode() != errorCodes.NOERR) {
                    SDFSLogger.getLog().warn("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                    SDFSLogger.getLog().warn("Downloading all files");
                    ReplicationImportEvent evt = new ReplicationImportEvent(".",
                            ".", client.url, client.volumeid,
                            client.mtls, false);
                    try {
                        new DownloadAll(this.client, evt).replicationSinkAll();
                    } catch (Exception e) {
                        evt.endEvent("unable to download all", SDFSEvent.ERROR, e);
                        throw new IOException(e);
                    }
                    return;
                }
                ImportFile impf = null;
                try {
                    if (!client.activeImports.contains(rs.getFile().getFilePath())) {
                        client.activeImports.add(rs.getFile().getFilePath());
                        if (rs.getActionType() == actionType.MFILEWRITTEN) {
                            ReplicationImportEvent evt = new ReplicationImportEvent(rs.getFile().getFilePath(),
                                    rs.getFile().getFilePath(),
                                    client.url, client.volumeid, client.mtls, false);
                            impf = new ImportFile(rs.getFile().getFilePath(), rs.getFile().getFilePath(), client,
                                    evt, true);

                        } else if (rs.getActionType() == actionType.MFILEDELETED) {
                            String pt = Main.volume.getPath() + File.separator + rs.getFile().getFilePath();
                            File _f = new File(pt);
                            FileIOServiceImpl.ImmuteLinuxFDFileFile(_f.getPath(), false);
                            MetaFileStore.getMF(_f).clearRetentionLock();
                            MetaFileStore.removeMetaFile(_f.getPath());
                        } else if (rs.getActionType() == actionType.MFILERENAMED) {
                            String spt = Main.volume.getPath() + File.separator + rs.getSrcfile();
                            File _sf = new File(spt);
                            String dpt = Main.volume.getPath() + File.separator + rs.getDstfile();
                            File _df = new File(dpt);

                            MetaFileStore.rename(_sf.getPath(), _df.getPath());
                        }

                    }
                    if (impf != null) {
                        arExecutor.execute(impf);
                    }
                } finally {
                    if (impf == null) {
                        client.activeImports.remove(rs.getFile().getFilePath());
                    }
                }
                seq = rs.getSeq();
            }
            arExecutor.shutdown();
            try {
                while (!arExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    SDFSLogger.getLog().debug("Awaiting replication to finish.");
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            synchronized (client.activeImports) {
                if (client.getSeq() < seq) {
                    client.setSequence(seq);
                }
            }
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
            this.list();
        } catch (ListCanceledException e) {
            SDFSLogger.getLog().warn("List Canceled.");
        } catch (Exception e) {
            SDFSLogger.getLog().warn("List Replication failed", e);
        }

    }

    private static class ListCanceledException extends Exception {

    }

}
