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
import org.opendedup.sdfs.notification.ReplicationImportEvent;

public class ListenRepl implements Runnable {
    ReplicationClient client;
    private transient RejectedExecutionHandler executionHandler = new BlockPolicy();

    public ListenRepl(ReplicationClient client) {
        this.client = client;
    }

    private static class ListenReplCanceled extends Exception {

    }

    public void listen() throws IOException, ListenReplCanceled {
        if(this.client.removed) {
            throw new ListenReplCanceled();
        }
        SDFSLogger.getLog().info("listening for new file changes");
        Iterator<VolumeEvent> fi = client.storageBlockingStub
                .subscribeToVolume(VolumeEventListenRequest.newBuilder()
                        .setPvolumeID(this.client.volumeid).setStartSequence(this.client.sequence).build());
        BlockingQueue<Runnable> aworksQueue = new ArrayBlockingQueue<Runnable>(100);
        ThreadPoolExecutor arExecutor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads + 1,
                10, TimeUnit.SECONDS, aworksQueue, new ProcessPriorityThreadFactory(Thread.NORM_PRIORITY),
                executionHandler);
        long seq = 0;
        while (fi.hasNext()) {
            if(this.client.removed) {
                throw new ListenReplCanceled();
            }
            VolumeEvent rs = fi.next();

            if (rs.getErrorCode() != errorCodes.NOERR) {
                SDFSLogger.getLog().warn("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                throw new IOException("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
            }
            ImportFile impf = null;
            synchronized (client.activeImports) {
                try {
                    if (!client.activeImports.contains(rs.getFile().getFilePath())) {
                        client.activeImports.add(rs.getFile().getFilePath());
                        if (rs.getActionType() == actionType.MFILEWRITTEN) {
                            ReplicationImportEvent evt = new ReplicationImportEvent(rs.getFile().getFilePath(),
                                    rs.getFile().getFilePath(),
                                    client.url, client.volumeid, client.mtls, false);
                            impf = new ImportFile(rs.getFile().getFilePath(), rs.getFile().getFilePath(), client,
                                    evt,true);

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
                        seq = rs.getSeq();
                        SDFSLogger.getLog().info("Sequence = " + seq);
                        if (client.sequence < seq) {
                            client.sequence = seq;
                            SDFSLogger.getLog().info("Client Sequence = " + client.sequence);
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
        synchronized (client.activeImports) {
            if (client.sequence < seq) {
                client.sequence = seq;
            }
        }
    }

    @Override
    public void run() {
        for (;;) {
            try {
                this.listen();
                break;
            } catch(ListenReplCanceled e)  {
                SDFSLogger.getLog().warn("ListenRepl Replication canceled");
                break;
            }
            catch (Exception e) {
                SDFSLogger.getLog().warn("ListenRepl Replication failed", e);
                try {
                    while (!client.isConnected()) {
                        try {
                            client.connect();
                            client.listThread = new Thread(new ListReplLogs(this.client));
                            client.listThread.start();
                        } catch (Exception e1) {
                            SDFSLogger.getLog().warn("Connecting to Source Failed", e1);
                            Thread.sleep(60 * 1000 * 5);
                        }
                    }
                } catch (InterruptedException e1) {
                    break;
                }

            }
        }
    }
}
