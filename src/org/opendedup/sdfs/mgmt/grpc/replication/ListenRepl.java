package org.opendedup.sdfs.mgmt.grpc.replication;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.WritableCacheBuffer.BlockPolicy;
import org.opendedup.sdfs.mgmt.grpc.FileIOServiceImpl;
import org.opendedup.sdfs.mgmt.grpc.replication.ReplicationClient.ReplicationConnection;
import org.opendedup.sdfs.notification.ReplicationImportEvent;

public class ListenRepl implements Runnable {
    ReplicationClient client;
    ReplicationConnection rc = null;
    private transient RejectedExecutionHandler executionHandler = new BlockPolicy();
    private Thread listThread;
    private Thread downloadThread;
    ListReplLogs ll;
    DownloadAll dl;

    public ListenRepl(ReplicationClient client) {
        this.client = client;
    }

    private static class ListenReplCanceled extends Exception {

    }

    private boolean replicateFile(VolumeEvent rs) {
        String pt = Main.volume.getPath() + File.separator + rs.getFile().getFilePath();
        File _f = new File(pt);
        if (!_f.exists()) {
            return true;
        }
        MetaDataDedupFile mf = MetaFileStore.getMF(_f);
        if (mf.lastModified() != rs.getFile().getMtime()) {
            return true;
        }
        return false;

    }

    public void listen() throws Exception, ListenReplCanceled {
        if (this.client.removed) {
            throw new ListenReplCanceled();
        }
        rc = client.getReplicationConnection();
        try {
            SDFSLogger.getLog().info("listening for new file changes");
            Iterator<VolumeEvent> fi = rc.getStorageBlockingStub()
                    .subscribeToVolume(VolumeEventListenRequest.newBuilder()
                            .setPvolumeID(this.client.volumeid).setStartSequence(this.client.getSeq()).build());
            BlockingQueue<Runnable> aworksQueue = new ArrayBlockingQueue<Runnable>(100);
            ThreadPoolExecutor arExecutor = new ThreadPoolExecutor(Main.writeThreads, Main.writeThreads + 1,
                    10, TimeUnit.SECONDS, aworksQueue, new ProcessPriorityThreadFactory(Thread.NORM_PRIORITY),
                    executionHandler);
            long seq = 0;
            while (fi.hasNext()) {
                if (this.client.removed) {
                    throw new ListenReplCanceled();
                }
                VolumeEvent rs = fi.next();

                if (rs.getErrorCode() != errorCodes.NOERR) {
                    SDFSLogger.getLog().warn("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                    throw new IOException("Sync Failed because " + rs.getErrorCode() + " msg:" + rs.getError());
                }
                ImportFile impf = null;
                synchronized (client) {
                    if (rs.getActionType() == actionType.MFILEWRITTEN && replicateFile(rs)) {
                        if (rs.getDirty()) {
                            ReplicationImportEvent evt = new ReplicationImportEvent(rs.getFile().getFilePath(),
                                    rs.getFile().getFilePath(),
                                    client.url, client.volumeid, client.mtls, false,
                                    0, 0, 0, true);
                            evt.persistEvent();

                            impf = new ImportFile(client,
                                    evt);
                        }

                    } else if (rs.getActionType() == actionType.MFILEDELETED) {

                        String pt = Main.volume.getPath() + File.separator + rs.getFile().getFilePath();
                        pt = pt.replaceFirst("\\.\\/", "");
                        pt = pt.replaceFirst("\\.\\\\", "");
                        File _f = new File(pt);
                        SDFSLogger.getLog().info("Deleting " + _f.getPath());
                        if (ReplicationClient.activeImports.containsKey(rs.getFile().getFilePath())) {
                            List<ReplicationImportEvent> al = ReplicationClient.activeImports
                                    .get(rs.getFile().getFilePath());
                            for (ReplicationImportEvent evt : al) {
                                evt.cancel();
                            }
                            boolean active = true;
                            while (active) {
                                Thread.sleep(1000);
                                active = false;
                                for (ReplicationImportEvent evt : al) {
                                    if (evt.getEndTime() <= 0) {
                                        active = true;
                                    }
                                }
                            }
                        }
                        FileIOServiceImpl.ImmuteLinuxFDFileFile(_f.getPath(), false);
                        MetaFileStore.getMF(_f).clearRetentionLock();
                        MetaFileStore.removeMetaFile(_f.getPath(), false, false, false);
                    } else if (rs.getActionType() == actionType.MFILERENAMED) {
                        String spt = Main.volume.getPath() + File.separator + rs.getSrcfile();
                        spt = spt.replaceFirst("\\.\\/", "");
                        spt = spt.replaceFirst("\\.\\\\", "");
                        File _sf = new File(spt);
                        String dpt = Main.volume.getPath() + File.separator + rs.getDstfile();
                        dpt = dpt.replaceFirst("\\.\\/", "");
                        dpt = dpt.replaceFirst("\\.\\\\", "");
                        File _df = new File(dpt);
                        if (!_sf.exists()) {

                            ReplicationImportEvent evt = new ReplicationImportEvent(rs.getFile().getFilePath(),
                                    rs.getFile().getFilePath(),
                                    client.url, client.volumeid, client.mtls, false, 0, 0, 0, true);
                            evt.persistEvent();
                            impf = new ImportFile(
                                    client, evt);
                        } else {

                            if (ReplicationClient.activeImports.containsKey(_sf.getPath())) {
                                List<ReplicationImportEvent> al = ReplicationClient.activeImports.get(_sf.getPath());
                                for (ReplicationImportEvent evt : al) {
                                    evt.cancel();
                                }
                                boolean active = true;
                                while (active) {
                                    Thread.sleep(1000);
                                    active = false;
                                    for (ReplicationImportEvent evt : al) {
                                        if (evt.getEndTime() <= 0) {
                                            active = true;
                                        }
                                    }
                                }
                            }
                            MetaFileStore.rename(_sf.getPath(), _df.getPath());
                        }
                    }
                    if (checkUpdateSeq()) {
                        seq = rs.getSeq();

                        if (client.getSeq() < seq) {
                            client.setSequence(seq);
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

            }
            synchronized (ReplicationClient.activeImports) {
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

    private boolean checkUpdateSeq() {
        if (dl == null && !listThread.isAlive()) {
            return true;
        }
        if (dl != null && dl.evt.success && dl.evt.getEndTime() > 0) {
            return true;
        }

        return false;

    }

    @Override
    public void run() {
        for (;;) {
            boolean interrupted = false;
            for (;;) {
                try {
                    client.getReplicationConnection();
                    break;
                } catch (Exception e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        interrupted = true;
                        break;
                    }
                } finally {
                    try {
                        client.closeReplicationConnection(rc);
                    } catch (Exception e) {

                    }
                }
            }
            if (interrupted) {
                break;
            }

            try {
                if (client.getSeq() > 0) {
                    if (this.ll != null) {
                        ll.close();
                        this.listThread.interrupt();
                        while (this.listThread.isAlive()) {
                            try {
                                Thread.sleep(10000);
                                SDFSLogger.getLog().info("List Thread Alive = " + this.listThread.isAlive());
                            } catch (Exception e) {
                                break;
                            }
                        }
                    }
                    this.ll = new ListReplLogs(this.client);
                    this.listThread = new Thread(ll);
                    this.listThread.start();
                } else {
                    if (this.dl != null) {
                        dl.close();
                        this.downloadThread.interrupt();
                        while (this.downloadThread.isAlive()) {
                            try {
                                Thread.sleep(10000);
                                SDFSLogger.getLog().info("List Thread Alive = " + this.downloadThread.isAlive());
                            } catch (Exception e) {
                                break;
                            }
                        }
                    }
                    ReplicationImportEvent evt = new ReplicationImportEvent(".",
                            ".", this.client.url, this.client.volumeid,
                            this.client.mtls, false, 0, 0, 0, true);
                    dl = new DownloadAll(this.client, evt);
                    downloadThread = new Thread(dl);
                    downloadThread.start();
                }
                this.listen();
            } catch (ListenReplCanceled e) {
                SDFSLogger.getLog().warn("ListenRepl Replication canceled");
                break;
            } catch (io.grpc.StatusRuntimeException e) {
                if (e.getCause() instanceof java.lang.InterruptedException) {
                    SDFSLogger.getLog().warn("ListenRepl Replication canceled");
                    break;
                } else {
                    try {
                        SDFSLogger.getLog().warn("ListenRepl Replication disconnected," +
                                " will try to reconnect in 1 minute ", e);
                        Thread.sleep(60 * 1000 * 1);
                    } catch (Exception e1) {
                        break;

                    }
                }
            } catch (Exception e) {
                SDFSLogger.getLog().warn("ListenRepl Replication disconnected," +
                        " will try to reconnect in 1 minute", e);
                try {
                    Thread.sleep(60 * 1000 * 1);
                } catch (Exception e1) {
                    break;

                }

            } finally {
                try {
                    client.closeReplicationConnection(rc);
                } catch (Exception e) {

                }
            }
        }
    }
}
