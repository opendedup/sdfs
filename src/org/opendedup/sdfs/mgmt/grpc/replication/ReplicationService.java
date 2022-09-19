package org.opendedup.sdfs.mgmt.grpc.replication;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Longs;

import org.opendedup.grpc.Storage.VolumeEvent;
import org.opendedup.grpc.Storage.actionType;
import org.opendedup.logging.SDFSLogger;
import org.opendedup.sdfs.filestore.MetaFileStore;
import org.opendedup.sdfs.filestore.cloud.FileReplicationService;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.io.events.ReplEvent;
import org.opendedup.util.RandomGUID;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionOptionsFIFO;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class ReplicationService {
    private static final long MB = 1024 * 1024;
    private static RocksDB repldb = null;
    private EventBus eventBus = new EventBus();
    private AtomicLong sequence = null;
    private int pl = 0;

    public ReplicationService(Volume vol) throws Exception {
        File directory = new File(vol.getReplPath() + File.separator);
        pl = vol.getPath().length();
        directory.mkdirs();
        RocksDB.loadLibrary();
        CompactionOptionsFIFO fifo = new CompactionOptionsFIFO();
        fifo.setMaxTableFilesSize(500 * MB);
        DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        Env env = Env.getDefault();
        options.setEnv(env);
        ColumnFamilyOptions familyOptions = new ColumnFamilyOptions();
        familyOptions.setCompactionOptionsFIFO(fifo);
        ColumnFamilyDescriptor evtArD = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, familyOptions);
        ArrayList<ColumnFamilyDescriptor> descriptors = new ArrayList<ColumnFamilyDescriptor>();
        descriptors.add(evtArD);
        ArrayList<ColumnFamilyHandle> handles = new ArrayList<ColumnFamilyHandle>();
        repldb = RocksDB.open(options, directory.getPath(), descriptors, handles);
        FileReplicationService.registerEvents(this);
        MetaDataDedupFile.registerListener(this);
        MetaFileStore.registerListener(this);
        RocksIterator iter = repldb.newIterator();
        iter.seekToLast();
        if (!iter.isValid()) {
            // no key to delete
            sequence = new AtomicLong(0);
        } else {
            sequence = new AtomicLong(Longs.fromByteArray(iter.key()));
        }
        SDFSLogger.getLog().info("Initialized Replication Service last sequence was " + this.sequence.get());
    }

    public void registerListener(Object obj) {
        eventBus.register(obj);
    }

    public void unregisterListener(Object obj) {
        try {
            eventBus.unregister(obj);
        } catch (Exception e) {
            SDFSLogger.getLog().debug("unable to unregister listener", e);
        }

    }

    public RocksIterator getIterator(long startSequence) throws IOException {

        RocksIterator iter = repldb.newIterator();
        iter.seek(Longs.toByteArray(startSequence));

        if (!iter.isValid()) {
            throw new IOException("no sequence at " + startSequence);
        }
        return iter;
    }

    @Subscribe
    public void syncEvent(org.opendedup.sdfs.io.events.MFileDownloaded _evt) {
        VolumeEvent.Builder b = VolumeEvent.newBuilder();
        b.setSeq(this.sequence.incrementAndGet()).setUuid(RandomGUID.getGuid())
        .setTimeStamp(System.currentTimeMillis()).setActionType(actionType.MFILEWRITTEN);
        try {
            synchronized (_evt.mf) {
               b.setFile(_evt.mf.toGRPC(false));

            }
        } catch (Exception e) {
            SDFSLogger.getLog().warn("unable to serialize message",e);
        }
    }

    @Subscribe
    public void syncEvent(org.opendedup.sdfs.io.events.MFileWritten _evt) {
        VolumeEvent.Builder b = VolumeEvent.newBuilder();
        b.setSeq(this.sequence.incrementAndGet()).setUuid(RandomGUID.getGuid())
        .setTimeStamp(System.currentTimeMillis()).setActionType(actionType.MFILEWRITTEN);
        try {
            synchronized (_evt.mf) {
               b.setFile(_evt.mf.toGRPC(false));

            }
            persistVolumeEvent(b);
        } catch (Exception e) {
            SDFSLogger.getLog().warn("unable to serialize message",e);
        }
    }

    @Subscribe
    public void syncEvent(org.opendedup.sdfs.io.events.MFileDeleted _evt) {
        VolumeEvent.Builder b = VolumeEvent.newBuilder();
        b.setSeq(this.sequence.incrementAndGet()).setUuid(RandomGUID.getGuid())
        .setTimeStamp(System.currentTimeMillis()).setActionType(actionType.MFILEDELETED);
        try {
            synchronized (_evt.mf) {
               b.setFile(_evt.mf.toGRPC(false));

            }
            persistVolumeEvent(b);
        } catch (Exception e) {
            SDFSLogger.getLog().warn("unable to serialize message",e);
        }
        
    }

    @Subscribe
    public void syncEvent(org.opendedup.sdfs.io.events.MFileRenamed _evt) {
        VolumeEvent.Builder b = VolumeEvent.newBuilder();
        b.setSeq(this.sequence.incrementAndGet()).setUuid(RandomGUID.getGuid())
        .setTimeStamp(System.currentTimeMillis()).setActionType(actionType.MFILERENAMED);
        try {
            synchronized (_evt.mf) {
               b.setFile(_evt.mf.toGRPC(false)).setSrcfile(_evt.from.substring(pl))
               .setDstfile(_evt.to.substring(pl));
            }
            persistVolumeEvent(b);
        } catch (Exception e) {
            SDFSLogger.getLog().warn("unable to serialize message",e);
        }
    }

    private void persistVolumeEvent(VolumeEvent.Builder b) throws RocksDBException {
        VolumeEvent ve = b.build();
        repldb.put(Longs.toByteArray(ve.getSeq()), ve.toByteArray());
        eventBus.post(new ReplEvent(ve));
    }

}
