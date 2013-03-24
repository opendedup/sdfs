package org.opendedup.sdfs.cluster;

import org.jgroups.JChannel;

import org.jgroups.blocks.MessageDispatcher;
import org.opendedup.sdfs.io.MetaDataDedupFile;
import org.opendedup.sdfs.io.MetaFileEventListener;
import org.opendedup.sdfs.io.Volume;
import org.opendedup.sdfs.notification.SDFSEvent;

public class JgroupsMetaFileEventSource implements MetaFileEventListener {
	
	private JChannel ch = null;
	private Volume vol = null;
	MessageDispatcher  disp;
	
	public JgroupsMetaFileEventSource(Volume vol) throws Exception {
		this.vol = vol;
		ch=new JChannel("/home/samsilverberg/workspace_sdfs/sdfs/src/jgroups.cfg.xml"); 
		ch.setDiscardOwnMessages(true);
        ch.connect("metafile-" +this.vol.getUuid());
        //disp=new MessageDispatcher(ch, null, null, this);
	}

	@Override
	public void onDedupChange(boolean dedup, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onDedupFileChange(String guid, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onExecutableChange(boolean executable, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onExecutableChange(boolean executable, boolean ownerOnly,
			MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onWritabeChange(boolean writable, boolean ownerOnly,
			MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onGroupChange(int grp, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onHiddenChange(boolean hidden, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onLastAccessedChange(long tm, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onLastModifiedChange(long tm, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onLengthChange(long length, boolean persist,
			MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onModeChange(int mode, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onOwnerChange(int owner, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onPermissionsChange(int permissions, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onReadableChange(boolean readable, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onReadableChange(boolean readable, boolean ownerOnly,
			MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onReadOnly(MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onSymlinkChange(boolean isSymlink, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onSymlinkPathChange(String path, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onTimeStampChange(long timestamp, boolean persist,
			MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onSetVMDK(boolean isVMDK, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onWritableChange(String writable, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onSnapshotExecuted(String snaptoPath, boolean overwrite,
			SDFSEvent evt, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onSync(MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onDelete(MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onRenameTo(String dst) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMkDir(MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMkDirs(MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onDfGuidChange(String guid, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onAddXAttribute(String name, String value, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onCopyTo(String npath, boolean overwrite, MetaDataDedupFile mf) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onCreate(MetaDataDedupFile mf) {
		// TODO Auto-generated method stub
		
	}

}
