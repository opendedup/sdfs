package org.opendedup.sdfs.filestore.cloud.azure;


import com.microsoft.azure.storage.table.TableServiceEntity;

public class BlobDataTracker extends TableServiceEntity {
	
	final static String PARTITION_KEY = "PartitionKey";
    final static String ROW_KEY = "RowKey";
    final static String TIMESTAMP = "Timestamp";
	
	public BlobDataTracker(long id,String storageType) {
		this.partitionKey = storageType;
		this.rowKey = Long.toString(id);
	}
	
	public  BlobDataTracker() {}
	
	
}
