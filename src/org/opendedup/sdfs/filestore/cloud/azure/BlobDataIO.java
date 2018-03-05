package org.opendedup.sdfs.filestore.cloud.azure;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Date;
import java.util.Random;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;

public class BlobDataIO {

	final static String PARTITION_KEY = "PartitionKey";
    final static String ROW_KEY = "RowKey";
    final static String TIMESTAMP = "Timestamp";
    String storageConnectionString = null;
    CloudTable cloudTable = null;
    CloudTableClient tableClient = null;
    public BlobDataIO(String tableName,String accessKey,String secretKey,String connectionProtocol) throws InvalidKeyException, URISyntaxException, StorageException {
    	storageConnectionString="DefaultEndpointsProtocol=" + connectionProtocol + ";" + "AccountName="
				+ accessKey + ";" + "AccountKey=" + secretKey;
    	CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);
		tableClient = storageAccount.createCloudTableClient();
		cloudTable = tableClient.getTableReference(tableName);
	    cloudTable.createIfNotExists();
    }
    
    public void updateBlobDataTracker(long id,String volid) throws StorageException {
    	BlobDataTracker tr = new BlobDataTracker(id,volid);
    	TableOperation tro = TableOperation.insertOrReplace(tr);
 	    cloudTable.execute(tro);
    }
    
    public Date getBlobDataTracker(long id,String volid) throws StorageException {
    	TableOperation tro = TableOperation.retrieve(volid, Long.toString(id), BlobDataTracker.class);
    	BlobDataTracker btr = cloudTable.execute(tro).getResultAsType();
    	return btr.getTimestamp();
    }
    
    public void removeBlobDataTracker(long id,String volid) throws StorageException {
    	TableOperation to = TableOperation.retrieve(volid, Long.toString(id), BlobDataTracker.class);
    	BlobDataTracker btr = cloudTable.execute(to).getResultAsType();
    	if(btr != null) {
    		TableOperation deleteBtr = TableOperation.delete(btr);

    	    // Submit the delete operation to the table service.
    	    cloudTable.execute(deleteBtr);
    	}
    }
    
    public  Iterable<BlobDataTracker> getBlobDataTrackers(int daysBack,String volid) {
    	long tm = ((long)daysBack * 86400000L);
    	return this.getBlobDataTrackers(tm, volid);
    }
    
    public Iterable<BlobDataTracker> getBlobDataTrackers(long msBack,String volid) {
    	long tm = System.currentTimeMillis() - msBack;
    	String partitionFilter = TableQuery.combineFilters(TableQuery.generateFilterCondition(
	    		PARTITION_KEY,
	            QueryComparisons.EQUAL,
	            volid), TableQuery.Operators.AND,TableQuery.generateFilterCondition(
	            		TIMESTAMP,
	    	            QueryComparisons.LESS_THAN,
	    	            new Date((long)(tm))));
    	TableQuery<BlobDataTracker> partitionQuery =
	            TableQuery.from(BlobDataTracker.class)
	            .where(partitionFilter);
    	 Iterable<BlobDataTracker> iter = cloudTable.execute(partitionQuery);
    	 return iter;
    }
    
    public void close() {
    	
    }
    
    
    
    
    public static void main(String [] args) throws InvalidKeyException, URISyntaxException, StorageException {
		BlobDataIO bio = new BlobDataIO("popadopa","oddsdfsn","sss","http");
		String volid="schwing";
		Random rnd = new Random();
		for(int i = 0;i<100;i++) {
			bio.updateBlobDataTracker(rnd.nextLong(), volid);
		}
		for(BlobDataTracker bt : bio.getBlobDataTrackers(1000L, volid)) {
			bio.removeBlobDataTracker(Long.parseLong(bt.getRowKey()), volid);
			System.out.println("Removed " + bt.getRowKey());
		}
		int k = 0;
		for(BlobDataTracker bt : bio.getBlobDataTrackers(1L, volid)) {
			k++;
			System.out.println("did not remove " + bt.getRowKey() + " k=" + k);
		}
		bio.removeBlobDataTracker(55, volid);
		bio.getBlobDataTracker(55, volid);
    }
}
