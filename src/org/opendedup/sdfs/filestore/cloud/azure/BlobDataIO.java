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
    
    
    public static void main(String [] args) throws InvalidKeyException, URISyntaxException, StorageException {
		String storageConnectionString = "nothing";
		CloudStorageAccount storageAccount =
		        CloudStorageAccount.parse(storageConnectionString);
		CloudTableClient tableClient = storageAccount.createCloudTableClient();

	    // Create the table if it doesn't exist.
	    String tableName = "aaadqadwe";
	    CloudTable cloudTable = tableClient.getTableReference(tableName);
	    cloudTable.createIfNotExists();
	    Random rnd =new Random();
	    BlobDataTracker tr = new BlobDataTracker(rnd.nextLong(),"archive");
	    System.out.println("1");
	    TableOperation insertCustomer1 = TableOperation.insertOrReplace(tr);
	    // Submit the operation to the table service.
	    cloudTable.execute(insertCustomer1);
	    System.out.println("2");
	    String partitionFilter = TableQuery.combineFilters(TableQuery.generateFilterCondition(
	    		PARTITION_KEY,
	            QueryComparisons.EQUAL,
	            "archive"), TableQuery.Operators.AND,TableQuery.generateFilterCondition(
	            		TIMESTAMP,
	    	            QueryComparisons.GREATER_THAN,
	    	            new Date((long)(1515118117098L))));
	    System.out.println("3");
	        // Specify a partition query, using "Smith" as the partition key filter.
	        TableQuery<BlobDataTracker> partitionQuery =
	            TableQuery.from(BlobDataTracker.class)
	            .where(partitionFilter);
	        System.out.println("4");
	        // Loop through the results, displaying information about the entity.
	        for (BlobDataTracker entity : cloudTable.execute(partitionQuery)) {
	            System.out.println(entity.getPartitionKey() +
	                "\t" + entity.getRowKey() + "\t" + entity.getTimestamp().getTime());
	        }
	        System.out.println("5");
	}
}
