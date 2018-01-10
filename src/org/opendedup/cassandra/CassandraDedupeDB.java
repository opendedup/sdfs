package org.opendedup.cassandra;



import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import org.opendedup.logging.SDFSLogger;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Apache Ignite usage on Kubernetes requires that each node (Pod in the
 * Kubernetes lingo) is able to access other pods directly. Proxies will not
 * work.
 * <p>
 * In order to find the desired pods IP addresses, one could query the
 * Kubernetes API for endpoints of a Service but that may prove too much of a
 * hurdle to maintain. Since DNS integration is a common best-practice when
 * assembling Kubernetes clusters, we will rely on SRV lookups.
 */
public class CassandraDedupeDB {

	InetSocketAddress[] contactPoints = { new InetSocketAddress("127.0.0.1",9042)};
	private Cluster cluster = null;
	private Session session = null;
	private String dataCenter ="datacenter1";
	private int replicationFactor = 3;
	BoundStatement hdbStatement = null;
	BoundStatement hdbStatementUpdate = null;
	BoundStatement refdbStatement = null;
	BoundStatement getHdbStatement = null;
	BoundStatement insertRefHashes = null;
	BoundStatement getRefHashes = null;
	BoundStatement delRef = null;
	BoundStatement delRefHdb = null;
	BoundStatement delHdb = null;
	
	private String serviceName = "opendedupe";

	public CassandraDedupeDB(InetSocketAddress[] contactPoints,String dataCenter,String serviceName,int replicationFactor) {
		this.contactPoints = contactPoints;
		this.dataCenter = dataCenter;
		this.serviceName = serviceName;
		this.createTableSpace();
		String writeHdb = "INSERT INTO "+serviceName +".hashdb (key, archive) VALUES (?,?) IF NOT EXISTS;";
		String writeRefHdb = "INSERT INTO "+serviceName +".refhashesdb (archive, hashes) VALUES (?,?)";
		String updateHdb = "UPDATE "+serviceName +".hashdb SET archive = ? WHERE key = ?";
		String updateRef = "UPDATE "+serviceName +".refdb SET ref = ref + ? WHERE archive = ?;";
		String getHdb = "Select archive from " + serviceName+".hashdb WHERE key = ?";
		String getRefHdb = "Select hashes from " + serviceName + ".refhashesdb WHERE archive = ?";
		String _deleteHash = "DELETE from " + serviceName + ".hashdb WHERE key = ?";
		String _deleteRef = "DELETE from " + serviceName + ".refdb WHERE archive = ?";
		String _deleteRefHdb = "DELETE from " + serviceName + ".refhashesdb WHERE archive = ?";
		hdbStatement = session.prepare(writeHdb).bind();
		refdbStatement = session.prepare(updateRef).bind();
		hdbStatementUpdate = session.prepare(updateHdb).bind();
		this.getHdbStatement = session.prepare(getHdb).bind();
		this.insertRefHashes = session.prepare(writeRefHdb).bind();
		this.getRefHashes = session.prepare(getRefHdb).bind();
		this.delHdb = session.prepare(_deleteHash).bind();
		this.delRef = session.prepare(_deleteRef).bind();
		this.delRefHdb = session.prepare(_deleteRefHdb).bind();
		this.replicationFactor = replicationFactor;
	}
	
	private void createTableSpace() {
		if (cluster == null) {
			cluster = Cluster.builder().addContactPointsWithPorts(contactPoints).build();

			SDFSLogger.getLog().info("Connected Cassandra to cluster: " + cluster.getMetadata().getClusterName());

			session = cluster.connect();
		}
		session.execute("CREATE KEYSPACE IF NOT EXISTS " + serviceName + " WITH replication "
				+ "= {'class':'NetworkTopologyStrategy', '"+this.dataCenter+"': "+this.replicationFactor+"};");
		session.execute("CREATE TABLE IF NOT EXISTS " + serviceName + ".hashdb (key blob PRIMARY KEY, archive bigint);");
		session.execute("CREATE TABLE IF NOT EXISTS " + serviceName + ".refdb (archive bigint PRIMARY KEY, ref counter);");
		session.execute("CREATE TABLE IF NOT EXISTS " + serviceName + ".refhashesdb (archive bigint PRIMARY KEY, hashes text);");
	}
	
	public long getHash(byte [] key) {
		ByteBuffer buffer =ByteBuffer.wrap(key);
		long id = -1;
		ResultSet rs = session.execute(getHdbStatement.bind(buffer));
		Row r = rs.one();
		if(r != null)
			id = r.getLong(0);
		return id;
	}
	
	public long setHash(byte [] key,long id) {
		ByteBuffer buffer =ByteBuffer.wrap(key);
		ResultSet rs = session.execute(this.hdbStatement.bind(buffer,id));
		Row r = rs.one();
		if(!r.getBool(0)) {
			id = r.getLong(2);
		}
		return id;
	}
	
	public void addCount(long id,long ct) {
		session.execute(this.refdbStatement.bind(ct,id));
	}
	
	public void insertHashes(long id,String hashes) {
		session.execute(insertRefHashes.bind(id,hashes));
	}
	
	public void deleteRef(long id) {
		ResultSet rs = session.execute(getRefHashes.bind(id));
		Row r = rs.one();
		ByteBuffer buf = r.getBytes(0);
		int ct = buf.getInt();
		int hl = buf.getInt();
		byte [] bk = new byte [hl];
		for(int i = 0;i< ct;i++) {
			buf.get(bk);
			ResultSet drs = session.execute(delHdb.bind(ByteBuffer.wrap(bk)));
			System.out.println(drs.one());
		}
		ResultSet _drs = session.execute(delRef.bind(id));
		System.out.println("#######" +_drs.one());
 	}
	
	
	public void close() {
		this.session.close();
		this.cluster.close();
	}

	
	
	public static void main(String [] args) {
		byte ib = 3;
		System.out.println(new Byte(ib).intValue());
		InetSocketAddress [] cipep = {new InetSocketAddress("192.168.0.105",9042)};
		CassandraDedupeDB db = new CassandraDedupeDB(cipep,"datacenter1","sam",1);
		
		Random rnd = new Random();
		
		long id = 78;
		ArrayList<byte []> al = new ArrayList<byte []>();
		long tm = System.currentTimeMillis();
		for(int i = 0;i<1000;i++) {
			byte [] key = new byte[16];
			rnd.nextBytes(key);
			long _id =db.getHash(key);
			if(_id == -1) {
				_id = db.setHash(key,id);
				if(_id == id) {
					al.add(key);
				} else {
					db.addCount(_id, 1);
				}
			} else {
				db.addCount(id, 1);
			}
		}
		System.out.println("Insert Time = "+ (System.currentTimeMillis()-tm));
		
		tm = System.currentTimeMillis();
		db.insertHashes(id, "woweee");
		System.out.println("Ref Insert Time = "+ (System.currentTimeMillis()-tm));
		db.close();
	}
}