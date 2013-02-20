
import org.apache.log4j.Logger;

import spout.EDAChunkSpout;
import utils.EDACassandraInsertBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import bolt.EDACassandraBolt;
import bolt.EDAPeakFinderBolt;
import bolt.EDASmoothBolt;


public class EDACassandraTopology {
	
	private static final String QUEUE_HOST = "localhost";
	private static final String QUEUE_NAME = "eda_queue";
	private static final int QUEUE_PORT = 2229;
	
	private static final String CASS_HOST_AND_PORT = "localhost:9160";
	private static final String CASS_KEYSPACE_NAME = "EDA";
	private static final String EDA_COL_FAMILY = "EDA";
	private static final int REP_FACTOR = 1;
	private static String CASS_CLUSTER_NAME = "cassStormCluster";
	
	
	public static Logger _logger = Logger.getLogger(EDACassandraTopology.class);
	public static int MAX_SPOUT_PENDING = 1000;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		int NUM_OF_WORKERS = Integer.parseInt(args[0]);
		int NUM_OF_CASS_BOLTS = Integer.parseInt(args[1]);
		//int MAX_SPOUT_PENDING = Integer.parseInt(args[2]);
		//int NUM_OF_MACHINES = Integer.parseInt(args[0]);
		
		Config config = new Config();
	    config.put(EDACassandraBolt.CASSANDRA_HOST_AND_PORT, CASS_HOST_AND_PORT);
	    config.put(EDACassandraBolt.CASSANDRA_KEYSPACE_NAME, CASS_KEYSPACE_NAME);
		config.put(EDACassandraBolt.CASSANDRA_CLUSTER_NAME, CASS_CLUSTER_NAME);    
		config.put(EDACassandraBolt.EDA_COLUMN_FAMILY, EDA_COL_FAMILY);
		config.put(EDACassandraBolt.REPLICATION_FACTOR, REP_FACTOR);
		
		TopologyBuilder builder = new TopologyBuilder();
		EDAChunkSpout kts = new EDAChunkSpout(QUEUE_HOST, QUEUE_PORT, QUEUE_NAME);
		builder.setSpout("pktspout", kts, 1);
		EDASmoothBolt esb = new EDASmoothBolt();
		builder.setBolt("smoother", esb ,NUM_OF_WORKERS).shuffleGrouping("pktspout");
		EDAPeakFinderBolt epfb = new EDAPeakFinderBolt();
		builder.setBolt("peakfinder", epfb, NUM_OF_WORKERS).shuffleGrouping("smoother");
		
		//TODO: CassandraBolt ID row
		EDACassandraInsertBolt ecb = new EDACassandraInsertBolt();
		builder.setBolt("cassandra", ecb, NUM_OF_CASS_BOLTS).shuffleGrouping("peakfinder");
		
		StormTopology topology = builder.createTopology();
		Config conf = new Config();
		conf.setNumWorkers(NUM_OF_WORKERS);
		conf.setMaxSpoutPending(MAX_SPOUT_PENDING);
		
		conf.setDebug(true);
		System.out.println(conf);
//		try {
//			StormSubmitter.submitTopology("simple", conf, topology);
//		} catch (AlreadyAliveException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InvalidTopologyException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}		
//		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, topology);
		//Utils.sleep(30000);
		//cluster.killTopology("test");
		//cluster.shutdown();
		
	}

}
