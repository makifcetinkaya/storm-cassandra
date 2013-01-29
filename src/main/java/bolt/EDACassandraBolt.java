package bolt;

import java.util.Arrays;
import java.util.Map;

import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class EDACassandraBolt extends BaseRichBolt{

	public static final String CASSANDRA_HOST_AND_PORT = "cass_host";
	public static final String CASSANDRA_KEYSPACE_NAME = "cass_ks_name";
	public static final String CASSANDRA_CLUSTER_NAME= "cass_cname";
	public static final String REPLICATION_FACTOR = "rep_factor";
	
	public static final String EDA_COLUMN_FAMILY = "eda_col_fam";	
	private static final String META_COLUMN_NAME = "meta";
	private static final String DATA_COLUMN_NAME = "data";
	private static final String MAX_PEAKS_COL_NAME = "max_peaks";
	private static final String MIN_PEAKS_COL_NAME = "min_peaks";
	
	private Cluster _cluster;
	private String _clusterName;
	private String _cassHostAndPort;
	private String _keyspaceName;
	private KeyspaceDefinition _ksDef;
	private String _edaColFamily;
	private int _replicationFactor;
	private Keyspace _keyspace;
	private ColumnFamilyTemplate<String, String> _colFamTemplate;
	private ColumnFamilyDefinition _cfDef;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_clusterName = stormConf.get(CASSANDRA_CLUSTER_NAME).toString();
		_cassHostAndPort = stormConf.get(CASSANDRA_HOST_AND_PORT).toString();
		_keyspaceName = stormConf.get(CASSANDRA_KEYSPACE_NAME).toString();
		_edaColFamily = stormConf.get(EDA_COLUMN_FAMILY).toString();
		_replicationFactor = Integer.parseInt(stormConf.get(REPLICATION_FACTOR).toString());
		_cluster = HFactory.getOrCreateCluster(_clusterName, _cassHostAndPort);
		_ksDef = _cluster.describeKeyspace(_keyspaceName);
		if(_ksDef == null){
			System.out.println("creating schema");
			this.createSchema();
		}
		_keyspace = HFactory.createKeyspace(_keyspaceName, _cluster, new AllOneConsistencyLevelPolicy());
		_colFamTemplate = new ThriftColumnFamilyTemplate<String, String>(
				_keyspace, _edaColFamily, StringSerializer.get(), StringSerializer.get());
		_colFamTemplate.addColumn(META_COLUMN_NAME, StringSerializer.get());
		_colFamTemplate.addColumn(DATA_COLUMN_NAME, BytesArraySerializer.get());
		_colFamTemplate.addColumn(MAX_PEAKS_COL_NAME, BytesArraySerializer.get());
		_colFamTemplate.addColumn(MIN_PEAKS_COL_NAME, BytesArraySerializer.get());
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String metadata = input.getString(0);
		byte[] data = input.getBinary(1);
		byte[] maxPeaks = input.getBinary(2);
		byte[] minPeaks = input.getBinary(3);
		
		String rowKey = this.createRowKey(metadata);
		
		ColumnFamilyUpdater<String, String> updater = _colFamTemplate.createUpdater();
		updater.addKey(rowKey);
		updater.setString(META_COLUMN_NAME, metadata);
		updater.setByteArray(DATA_COLUMN_NAME, data);
		updater.setByteArray(MAX_PEAKS_COL_NAME, maxPeaks);
		updater.setByteArray(MIN_PEAKS_COL_NAME, minPeaks);
		try {
			_colFamTemplate.update(updater);
		} catch (HectorException e) {
			System.out.println("prob adding");
			e.printStackTrace();
		}
		
	}
	
	//TODO: make usre the data_type+user_id+start_time info are in pre-determined format
	private String createRowKey(String metadata){
		String[] chunkInfo = metadata.split(",");
		return chunkInfo[0]+","+chunkInfo[1]+","+chunkInfo[2];
	}
	
	private void createSchema(){
		_cfDef = HFactory.createColumnFamilyDefinition(_keyspaceName, _edaColFamily);
		_ksDef = HFactory.createKeyspaceDefinition(_keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS,
				_replicationFactor, Arrays.asList(_cfDef));
		_cluster.addKeyspace(_ksDef, true);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}



}
