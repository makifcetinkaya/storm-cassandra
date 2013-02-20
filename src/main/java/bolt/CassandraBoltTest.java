package bolt;

import java.util.Arrays;
import java.util.List;

import net.lag.kestrel.thrift.Item;

import org.apache.thrift7.TException;

import utils.Conversions;

import backtype.storm.spout.KestrelThriftClient;
import backtype.storm.tuple.Values;

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

public class CassandraBoltTest {
	
	private static final String CASS_HOST_AND_PORT = "localhost:9160";
	private static final String CASS_KEYSPACE_NAME = "EDA";
	private static final String EDA_COL_FAMILY = "EDA";
	private static final int REP_FACTOR = 1;
	private static String CASS_CLUSTER_NAME = "cassStormCluster";
	
	public static final String EDA_COLUMN_FAMILY = "eda_col_fam";	
	private static final String META_COLUMN_NAME = "meta";
	private static final String DATA_COLUMN_NAME = "data";
	private static final String MAX_PEAKS_COL_NAME = "max_peaks";
	private static final String MIN_PEAKS_COL_NAME = "min_peaks";
	
	private static Cluster _cluster;
	private static String _clusterName = CASS_CLUSTER_NAME;
	private static String _cassHostAndPort = CASS_HOST_AND_PORT;
	private static String _keyspaceName = CASS_KEYSPACE_NAME;
	private static KeyspaceDefinition _ksDef;
	private static String _edaColFamily = EDA_COL_FAMILY;
	private static int _replicationFactor = REP_FACTOR;
	private static  Keyspace _keyspace;
	private static ColumnFamilyTemplate<String, String> _colFamTemplate;
	private static ColumnFamilyDefinition _cfDef;
	
	private static KestrelThriftClient _kestrelClient;
	private static String _kestrelHost = "localhost";
	private static int _kestrelPort = 2229;
	private static final String QUEUE_NAME = "eda_queue";
	
	public static final int TIMEOUT = 50;
	private static final int MAGIC_NUM = 239155;
	public static final int NAME_LEN = 50;
	private static final int HEADER_SIZE = 4;
	private static final int META_SIZE = 2*NAME_LEN + 12;
	private static final int DATA_SIZE = 8000;
	private static final int CHUNK_SIZE = DATA_SIZE + META_SIZE + HEADER_SIZE;
	private static final int MAX_ITEMS = 10;
	private static final int AUTO_ABORT_MSEC = 50;
	
	
	private static void prepare(){
		
		_cluster = HFactory.getOrCreateCluster(_clusterName, _cassHostAndPort);
		_ksDef = _cluster.describeKeyspace(_keyspaceName);
		if(_ksDef == null){
			System.out.println("creating schema");
			createSchema();
		}
		_keyspace = HFactory.createKeyspace(_keyspaceName, _cluster, new AllOneConsistencyLevelPolicy());
		_colFamTemplate = new ThriftColumnFamilyTemplate<String, String>(
				_keyspace, _edaColFamily, StringSerializer.get(), StringSerializer.get());
		_colFamTemplate.addColumn(META_COLUMN_NAME, StringSerializer.get());
		_colFamTemplate.addColumn(DATA_COLUMN_NAME, BytesArraySerializer.get());
		_colFamTemplate.addColumn(MAX_PEAKS_COL_NAME, BytesArraySerializer.get());
		_colFamTemplate.addColumn(MIN_PEAKS_COL_NAME, BytesArraySerializer.get());
	}
	
	private static void createSchema(){
		_cfDef = HFactory.createColumnFamilyDefinition(_keyspaceName, _edaColFamily);
		_ksDef = HFactory.createKeyspaceDefinition(_keyspaceName, ThriftKsDef.DEF_STRATEGY_CLASS,
				_replicationFactor, Arrays.asList(_cfDef));
		_cluster.addKeyspace(_ksDef, true);
	}
	
	
	private static boolean isCorrHeader(byte[] byta){
		int num = Conversions.bytaToInt(byta);
		if (num == MAGIC_NUM){
			return true;
		}else{
			return false;
		}
	}
	
	private static String cassTest_MetaArrToStr(byte[] bArr){
		byte[] fName = Arrays.copyOfRange(bArr, 0, NAME_LEN*2);
		
		String type = "eda";
		String uid = "mac";
		
		byte[] cSize = Arrays.copyOfRange(bArr, NAME_LEN*2, NAME_LEN*2+4);
		int chunkSize = Conversions.bytaToInt(cSize);
		byte[] cIndex = Arrays.copyOfRange(bArr, NAME_LEN*2+4, NAME_LEN*2+8);
		int chunkIndex = Conversions.bytaToInt(cIndex);
		
		String startTime = Conversions.bytaToInt(cSize)*Conversions.bytaToInt(cIndex)+"";

		
		return type+","+uid+","+cIndex;
	}
	
	
	private static void insertRawEDAData(ColumnFamilyTemplate<String, String> template, String rowKey, String metadata, byte[] data){
		ColumnFamilyUpdater<String, String> updater = template.createUpdater();
		updater.addKey(rowKey);
		updater.setByteArray(DATA_COLUMN_NAME, data);
		updater.setString(META_COLUMN_NAME, metadata);

		try {
			System.out.println("updating database");
		    template.update(updater);
		} catch (HectorException e) {
			System.out.println("prob adding");
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// key: "eda","mac","1" - > last one is chunk Index
		prepare();
		try {
			_kestrelClient = new KestrelThriftClient(_kestrelHost, _kestrelPort);
			while(true){
				try {
					List<Item> items = _kestrelClient.get(QUEUE_NAME, MAX_ITEMS, TIMEOUT, AUTO_ABORT_MSEC);
					System.out.println("num of items:"+items.size());
					Thread.sleep(1000);
					for(Item item: items){
						byte[] packet = item.get_data();
						assert isCorrHeader(Arrays.copyOfRange(packet, 0, 4));
						byte[] metadata = Arrays.copyOfRange(packet, 4, META_SIZE);
						byte[] data = Arrays.copyOfRange(packet, META_SIZE, META_SIZE+DATA_SIZE);
						String metastr = cassTest_MetaArrToStr(metadata);
						insertRawEDAData(_colFamTemplate, metastr, metastr, data);
					}
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
