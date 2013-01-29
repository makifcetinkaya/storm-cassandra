package spout;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.lag.kestrel.thrift.Item;

import org.apache.thrift7.TException;

import utils.Conversions;

import backtype.storm.spout.KestrelThriftClient;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class EDAChunkSpout extends BaseRichSpout{
		
		private int _port;
		private String _queueName;
		private String _hostname;
		
		public static final String EDA_OUT_DIR = "/home/slices/output/";
		
		public static final int TIMEOUT = 50;
		private static final int MAGIC_NUM = 239155;
		public static final int NAME_LEN = 50;
		private static final int HEADER_SIZE = 4;
		private static final int META_SIZE = 2*NAME_LEN + 12;
		private static final int DATA_SIZE = 8000;
		private static final int CHUNK_SIZE = DATA_SIZE + META_SIZE + HEADER_SIZE;
		private static final int MAX_ITEMS = 10;
		private static final int AUTO_ABORT_MSEC = 50;
			
		private SpoutOutputCollector _collector;
		private Map _conf;
		private File _logFile;
		private FileWriter _fileWriter;
		private int _i;
		KestrelThriftClient _kestrelClient;
		

		public EDAChunkSpout(String hostname, int port, String queueName) {
			_hostname = hostname;
			_port = port;
			_queueName = queueName;
		// TODO Auto-generated constructor stub
		}
		
		
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			_i = 1;
			_collector = collector;
			_conf = conf;
			_logFile = new File(EDA_OUT_DIR+"log-combine");
			try {
				_kestrelClient = new KestrelThriftClient(_hostname, _port);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
				
				
			try {
				_fileWriter = new FileWriter(_logFile, true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
						
		}

		public void nextTuple() {
			// TODO Auto-generated method stub			
			try {
				List<Item> items = _kestrelClient.get(_queueName, MAX_ITEMS, TIMEOUT, AUTO_ABORT_MSEC);
				//System.out.println("num of items:"+items.size());
				for(Item item: items){
					byte[] packet = item.get_data();
					assert isCorrHeader(Arrays.copyOfRange(packet, 0, 4));
					byte[] metadata = Arrays.copyOfRange(packet, 4, META_SIZE);
					byte[] data = Arrays.copyOfRange(packet, META_SIZE, META_SIZE+DATA_SIZE);
					String metastr = cassTest_MetaArrToStr(metadata);
					System.out.println("emitting values now");
					_collector.emit(new Values(metastr, data));
				}
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}

		private  void log(String msg){
			try {
				_fileWriter.append(msg+'\n');
				_fileWriter.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		private static boolean isCorrHeader(byte[] byta){
			int num = Conversions.bytaToInt(byta);
			if (num == MAGIC_NUM){
				return true;
			}else{
				return false;
			}
		}
		public static String metaArrToStr(byte[] bArr){
			byte[] fName = Arrays.copyOfRange(bArr, 0, NAME_LEN*2);
			String fileName = Conversions.bytaToUTF(fName);
			byte[] cSize = Arrays.copyOfRange(bArr, NAME_LEN*2, NAME_LEN*2+4);
			int chunkSize = Conversions.bytaToInt(cSize);
			byte[] cIndex = Arrays.copyOfRange(bArr, NAME_LEN*2+4, NAME_LEN*2+8);
			int chunkIndex = Conversions.bytaToInt(cIndex);
			byte[] nOfChunks = Arrays.copyOfRange(bArr, NAME_LEN*2+8, NAME_LEN*2+12);
			int numOfChunks = Conversions.bytaToInt(nOfChunks);
			
			return fileName+","+chunkSize+","+chunkIndex+","+numOfChunks;
		}
		
		public static String cassTest_MetaArrToStr(byte[] bArr){
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
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("metadata", "eda"));
		}		
	}
