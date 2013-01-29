package bolt;


import java.util.Map;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Conversions;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flanagan.analysis.CurveSmooth;

public class EDASmoothBolt extends BaseRichBolt{
		private OutputCollector _collector;
		Logger _logger = LoggerFactory.getLogger(EDASmoothBolt.class);
		
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			_collector = collector;
		}

		/*
		 * Reads the EDA file, performs smoothing on EDA data, and emits it as byte[]
		 * */
		public void execute(Tuple input) {
			String metadata = input.getString(0);			
			byte[] data = input.getBinary(1);
			
			float[] fArr = Conversions.toFloatArr(data);
			double[] eda = Conversions.toDoubla(fArr);
			
			CurveSmooth cs = new CurveSmooth(eda);
			double[] smoothEDA = cs.movingAverage(40);			
			byte[] bArr = Conversions.toBytaArr(smoothEDA);
			//int chunkIndex = Integer.parseInt(fileInfo[2]);
			//_logger.info("---------- SENDING SMOOTHED PART-----------");
			_collector.emit(new Values(metadata, bArr));
		}

		
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("metadata","eda"));
		}
		
	}