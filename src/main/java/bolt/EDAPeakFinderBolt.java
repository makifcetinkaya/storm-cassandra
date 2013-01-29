package bolt;


import java.util.ArrayList;
import java.util.Map;

import utils.Conversions;
import utils.PeakDetector;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class EDAPeakFinderBolt extends BaseRichBolt{

	private OutputCollector _collector;
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
	}

	/*
	 * Takes a float of byte array as input, and find peaks on the values
	 * */
	public void execute(Tuple input) {
		// TODO create peak detector according to sampling rate
		String metadata = input.getString(0);
		byte[] data = input.getBinary(1);
		//_collector.emit(new Values(metadata, data, data, data));
		
		int chunkIndex = Integer.parseInt(metadata.split(",")[2]);
		
		float[] vals = Conversions.toFloatArr(data);
		ArrayList[] peaks = PeakDetector.peakDetect(vals, 300, 0);
		ArrayList<Integer> maxPeaks = peaks[0];
		ArrayList<Integer> minPeaks = peaks[1];
		
		byte[] maxPeaksByta = Conversions.toBytaArr(maxPeaks.toArray(new Integer[0]));
		byte[] minPeaksByta = Conversions.toBytaArr(minPeaks.toArray(new Integer[0]));
		//System.out.println("---------- SENDING PEAKS FOR PART: "+chunkIndex+"-----------");
		_collector.emit(new Values(metadata, data, maxPeaksByta, minPeaksByta));
	}
	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("metadata","eda", "maxPeaks", "minPeaks"));
	}

}
