package utils;
import java.util.ArrayList;
import java.util.Arrays;


public class PeakDetector {

	public static ArrayList[] peakDetect(float[] vals, int lookAhead, float delta){
		ArrayList<Integer> maxPeaks = new ArrayList<Integer>();
		ArrayList<Integer> minPeaks = new ArrayList<Integer>();
		int dump = -1;		
		float mn = Float.MAX_VALUE;
		float mx = Float.MIN_VALUE;
		int mxpos = 0; int mnpos = 0;
		float[] valsAhead;
		int length = vals.length;
		
		assert lookAhead > 0;
		
		for(int i = 0; i<length-lookAhead; i++){
			//System.out.println("i is:"+i);
			float y = vals[i];
			if (y > mx){ mx = y; mxpos = i; }
			if (y < mn){ mn = y; mnpos = i; }
			
			// Look for a maximum
			if (y < mx-delta  && mx != Float.MAX_VALUE){
				valsAhead = Arrays.copyOfRange(vals, i, i+lookAhead);
				float maxAhead = getMax(valsAhead);
				if (maxAhead < mx -delta ){ 
					//System.out.println("max val:"+vals[i]);
					maxPeaks.add(mxpos);
					if(dump == -1){ dump = 1;}
					mx = Float.MAX_VALUE;
					mn = Float.MAX_VALUE;
					if (i+lookAhead >= length){ break; }
					continue;
				}
			}
			
			// Look for a minimum
			if (y > mn+delta && mn != Float.MIN_VALUE){
				valsAhead = Arrays.copyOfRange(vals, i, i+lookAhead); 
				float minAhead = getMin(valsAhead);
				if (minAhead > mn ){
					//System.out.println("min val:"+vals[i]);
					minPeaks.add(mnpos);
					if(dump == -1){ dump = 2;}
					mn = Float.MIN_VALUE;
					mx = Float.MIN_VALUE;
					if (i+lookAhead >= length){ break;	}
					continue;
				}
			}
		}
		if (dump == 1){ maxPeaks.remove(0); }
		if (dump == 2){ minPeaks.remove(0); }
		
		return new ArrayList[]{maxPeaks, minPeaks};
	}
	
	private static  float getMax(float[] arr){
		float max = arr[0];
		for(int i = 0; i < arr.length; i++){
			if (arr[i] > max){ max = arr[i]; }
		}
		return max;
	}
	
	private static float getMin(float[] arr){
		float min = arr[0];
		for(int i = 0; i < arr.length; i++){
			if (arr[i] < min){ min = arr[i]; }
		}
		return min;
	}
}
