package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class EDAFileWriter {
	
	public static void writeToFile(String fileName, byte[] content){
		assert content.length%4 == 0;
		File file = new File(fileName);
		try{
			FileOutputStream fos = new FileOutputStream(file);
			fos.write(content);
			fos.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public static void writeToFile(String fileName, int index, byte[] content){
		assert content.length%4 == 0;
		assert index >= 0;
		File file = new File(fileName); // Creates the file if not exists
		try {
			RandomAccessFile RAF = new RandomAccessFile(file, "rw");
			RAF.seek(index);
			RAF.write(content);
			RAF.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void writePeaksToFile(String fileName, int peaksOffset, byte[] peaks){
		assert peaks.length%4 == 0;
		assert peaksOffset >= 0;
		int numOfPeaks = peaks.length/4;

		File file = new File(fileName);
		try {
			FileWriter fw = new FileWriter(file,true);
			BufferedWriter out = new BufferedWriter(fw);
			for(int i = 0; i < numOfPeaks; i++){
				byte[] b = Arrays.copyOfRange(peaks, 4*i, 4*i+4); 
				int globalIndex = Conversions.bytaToInt(b) + peaksOffset;
				out.append(Integer.toString(globalIndex));
				out.append(',');
			}
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
