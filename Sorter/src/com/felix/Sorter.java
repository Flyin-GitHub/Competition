package com.felix;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Sorter {
	static int longLength[] = { 1, 2, 1, 1, 2, 3 };
	static int colIndex[] = { 0, 1, 3, 4, 5, 7 };
	static int byteLength[] = { 8, 16, 8, 8, 16, 16 };
	static int types[] = { 1, 0, 1, 2, 1, 0 };// true: right
	static int threadCount = 6;
	
	static DecodeTask[] tasks = new DecodeTask[threadCount];
	
	static{
		for(int i = 0; i <  threadCount; i++){
			tasks[i] = new DecodeTask();
		}
	}

	public void call(File inputFile, File outputFile, File tempDir)
			throws Exception {

		// read data
		long start = System.currentTimeMillis();
		FileInputStream fi = new FileInputStream(inputFile);
		FileChannel fc = fi.getChannel();
		int fileSize = (int) inputFile.length();
		ByteBuffer buffer = ByteBuffer.allocate(fileSize);
		fc.read(buffer);
		long end = System.currentTimeMillis();
		System.out.println("read:" + (end - start));
		start = System.currentTimeMillis();
		byte[] biData = buffer.array();
		DecodeTask tempTask = new DecodeTask();
		tempTask.biData = biData;
		tempTask.capacity = 5000010;
		tempTask.blockSize = fileSize;
		start = System.currentTimeMillis();
		tempTask.run();
		end = System.currentTimeMillis();
		
		System.out.println(end - start);
		

	}

	public static void main(String args[]) throws Exception {
		new Sorter().call(new File("input.csv"), new File("output.csv"), null);
	}

	
	private class SortTask implements Runnable{
		public Trade[] data;
		public int start;
		public int end;

		@Override
		public void run() {
			Arrays.sort(data,start,end);
			Arrays.sort(data);
		}
		
	}
	private static class DecodeTask implements Runnable {

		public byte[] biData;

		public int startIndex = 0;
		public int blockSize = 0;
		public int row = 0;
		public int capacity = 1200000;
		public long[][] data = null;

		public Trade lastTrade = new Trade();;

		@Override
		public void run() {
			data = new long[capacity][12];
			int col = 0;
			int rowStart = startIndex;
			int colStart = startIndex;
			byte[] tempData = new byte[32];
			byte[] clearData = new byte[32];
			int length = 0;
			int index = 0;
			//data[row] = new Trade();
			for (index = startIndex; index < startIndex + blockSize; index++) {
				byte c = biData[index];
				if (c == '\n') {
					data[row][10] = rowStart;
					data[row][11] = index - rowStart + 1;
					colStart = rowStart = index + 1;
					col = 0;
					length = 0;
					//data[++row] = new Trade();
				} else if ((c == '\r' || c == ',' || c == '.') && row != 0) {
					if (types[col] == 1) { // right
						int diffLen = byteLength[col] - length;
						
						 for(int i = length - 1; i > 0; i--){ tempData[i +
						 diffLen] = tempData[i]; } for(int j = 0; j <
						 byteLength[col] - length; j ++ ){ tempData[j]=0; }
						 

						System.arraycopy(clearData, 0, tempData, 0, diffLen);
						System.arraycopy(biData, colStart, tempData, diffLen,
								length);

					} else if (types[col] == 2) {// date
						System.arraycopy(biData, colStart + 6, tempData, 0, 4);// year
						System.arraycopy(biData, colStart, tempData, 4, 2);// month
						System.arraycopy(biData, colStart + 2, tempData, 6, 2);
					} else { // left
						int diffLen = byteLength[col] - length;
						System.arraycopy(biData, colStart, tempData, diffLen,
								length);
						System.arraycopy(clearData, 0, tempData, length,
								diffLen);

						
						  for(int i = length - 1; i < byteLength[col]; i ++ ){
						 tempData[i]=0; }
						 
					}
					for (int i = 0; i < longLength[col]; i++) {
						int off = i * 8;
						data[row][colIndex[col] + i] = ((tempData[off + 7] & 0xFFL))
								+ ((tempData[off + 6] & 0xFFL) << 8)
								+ ((tempData[off + 5] & 0xFFL) << 16)
								+ ((tempData[off + 4] & 0xFFL) << 24)
								+ ((tempData[off + 3] & 0xFFL) << 32)
								+ ((tempData[off + 2] & 0xFFL) << 40)
								+ ((tempData[off + 1] & 0xFFL) << 48)
								+ (((long) tempData[off]) << 56);
					}
					colStart = index + 1;
					length = 0;
					col++;
				} else {
					length++;
				}
			}
			/*lastTrade.start = rowStart;
			if ((index - rowStart) != 0) {
				lastTrade.length = index - rowStart;
			}*/

		}

	}
	

}

class Trade implements Comparable<Trade> {
	public long[] data = new long[10];
	public int start = 0;
	public int length = 0;

	@Override
	public int compareTo(Trade o) {
		for (int i = 0; i < 11; i++) {
			if (data[i] > o.data[i])
				return 1;
			if (data[i] < o.data[i])
				return -1;
		}
		return 0;
	}
}
