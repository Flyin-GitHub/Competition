package com.felix;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SorterTest {
	static int longLength[] = { 1, 2, 1, 1, 2, 3 };
	static int colIndex[] = { 0, 1, 3, 4, 5, 7 };
	static int byteLength[] = { 8, 16, 8, 8, 16, 16 };
	static int types[] = { 1, 0, 1, 2, 1, 0 };// true: right
	static int threadCount = 6;

	static DecodeTask[] tasks = new DecodeTask[threadCount];

	static {
		for (int i = 0; i < threadCount; i++) {
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
		/*
		 * ByteBuffer buffer = ByteBuffer.allocate(fileSize); fc.read(buffer);
		 */
		MappedByteBuffer buffer = fc.map(MapMode.READ_ONLY, 0, fileSize);
		long end = System.currentTimeMillis();
		//System.out.println("read:" + (end - start));
		//start = System.currentTimeMillis();
		// byte[] biData = buffer.array();
		int eachBlockSize = fileSize / threadCount;
		int balance = eachBlockSize + (fileSize % threadCount);
		Trade[] data = new Trade[10000000];
		// multi-thread
		ExecutorService executors = Executors.newFixedThreadPool(threadCount);
		tasks = new DecodeTask[threadCount];
		for (int i = 0; i < threadCount - 1; i++) {
			DecodeTask task = new DecodeTask();
			task.buffer = buffer;
			task.startIndex = i * eachBlockSize;
			task.blockSize = eachBlockSize;
			tasks[i] = task;
			executors.submit(task);
		}
		if (0 != balance) {

			DecodeTask task = new DecodeTask();
			task.buffer = buffer;
			task.startIndex = (threadCount - 1) * eachBlockSize;
			task.blockSize = balance;
			tasks[threadCount - 1] = task;
			executors.submit(task);
		} else {
			DecodeTask task = new DecodeTask();
			task.buffer = buffer;
			task.startIndex = (threadCount - 1) * eachBlockSize;
			task.blockSize = eachBlockSize;
			tasks[threadCount - 1] = task;
			executors.submit(task);
		}

		executors.shutdown();
		executors.awaitTermination(1, TimeUnit.MINUTES);
		ExecutorService sortExecutors = Executors
				.newFixedThreadPool(threadCount);
		for (int i = 0; i < threadCount - 1; i++) {
			DecodeTask tempTask = new DecodeTask();
			tempTask.buffer = buffer;
			tempTask.startIndex = tasks[i].lastTrade.start;
			tempTask.blockSize = tasks[i].lastTrade.length
					+ tasks[i + 1].data[0].length;
			tempTask.capacity = 3;
			tempTask.row = 1;
			tempTask.run();
			tasks[i].data[(tasks[i].row)++] = tempTask.data[1];
			SortTask sortTask = new SortTask();
			sortTask.start = 1;
			sortTask.end = tasks[i].row;
			sortTask.data = tasks[i].data;
			sortExecutors.submit(sortTask);

		}

		SortTask sortTask = new SortTask();
		sortTask.start = 1;
		sortTask.end = tasks[threadCount - 1].row;
		sortTask.data = tasks[threadCount - 1].data;
		sortExecutors.submit(sortTask);
		sortExecutors.shutdown();
		sortExecutors.awaitTermination(1, TimeUnit.MINUTES);

		int row = 0;
		data[row++] = tasks[0].data[0];
		for (int i = 0; i < threadCount; i++) {
			for (int j = 1; j < tasks[i].row; j++)
				data[row++] = tasks[i].data[j];
		}
		//end = System.currentTimeMillis();
		//System.out.println(end - start);
		// sort the data;
		//start = System.currentTimeMillis();
		Arrays.sort(data, 1, row);
		//end = System.currentTimeMillis();
		//System.out.println("sort:" + (end - start));
		// output the data
		//start = System.currentTimeMillis();
		RandomAccessFile arf = new RandomAccessFile(outputFile, "rw");
		FileChannel foc = arf.getChannel();
		MappedByteBuffer mbb = foc.map(MapMode.READ_WRITE, 0,
				inputFile.length());
		for (int i = 0; i < row; i++) {
			buffer.limit(data[i].start + data[i].length);
			buffer.position(data[i].start);
			mbb.put(buffer);
		}
		fi.close();
		arf.close();
		end = System.currentTimeMillis();
		System.out.println(end - start);

	}

	public static void main(String args[]) throws Exception {
		new SorterTest()
				.call(new File("input.csv"), new File("output.csv"), null);
	}

	private class SortTask implements Runnable {
		public Trade[] data;
		public int start;
		public int end;

		@Override
		public void run() {
			Arrays.sort(data, start, end);
		}

	}

	private static class DecodeTask implements Runnable {

		public MappedByteBuffer buffer;

		public int startIndex = 0;
		public int blockSize = 0;
		public int row = 0;
		public int capacity = 1200000;
		public Trade[] data = null;

		public Trade lastTrade = new Trade();;

		@Override
		public void run() {
			data = new Trade[capacity];
			int col = 0;
			int rowStart = startIndex;
			int colStart = startIndex;
			byte[] tempData = new byte[32];
			byte[] clearData = new byte[32];
			int length = 0;
			int index = 0;
			data[row] = new Trade();
			try {

				for (index = startIndex; index < startIndex + blockSize; index++) {
					byte c = buffer.get(index);
					if (c == '\n') {
						data[row].start = rowStart;
						data[row].length = index - rowStart + 1;
						colStart = rowStart = index + 1;
						col = 0;
						length = 0;
						data[++row] = new Trade();
					} else if ((c == '\r' || c == ',' || c == '.') && row != 0) {
						if (types[col] == 1) { // right
							int diffLen = byteLength[col] - length;

							System.arraycopy(clearData, 0, tempData, 0, diffLen);

							for (int i = diffLen, end = diffLen + length, position = colStart; i < end; i++, position++)
								tempData[i] = buffer.get(position);

						} else if (types[col] == 2) {// date
							for (int i = 0, end = 4, position = colStart + 6; i < end; i++, position++)
								tempData[i] = buffer.get(position);
							for (int i = 4, end = 6, position = colStart; i < end; i++, position++)
								tempData[i] = buffer.get(position);
							for (int i = 6, end = 8, position = colStart + 2; i < end; i++, position++)
								tempData[i] = buffer.get(position);
						} else { // left
							int diffLen = byteLength[col] - length;
							for (int i = diffLen, end = diffLen + length, position = colStart; i < end; i++, position++)
								tempData[i] = buffer.get(position);
							System.arraycopy(clearData, 0, tempData, length,
									diffLen);

						}
						for (int i = 0; i < longLength[col]; i++) {
							int off = i * 8;
							data[row].data[colIndex[col] + i] = ((tempData[off + 7] & 0xFFL))
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
			} catch (Exception e) {
				System.out.println("c:" + buffer.capacity() + "p:"
						+ buffer.position() + "l:" + buffer.limit());
				e.printStackTrace();
			}
			lastTrade.start = rowStart;
			if ((index - rowStart) != 0) {
				lastTrade.length = index - rowStart;
			}

		}

	}

}
