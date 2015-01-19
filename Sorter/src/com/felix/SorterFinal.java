package com.felix;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * This a main class is for the final competition.</br>
 * 
 * 256M * 1024 = 262144 BYTE
 * 
 * 900 * 4 = 3600 Byte
 * 
 * 262144 - 3600 = 258544 byte
 * 
 * 
 * 
 * 
 * @author Felix
 * 
 */

public class SorterFinal {

	/**
	 * this is not a realize size, just approximately.
	 */
	private static final int MAX_BYTE_COUNT_OF_LINE = 128;
	
	/**
	 * the size of buffer,
	 * which is used to storage the original data from file.
	 */
	private static final int MAX_BYTE_COUNT_OF_BUFFER = 200000;
	
	/**
	 * the block count of file
	 */
	private static final int BLOCK_COUNT = 3;
	
	
	/**
	 * the thread count of process block data.
	 */
	
	private static final int THREAD_COUNT = 8;
	
	/**
	 * it is a data buffer,
	 * which is used to storage the data which has been decoded.
	 */
	private static int[][] data = new int[9000][];
	
	
	

	public void call(File inputFile, File outputFile, File tempDir) 
			throws Exception {
		long fileLen = inputFile.length();
		long blockSize = fileLen / BLOCK_COUNT;
		int blockBufferSize = MAX_BYTE_COUNT_OF_BUFFER / BLOCK_COUNT;
		FileInputStream fi = new FileInputStream(inputFile);
		FileChannel fc = fi.getChannel();
		
		

	}

	/**
	 * Get the end offset which is a end of line of a block with a specified end
	 * offset,
	 * 
	 * @param fc
	 * @param end
	 * @return 
	 * 		the next index of block end offset index.
	 * @throws IOException
	 */
	private int getBlockEndOffset(FileChannel fc, int end) 
			throws IOException {
		ByteBuffer buffer = fc.map(MapMode.READ_ONLY, end,
				MAX_BYTE_COUNT_OF_LINE);
		for ( ; '\n' != buffer.get(); end++);
		return end;
	}
	
	
 
	private int getBlockEndOffset(byte[] biData, int end) 
			throws IOException {
		for ( ; '\n' != biData[end]; end++);
		return end;
	}
	
	private void processBlockData(FileInputStream fi, int bufferSize,
			int start, int end) throws IOException{
		ByteBuffer buffer = ByteBuffer.allocateDirect(
				bufferSize);
		fi.skip(start);
		FileChannel fc = fi.getChannel();
		
		int _len = fc.read(buffer);
		int _blockSize = _len / THREAD_COUNT;
		
		byte[] biData = buffer.array();
		int _end = getBlockEndOffset(biData,_blockSize);
		
	}

	public static void main(String args[]) {

	}
	

	class ParseTask implements Runnable{
		
		private byte biData;
		private int start;
		private int end;

		@Override
		public void run() {
			
		}
		
	}
}
