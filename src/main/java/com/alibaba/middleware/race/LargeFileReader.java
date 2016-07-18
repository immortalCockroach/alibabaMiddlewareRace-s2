package com.alibaba.middleware.race;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.StringUtils;
@Deprecated
public class LargeFileReader {

	public static void main(String args[]) throws IOException {
		LargeFileReader handler = new LargeFileReader();

		File largeFile = new File("big_order_records.txt");

		handler.readLargeTextFile(largeFile);
	}

	/**
	 * 
	 * A disk controller moves data in fixed-size blocks. The block sizes used
	 * are usually in powers of 2 to simplify addressing.
	 * <p>
	 * Operating systems divide their memory address spaces into memory-pages.
	 * Memory pages are always multiples of the disk block size.
	 * <p>
	 * Buffers are therefore initialised with an initial capacity, that's
	 * typically a memory-page size.
	 */

	/**
	 * Reads a large text file by mapping parts of it into physical memory and
	 * reading it directly by avoiding expensive copy operations between kernel
	 * and user space.
	 * <p>
	 * Memory-mapped file allows one to pretend that the entire file is in
	 * memory there by boosting the programs's performance significantly.
	 * 
	 * @param fileChannel
	 *            Channel of a random access file.
	 * @throws IOException
	 */
	public void readLargeTextFile(File file) throws IOException {
		RandomAccessFile rFile = new RandomAccessFile(file, "r");
		FileChannel fileChannel = rFile.getChannel();

		long fileLength = fileChannel.size();
		// System.out.println("Length of the file measure in bytes = " +
		// fileLength);
		//
		// System.out.println("\n ***** Content of the file ***** \n");
		int i = 0;
		StringBuilder residual = new StringBuilder();
		boolean finalBlockContent = false;
		long start = System.currentTimeMillis();
		for (long position = 0; position < fileLength; position += CommonConstants.BLOCK_SIZE) {

			long size;
			if (position + CommonConstants.BLOCK_SIZE < fileLength)
				size = CommonConstants.BLOCK_SIZE;
			else {
				size = fileLength - position;
				finalBlockContent = true;
			}

			/**
			 * Produce a MappedByteBuffer from the channel, which is a
			 * particular kind of direct buffer. Specify the starting point and
			 * the length of the region that you want to map in the file; this
			 * means that you have the option to map smaller regions of a large
			 * file.
			 * 
			 * The file created is 1 MB long. It appears to be accessible all at
			 * once because only portions of it are brought into memory, and
			 * other parts are swapped out. This way, a large file (up to 2 GB)
			 * can easily be modified. Note that the file-mapping facilities of
			 * the underlying operating system are used to maximise performance.
			 */
			MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size);

			/*
			 * mappedByteBuffer will have a position of zero and a limit and
			 * capacity of size; its mark will be undefined.
			 */

			byte[] destinationByteArray = new byte[mappedByteBuffer.limit()];

			mappedByteBuffer.get(destinationByteArray);
			String blockContent = StringUtils.convert(destinationByteArray, "UTF-8");
			// blockContent = blockContent.trim();

			// Prepend the residual string if it exists
			if (!StringUtils.isBlank(residual.toString())) {
				blockContent = residual + blockContent;
				residual.delete(0, residual.length());
			}

			if (!blockContent.endsWith(CommonConstants.NEW_LINE) && !finalBlockContent) {
				int lastNewLineIndex = StringUtils.lastIndexOf(blockContent, CommonConstants.NEW_LINE);

				String contentAfterLastLineSperator = StringUtils.substring(blockContent, lastNewLineIndex);

				if (!StringUtils.isBlank(contentAfterLastLineSperator)) {
					// Save the string after last line separator
					residual.append(contentAfterLastLineSperator);
					// remove the residual string from block content
					blockContent = StringUtils.removeEnd(blockContent, contentAfterLastLineSperator);
				}

			}
			List<String> lines = StringUtils.tokenise(blockContent);

			// Printing the content of each line to prove this works!
			// System.out.println("********** Content of 1 kb block printed one
			// line at a time. **********");
			for (String line : lines) {
				try {
					if(line!=null && line.length()>0)
					createKVMapFromLine(line);
				} catch (StringIndexOutOfBoundsException e) {
					System.out.println("line:" + line);
				}
			}

			// TODO: Obtain the lines of textual content for each block of data
			// and process it further according to your needs.
			// Feel free to adapt this - One approach is to process these blocks
			// of lines in parallel instead of sequential processing
		}
		 System.out.println(System.currentTimeMillis()-start);
		// System.out.println("********** End of file's content **********");
		rFile.close();
		fileChannel.close();
	}

	private static Map<String, String> createKVMapFromLine(String line) {
		String[] kvs = line.split("\t");
		Map<String, String> kvMap = new HashMap<>();
		for (String rawkv : kvs) {
			int p = rawkv.indexOf(':');
			String key = rawkv.substring(0, p);
			String value = rawkv.substring(p + 1);
			if (key.length() == 0 || value.length() == 0) {
				throw new RuntimeException("Bad data:" + line);
			}
			kvMap.put(key, value);
		}
		return kvMap;
	}

}
