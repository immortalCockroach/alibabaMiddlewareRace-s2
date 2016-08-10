package com.alibaba.middleware.race.utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class IOUtils {
	public static ExtendBufferedReader createReader(String file, int blockSize) throws FileNotFoundException {
		return new ExtendBufferedReader(new FileReader(file), blockSize);
	}
	
	public static ExtendBufferedWriter createWriter(String file, int blockSize) throws IOException {
		return new ExtendBufferedWriter(new FileWriter(file), blockSize);
	}
	
	public static String readLine(String file, long offset, int length) {
		byte[] content = new byte[length];
		String line = null;
		try (RandomAccessFile buyerFileReader = new RandomAccessFile(file, "r")) {
			buyerFileReader.seek(offset);
			buyerFileReader.read(content);
			line = new String(content);

		} catch (IOException e) {
			// 忽略
		}
		return line;
	}
	
	public static List<byte[]> readOrderFilesGroupByGood(String orderIndexFile, long offset, int count) {
		List<byte[]> offsetRecords = new ArrayList<>(100);
		try (RandomAccessFile indexFileReader = new RandomAccessFile(orderIndexFile, "r")) {
			indexFileReader.seek(offset);
			for (int i = 0; i <= count - 1; i++) {
				byte[] content = new byte[16];
				indexFileReader.read(content);
				offsetRecords.add(content);

			}

		} catch (IOException e) {

		}
		return offsetRecords;
	}
}
