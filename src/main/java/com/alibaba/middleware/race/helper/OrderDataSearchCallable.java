package com.alibaba.middleware.race.helper;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.StringUtils;

/**
 * order线程池查询的Callable类，负责查询一个order文件中的多条记录
 * 
 * @author immortalCockRoach
 *
 */
public class OrderDataSearchCallable implements Callable<List<Row>> {

	private int fileIndex;
	private PriorityQueue<IndexFileTuple> sequenceQueue;
	private List<String> orderFiles;

	public OrderDataSearchCallable(int fileIndex, PriorityQueue<IndexFileTuple> sequenceQueue,
			List<String> orderFiles) {
		this.fileIndex = fileIndex;
		this.sequenceQueue = sequenceQueue;
		this.orderFiles = orderFiles;
	}

	@Override
	public List<Row> call() throws Exception {
		// 这个sequence不可能是负数
		List<Row> result = new ArrayList<>(sequenceQueue.size());
		String file = orderFiles.get(fileIndex);
		// System.out.println("file:"+file);
		IndexFileTuple sequence;
		try (RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
			sequence = sequenceQueue.poll();
			while (sequence != null) {

				long offset = sequence.getOffset();
				// System.out.println("offset:"+offset);
				// System.out.println("lenth:"+Integer.valueOf(sequence[1]));
				byte[] content = new byte[sequence.getLength()];
				orderFileReader.seek(offset);
				orderFileReader.read(content);
				String line = new String(content);

				Row kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
				// buyer的话需要join
				result.add(kvMap);
				// salerGoodsQueue.offer(kvMap);
				sequence = sequenceQueue.poll();
			}

		}
		return result;
	}

}