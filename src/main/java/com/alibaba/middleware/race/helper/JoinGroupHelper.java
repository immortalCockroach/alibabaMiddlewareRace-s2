package com.alibaba.middleware.race.helper;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;

public class JoinGroupHelper {

	/**
	 * 判断key的join 只对单个key有效果,根据具体的key决定join的表 order代表不join buyer代表只join buyer表
	 * good表示只join good表 返回all代表无法判断 全部join
	 * 
	 * @param key
	 * @return
	 */
	public static String getKeyJoin(String key) {
		if (key.startsWith("a_b_")) {
			return "buyer";
		}

		if (key.startsWith("a_g_")) {
			return "good";
		}

		if (key.startsWith("a_o_")) {
			return "order";
		}
		if (key.equals("orderid") || key.equals("buyerid") || key.equals("goodid") || key.equals("createtime")
				|| key.equals("amount") || key.equals("done") || key.equals("remark")) {
			return "order";
		}

		if (key.equals("buyername") || key.equals("address") || key.equals("contactphone")) {
			return "buyer";
		}

		if (key.equals("salerid") || key.equals("price") || key.equals("offprice") || key.equals("description")
				|| key.equals("good_name")) {
			return "good";
		}

		return "all";
	}

	public static HashSet<String> createQueryKeys(Collection<String> keys) {
		if (keys == null) {
			return null;
		}
		return new HashSet<String>(keys);
	}

	/**
	 * 根据需要访问的原始文件(包括order、buyer和good)和其中每条记录的offset分别进行group和排序
	 * 减少原始文件的打开和关闭，对offset排序后尽量多利用系统的IO缓冲区
	 * 在query2 3 4读取order原始文件和join时都使用
	 * 
	 * @param offsetRecords
	 * @return
	 */
	public static Map<Integer, PriorityQueue<IndexFileTuple>> createDataAccessSequence(
			Collection<byte[]> offsetRecords) {
		Map<Integer, PriorityQueue<IndexFileTuple>> result = new HashMap<>(64);
		// 对原始记录进行按文件group,文件内按offset排序
		for (byte[] e : offsetRecords) {
			IndexFileTuple tuple = new IndexFileTuple(e);
			int fileIndex = tuple.getFileIndex();
			if (result.containsKey(tuple.getFileIndex())) {
				result.get(fileIndex).offer(tuple);
			} else {
				PriorityQueue<IndexFileTuple> resultQueue = new PriorityQueue<>(50, new Comparator<IndexFileTuple>() {

					// 比较记录的offset
					@Override
					public int compare(IndexFileTuple o1, IndexFileTuple o2) {
						// TODO Auto-generated method stub
						return o1.getOffset() > o2.getOffset() ? 1 : -1;
					}

				});
				resultQueue.offer(tuple);
				result.put(fileIndex, resultQueue);
			}
		}
		return result;
	}
}
