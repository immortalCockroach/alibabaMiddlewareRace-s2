package com.alibaba.middleware.race.helper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.ResultImpl;
import com.alibaba.middleware.race.Row;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.StringUtils;

/**
 * 用于0或者1次join时的简化操作，针对query 2 3 4
 * 简化的方式为根据order的goodid或者buyerid取出无重复的buyer或good原始文件的索引信息
 * 然后将这些索引信息按照good或者buyer文件group 并按照offset排序，减少重复读取，并利用系统IO缓冲区
 * @author immortalCockRoach
 *
 */
public class JoinOne implements Iterator<OrderSystem.Result> {
	private List<Row> orderRows;
	private Row fixedRow;
	// orderQueue和joinDataQueue的排序比较器
	private Comparator<Row> comparator;
	private PriorityQueue<Row> orderQueue;
	private Map<String, Row> joinDataMap;
	private List<byte[]> joinDataIndexList;
	// "buyerid" 或者"goodid" null表示不需要join
	private String joinId;
	private Collection<String> queryKeys;
	private List<String> files;
	private IndexOperater operator;

	public JoinOne(List<Row> orderRows, Row fixedRow, Comparator<Row> comparator, String joinId,
			Collection<String> queryKeys, List<String> files, IndexOperater operater) {
		this.orderRows = orderRows;
		this.fixedRow = fixedRow;
		this.comparator = comparator;
		this.joinId = joinId;
		this.queryKeys = queryKeys;
		this.orderQueue = new PriorityQueue<>(64, comparator);
		for (Row orderRow : orderRows) {
			orderQueue.offer(orderRow);
		}
		this.files = files;
		// 读取不同的good(buyer)Id
		// 查找Row(不论是从cache还是通过memoryMap的索引去原始文件查找)组成一个map供之后join
		// 当orderRow为空或者query3最后得到的tag为order或者good的时候 不需要查询join
		if (joinId != null && orderRows.size() > 0) {
			this.operator = operater;
			joinDataIndexList = new ArrayList<>(100);
			joinDataMap = new HashMap<>(64);
			getUniqueDataIndex();
			// 当有没有现成的Row的时候
			if (joinDataIndexList.size() > 0) {
				traverseOriginalFile();
			}
		}
	}

	/**
	 * 根据orderRow的信息查找出无重复的buyer或者good的index信息 放入Map
	 */
	private void getUniqueDataIndex() {
		// 首先尽量从cache中取Row 如果没有的话再去memoryMap中查找
		for (Row orderRow : orderRows) {
			String id = orderRow.getKV(joinId).valueAsString();
			// 已经Map包含这个Row了就跳过
			if (!joinDataMap.containsKey(id)) {
				if (joinId.equals("goodid")) {
					joinDataIndexList.add(((GoodIndexOperater) operator).getGoodTupleByGoodId(id).getOriginalByte());
				} else {
					joinDataIndexList.add(((BuyerIndexOperater) operator).getBuyerTupleByBuyerId(id).getOriginalByte());
				}
			}
		}
	}

	/**
	 * 根据group好的信息去原始的good buyer中查找记录 由于线上测试时join相对于order文件的查找来说开销较小，所以这里没有使用线程池
	 */
	private void traverseOriginalFile() {
		// 此时得到的joinDataIndexSet 为无重复的good/buyer的index信息
		Map<Integer, PriorityQueue<IndexFileTuple>> originalDataAccessSequence = JoinGroupHelper
				.createDataAccessSequence(joinDataIndexList);
		for (Map.Entry<Integer, PriorityQueue<IndexFileTuple>> e : originalDataAccessSequence.entrySet()) {
			String file = null;
			file = files.get(e.getKey());
			// System.out.println("file:"+file);
			IndexFileTuple sequence;
			String line;
			try (RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
				sequence = e.getValue().poll();
				while (sequence != null) {

					long offset = sequence.getOffset();

					byte[] content = new byte[sequence.getLength()];
					orderFileReader.seek(offset);
					orderFileReader.read(content);
					line = new String(content);

					Row kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
					if (joinId.equals("buyerid")) {
						String id = kvMap.getKV("buyerid").valueAsString();
						joinDataMap.put(id, kvMap);
					} else {
						String id = kvMap.getKV("goodid").valueAsString();
						joinDataMap.put(id, kvMap);
					}
					sequence = e.getValue().poll();
				}

			} catch (IOException e1) {

			}
		}
	}

	@Override
	public boolean hasNext() {

		return orderQueue != null && orderQueue.size() > 0;
	}

	@Override
	public Result next() {
		if (!hasNext()) {
			return null;
		}
		// 不需要join的时候直接create
		if (joinId == null) {
			return ResultImpl.createResultRow(orderQueue.poll(), fixedRow, null,
					JoinGroupHelper.createQueryKeys(queryKeys));
		} else {
			// 需要join的时候从Map中拉取
			Row orderRow = orderQueue.poll();
			Row dataRow = joinDataMap.get(orderRow.getKV(joinId).valueAsString());
			return ResultImpl.createResultRow(orderRow, fixedRow, dataRow, JoinGroupHelper.createQueryKeys(queryKeys));
		}
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
	}
}
