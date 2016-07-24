package com.alibaba.middleware.race;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedReader;
import com.alibaba.middleware.race.utils.ExtendBufferedWriter;
import com.alibaba.middleware.race.utils.IOUtils;
import com.alibaba.middleware.race.utils.StringUtils;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {

	static private String booleanTrueValue = "true";
	static private String booleanFalseValue = "false";
	
	private Collection<String> orderFiles;
	private Collection<String> goodFiles;
	private Collection<String> buyerFiles;
	
	private String query1Path;
	private String query2Path;
	private String query3Path;
//	private String query4Path;
	
	private String buyersPath;
	private String goodsPath;
	
	private ExtendBufferedWriter[] query1Writers;
	private long[] query1Offset;
	private ExtendBufferedWriter[] query1IndexWriters;
	private ExtendBufferedWriter[] query2Writers;
	private long[] query2Offset;
	private ExtendBufferedWriter[] query2IndexWriters;
	private ExtendBufferedWriter[] query3Writers;
	private long[] query3Offset;
	private ExtendBufferedWriter[] query3IndexWriters;
//	private BufferedWriter[] query4Writers;
	
	private ExtendBufferedWriter[] buyersWriters;
	private long[] buyersOffset;
	private ExtendBufferedWriter[] buyersIndexWriters;
	private ExtendBufferedWriter[] goodsWriters;
	private long[] goodsOffset;
	private ExtendBufferedWriter[] goodsIndexWriters;
	
//	private Lock query1Lock;
//	private Lock query2Lock;
//	private Lock query3Lock;
//	private Lock query4Lock;

	/**
	 * KeyValue的实现类，代表一行中的某个key-value对 raw数据采用String来存储 之后根据情况返回对应的long获得double
	 * 
	 * @author immortalCockRoach
	 *
	 */
	static private class KV implements Comparable<KV>, KeyValue {
		String key;
		String rawValue;

		boolean isComparableLong = false;
		long longValue;

		private KV(String key, String rawValue) {
			this.key = key;
			this.rawValue = rawValue;
			if (key.equals("createtime") || key.equals("orderid")) {
				isComparableLong = true;
				longValue = Long.parseLong(rawValue);
			}
		}

		public String key() {
			return key;
		}

		public String valueAsString() {
			return rawValue;
		}

		public long valueAsLong() throws TypeException {
			try {
				return Long.parseLong(rawValue);
			} catch (NumberFormatException e) {
				throw new TypeException();
			}
		}

		public double valueAsDouble() throws TypeException {
			try {
				return Double.parseDouble(rawValue);
			} catch (NumberFormatException e) {
				throw new TypeException();
			}
		}

		public boolean valueAsBoolean() throws TypeException {
			if (this.rawValue.equals(booleanTrueValue)) {
				return true;
			}
			if (this.rawValue.equals(booleanFalseValue)) {
				return false;
			}
			throw new TypeException();
		}

		public int compareTo(KV o) {
			if (!this.key().equals(o.key())) {
				throw new RuntimeException("Cannot compare from different key");
			}
			if (isComparableLong) {
				return Long.compare(this.longValue, o.longValue);
			}
			return this.rawValue.compareTo(o.rawValue);
		}

		@Override
		public String toString() {
			return "[" + this.key + "]:" + this.rawValue;
		}
	}

	/**
	 * Row为一行的数据，使用一个Map来存储 key为每行数据中的key,而value为每行数据中的k-v对
	 * 
	 * @author immortalCockRoach
	 *
	 */
	@SuppressWarnings("serial")
	static class Row extends HashMap<String, KV> {
		Row() {
			super();
		}

		Row(KV kv) {
			super();
			this.put(kv.key(), kv);
		}

		KV getKV(String key) {
			KV kv = this.get(key);
			if (kv == null) {
				throw new RuntimeException(key + " is not exist");
			}
			return kv;
		}
		
		KV removeKV(String key){
			KV kv = this.remove(key);
			return kv;
		}

		Row putKV(String key, String value) {
			KV kv = new KV(key, value);
			this.put(kv.key(), kv);
			return this;
		}

		Row putKV(String key, long value) {
			KV kv = new KV(key, Long.toString(value));
			this.put(kv.key(), kv);
			return this;
		}
	}
	
	class HashIndexCreator implements Runnable{
		private String hashId;
		private ExtendBufferedWriter[] writers;
		private ExtendBufferedWriter[] offSetwriters;
		private long[] writersOffset;
		private Collection<String> files;
		private CountDownLatch latch;
		private final int BUCKET_SIZE;
		private final int BLOCK_SIZE;
		private String[] identities;
		
		public HashIndexCreator(String hashId, ExtendBufferedWriter[] writers, ExtendBufferedWriter[] offsetWriters,
				long[] writersOffset, Collection<String> files, int bUCKET_SIZE, int blockSize, CountDownLatch latch, String[] identities) {
			super();
			this.latch = latch;
			this.hashId = hashId;
			this.writers = writers;
			this.offSetwriters = offsetWriters;
			this.files = files;
			this.writersOffset = writersOffset;
			BUCKET_SIZE = bUCKET_SIZE;
			BLOCK_SIZE = blockSize;
			this.identities = identities;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			for(String orderFile : this.files) {
				Row kvMap;
				KV orderKV;
				int index;
				ExtendBufferedWriter bw;
				ExtendBufferedWriter offsetBw;
				long offset;
				long length;
				try (ExtendBufferedReader reader = IOUtils.createReader(orderFile, BLOCK_SIZE)) {
					String line = reader.readLine();
					while (line != null) {
						StringBuilder offSetMsg = new StringBuilder();
						kvMap = createKVMapFromLine(line);
						// windows测试 提交的时候修改为1
						length = line.getBytes().length;
						// orderId一定存在且为long
						orderKV = kvMap.getKV(hashId);
						index = indexFor(
								hashWithDistrub(hashId.equals("orderid") ? orderKV.longValue : orderKV.rawValue),
								BUCKET_SIZE);
						// 获得对应的writer
						bw = writers[index];
						offsetBw = offSetwriters[index];
						
						// 计算offSet并回写
						offset = writersOffset[index]; 

						
						// index file的写入 具体为identifier:offSet
						// 多个identifier采用拼接
						for(String e : identities) {
							offSetMsg.append(kvMap.getKV(e).rawValue) ;
						}
						offSetMsg.append(":");
						offSetMsg.append(offset);
						// 写入对应的索引文件 此处不换行
						offsetBw.write(offSetMsg.toString() + "\t");
						
						// 此处表示下一个offSet的开始 所以放到后面
						offset += (length + 1);
						writersOffset[index] = offset;
						bw.write(line);
						bw.newLine();

						line = reader.readLine();
					}
					
				} catch (IOException e) {
					// 忽略
				}
			}
			this.latch.countDown();
		}
	}

	/**
	 * 代表一行的数据 其中对Row进行了再次的封装(包含了orderid和真正的long的Row)，其中的key为orderId
	 * 
	 * @author immortalCockRoach
	 *
	 */
	private static class ResultImpl implements Result {
		private long orderid;
		private Row kvMap;

		private ResultImpl(long orderid, Row kv) {
			this.orderid = orderid;
			this.kvMap = kv;
		}

		static private ResultImpl createResultRow(Row orderData, Row buyerData, Row goodData,
				Set<String> queryingKeys) {
			if (orderData == null || buyerData == null || goodData == null) {
				throw new RuntimeException("Bad data!");
			}
			Row allkv = new Row();
			long orderid;
			try {
				orderid = orderData.get("orderid").valueAsLong();
			} catch (TypeException e) {
				throw new RuntimeException("Bad data!");
			}

			for (KV kv : orderData.values()) {
				if (queryingKeys == null || queryingKeys.contains(kv.key)) {
					allkv.put(kv.key(), kv);
				}
			}
			for (KV kv : buyerData.values()) {
				if (queryingKeys == null || queryingKeys.contains(kv.key)) {
					allkv.put(kv.key(), kv);
				}
			}
			for (KV kv : goodData.values()) {
				if (queryingKeys == null || queryingKeys.contains(kv.key)) {
					allkv.put(kv.key(), kv);
				}
			}
			return new ResultImpl(orderid, allkv);
		}

		public KeyValue get(String key) {
			return this.kvMap.get(key);
		}

		public KeyValue[] getAll() {
			return kvMap.values().toArray(new KeyValue[0]);
		}

		public long orderId() {
			return orderid;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("orderid: " + orderid + " {");
			if (kvMap != null && !kvMap.isEmpty()) {
				for (KV kv : kvMap.values()) {
					sb.append(kv.toString());
					sb.append(",\n");
				}
			}
			sb.append('}');
			return sb.toString();
		}
	}

	/**
	 * TreeMap排序时候需要的comparable类,排序的时候需要根据orderingKeys中的顺序去取row中取对应的KV
	 * 
	 * @author immortalCockRoach
	 *
	 */
	static private class ComparableKeys implements Comparable<ComparableKeys> {
		List<String> orderingKeys;
		Row row;

		private ComparableKeys(List<String> orderingKeys, Row row) {
			if (orderingKeys == null || orderingKeys.size() == 0) {
				throw new RuntimeException("Bad ordering keys, there is a bug maybe");
			}
			this.orderingKeys = orderingKeys;
			this.row = row;
		}

		public int compareTo(ComparableKeys o) {
			if (this.orderingKeys.size() != o.orderingKeys.size()) {
				throw new RuntimeException("Bad ordering keys, there is a bug maybe");
			}
			for (String key : orderingKeys) {
				KV a = this.row.get(key);
				KV b = o.row.get(key);
				if (a == null || b == null) {
					throw new RuntimeException("Bad input data: " + key);
				}
				int ret = a.compareTo(b);
				if (ret != 0) {
					return ret;
				}
			}
			return 0;
		}
	}

	/**
	 * 根据参数新建新建文件 目录等操作
	 */
	public OrderSystemImpl() {
//		query1Lock = new ReentrantLock();
//		query2Lock = new ReentrantLock();
//		query3Lock = new ReentrantLock();
//		query4Lock = new ReentrantLock();
		this.query1Offset = new long[CommonConstants.ORDER_SPLIT_SIZE];
		this.query2Offset = new long[CommonConstants.ORDER_SPLIT_SIZE];
		this.query3Offset = new long[CommonConstants.ORDER_SPLIT_SIZE];
		
		this.buyersOffset =  new long[CommonConstants.OTHER_SPLIT_SIZE];
		this.goodsOffset =  new long[CommonConstants.OTHER_SPLIT_SIZE];
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		// init order system
		List<String> orderFiles = new ArrayList<String>();
		List<String> buyerFiles = new ArrayList<String>();

		List<String> goodFiles = new ArrayList<String>();
		List<String> storeFolders = new ArrayList<String>();

		orderFiles.add("prerun_data\\order.0.0");
		orderFiles.add("prerun_data\\order.1.1");
		orderFiles.add("prerun_data\\order.2.2");
		orderFiles.add("prerun_data\\order.0.3");
		buyerFiles.add("prerun_data\\buyer.0.0");
		buyerFiles.add("prerun_data\\buyer.1.1");
		goodFiles.add("prerun_data\\good.0.0");
		goodFiles.add("prerun_data\\good.1.1");
		goodFiles.add("prerun_data\\good.2.2");
		storeFolders.add("./");

		storeFolders.add("./data");
		OrderSystem os = new OrderSystemImpl();
		os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

		// 用例
		long start = System.currentTimeMillis();
		long orderid = 609670049;
		System.out.println("\n查询订单号为" + orderid + "的订单");
		List<String> keys = new ArrayList<>();
		keys.add("description");
		System.out.println(os.queryOrder(orderid, keys));
		System.out.println(System.currentTimeMillis()-start);
//		System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
//		System.out.println(os.queryOrder(orderid, new ArrayList<String>()));

//		System.out.println("\n查询订单号为" + orderid + "的订单的contactphone, buyerid,foo, done, price字段");
//		List<String> queryingKeys = new ArrayList<String>();
//		queryingKeys.add("contactphone");
//		queryingKeys.add("buyerid");
//		queryingKeys.add("foo");
//		queryingKeys.add("done");
//		queryingKeys.add("price");
//		Result result = os.queryOrder(orderid, queryingKeys);
//		System.out.println(result);
//		System.out.println("\n查询订单号不存在的订单");
//		result = os.queryOrder(1111, queryingKeys);
//		if (result == null) {
//			System.out.println(1111 + " order not exist");
//		}
//		System.out.println(System.currentTimeMillis() - start);
//		long start = System.currentTimeMillis();
//		String buyerid = "wx-a0e0-6bda77db73ca";
//		long startTime = 1462018520;
//		long endTime = 1473999229;
//		
//		Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
//		System.out.println("time:"+(System.currentTimeMillis() - start));
//		System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
//		while (it.hasNext()) {
//			System.out.println(it.next());
//		}
		
		//
//		String goodid = "gd-b972-6926df8128c3";
//		String salerid = "almm-b250-b1880d628b9a";
//		System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
//		List<String> keys = new ArrayList<>();
//		keys.add("a_g_32587");
//		keys.add("a_o_30709");
//		long start = System.currentTimeMillis();
//		Iterator it = os.queryOrdersBySaler(salerid, goodid, keys);
//		System.out.println("time:"+(System.currentTimeMillis()-start));
//		while (it.hasNext()) {
//			System.out.println(it.next());
//		}
		//
//		long start = System.currentTimeMillis();
//		String goodid = "al-9c4c-ac9ed4b6ad35";
//		String attr = "offprice";
//		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
//		System.out.println(os.sumOrdersByGood(goodid, attr));
//		System.out.println(System.currentTimeMillis() -start);
//		String goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
//		String attr = "app_order_33_0";
//		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
//		System.out.println(os.sumOrdersByGood(goodid, attr));
//
//		attr = "done";
//		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
//		KeyValue sum = os.sumOrdersByGood(goodid, attr);
//		if (sum == null) {
//			System.out.println("由于该字段是布尔类型，返回值是null");
//		}
//
//		attr = "foo";
//		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
//		sum = os.sumOrdersByGood(goodid, attr);
//		if (sum == null) {
//			System.out.println("由于该字段不存在，返回值是null");
//		}
	}



	/**
	 * 将一行的数据按照"\t"分割并使用":"构建成map
	 * 
	 * @param line
	 * @return
	 */
	Row createKVMapFromLine(String line) {
		String[] kvs = line.split("\t");
		Row kvMap = new Row();
		for (String rawkv : kvs) {
			int p = rawkv.indexOf(':');
			String key = rawkv.substring(0, p);
			String value = rawkv.substring(p + 1);
			if (key.length() == 0 || value.length() == 0) {
				System.out.println(line);
				throw new RuntimeException("Bad data:" + line);
			}
			KV kv = new KV(key, value);
			kvMap.put(kv.key(), kv);
		}
		return kvMap;
	}
	
	HashMap<String,String> createMapFromLongLine(String longLine) {
		String[] kvs = longLine.split("\t");
		HashMap<String,String> result = new HashMap<>(1024);
		for (String rawkv : kvs) {
			int p = rawkv.indexOf(':');
			String key = rawkv.substring(0, p);
			String value = rawkv.substring(p + 1);
			result.put(key, value);
		}
		
		return result;
	}
	
	List<Long> createListFromLongLine(String longLine, String k) {
		String[] kvs = longLine.split("\t");
		List<Long> result = new ArrayList<>(1024);
		for (String rawkv : kvs) {
			int p = rawkv.indexOf(':');
			String key = rawkv.substring(0, p);
			if (k.equals(key)) {
				result.add(Long.parseLong(rawkv.substring(p + 1)));
			}	
		}
		
		return result;
	}

//	private abstract class DataFileHandler {
//		abstract void handleRow(Row row);
//
//		void handle(Collection<String> files) throws IOException {
//			for (String file : files) {
//				BufferedReader bfr = IOUtils.createReader(file);
//				try {
//					String line = bfr.readLine();
//					while (line != null) {
//						Row kvMap = createKVMapFromLine(line);
//						handleRow(kvMap);
//						line = bfr.readLine();
//					}
//				} finally {
//					bfr.close();
//				}
//			}
//		}
//	}

	public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException, InterruptedException {
		System.out.println("orders:");
		for(String s:orderFiles) {
			System.out.println(s);
		}
		System.out.println("buyers:");
		for(String s:buyerFiles) {
			System.out.println(s);
		}
		System.out.println("goods:");
		for(String s:goodFiles) {
			System.out.println(s);
		}
		System.out.println("storeFolers:");
		for(String s:storeFolders) {
			System.out.println(s);
		}
		long start = System.currentTimeMillis();
		this.orderFiles = orderFiles;
		this.buyerFiles = buyerFiles;
		this.goodFiles = goodFiles;
		constructDir(storeFolders);
		long dir = System.currentTimeMillis();
		constructWriterForIndexFile();
		long writer = System.currentTimeMillis();
		constructHashIndex();
		long index = System.currentTimeMillis();
		closeWriter();
		long closeWriter = System.currentTimeMillis();
		System.out.println("dir time:"+ (dir-start));
		System.out.println("writer time:"+ (writer-dir));
		System.out.println("index time:"+ (index-writer));
		System.out.println("close time:"+ (closeWriter-index));
		
		System.out.println("construct KO");
		

	}
	private void constructHashIndex() {
		// 5个线程各自完成之后 该函数才能返回
		CountDownLatch latch = new CountDownLatch(5);
		new Thread(new HashIndexCreator("orderid", query1Writers,query1IndexWriters, query1Offset,orderFiles, CommonConstants.ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch,new String[]{"orderid"})).start();
		new Thread(new HashIndexCreator("buyerid", query2Writers,query2IndexWriters, query2Offset, orderFiles, CommonConstants.ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch,new String[]{"buyerid","createtime"})).start();
		new Thread(new HashIndexCreator("goodid", query3Writers, query3IndexWriters, query3Offset ,orderFiles, CommonConstants.ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch, new String[]{"goodid"})).start();
		// new Thread(new HashIndexCreator("goodid", query4Writers, orderFiles,
		// CommonConstants.ORDER_SPLIT_SIZE,latch)).start();

		
//		try {
//			latch.await();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		latch = new CountDownLatch(2);
		new Thread(new HashIndexCreator("buyerid", buyersWriters, buyersIndexWriters, buyersOffset ,buyerFiles, CommonConstants.OTHER_SPLIT_SIZE,
				CommonConstants.OTHERFILE_BLOCK_SIZE, latch, new String[]{"buyerid"})).start();
		new Thread(new HashIndexCreator("goodid", goodsWriters, goodsIndexWriters, goodsOffset, goodFiles, CommonConstants.OTHER_SPLIT_SIZE,
				CommonConstants.OTHERFILE_BLOCK_SIZE, latch, new String[]{"goodid"})).start();
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private int hashWithDistrub(Object k) {
//	        int h = 0;
//	        h ^= k.hashCode();
//
//	        // This function ensures that hashCodes that differ only by
//	        // constant multiples at each bit position have a bounded
//	        // number of collisions (approximately 8 at default load factor).
//	        h ^= (h >>> 20) ^ (h >>> 12);
//	        return h ^ (h >>> 7) ^ (h >>> 4);
		return k.hashCode();
	    
	}
	
	private int indexFor(int hashCode,int length) {
		return hashCode & (length - 1);
	}
	/**
	 * 创建构造索引的writer
	 */
	private void constructWriterForIndexFile() {
		// 创建4种查询的4中索引文件和买家 商品信息的writer
		this.query1Writers = new ExtendBufferedWriter[CommonConstants.ORDER_SPLIT_SIZE];
		this.query1IndexWriters = new ExtendBufferedWriter[CommonConstants.ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.ORDER_SPLIT_SIZE; i++) {
			try {
				query1Writers[i] = IOUtils.createWriter(this.query1Path + File.separator + i, CommonConstants.INDEX_BLOCK_SIZE);
				query1IndexWriters[i] = IOUtils.createWriter(this.query1Path + File.separator + i + CommonConstants.INDEX_SUFFIX, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}
		this.query2Writers = new ExtendBufferedWriter[CommonConstants.ORDER_SPLIT_SIZE];
		this.query2IndexWriters = new ExtendBufferedWriter[CommonConstants.ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.ORDER_SPLIT_SIZE; i++) {
			try {
				query2Writers[i] = IOUtils.createWriter(this.query2Path + File.separator + i, CommonConstants.INDEX_BLOCK_SIZE);
				query2IndexWriters[i] = IOUtils.createWriter(this.query2Path + File.separator + i + CommonConstants.INDEX_SUFFIX, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}
		
		this.query3Writers = new ExtendBufferedWriter[CommonConstants.ORDER_SPLIT_SIZE];
		this.query3IndexWriters = new ExtendBufferedWriter[CommonConstants.ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.ORDER_SPLIT_SIZE; i++) {
			try {
				query3Writers[i] = IOUtils.createWriter(this.query3Path + File.separator + i, CommonConstants.INDEX_BLOCK_SIZE);
				query3IndexWriters[i] = IOUtils.createWriter(this.query3Path + File.separator + i + CommonConstants.INDEX_SUFFIX, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}

//		this.query4Writers = new BufferedWriter[CommonConstants.ORDER_SPLIT_SIZE];
//		for (int i = 0; i < CommonConstants.ORDER_SPLIT_SIZE; i++) {
//			try {
//				query4Writers[i] = IOUtils.createWriter(this.query4Path + File.separator + i);
//			} catch (IOException e) {
//
//			}
//		}
		
		this.buyersWriters = new ExtendBufferedWriter[CommonConstants.OTHER_SPLIT_SIZE];
		this.buyersIndexWriters = new ExtendBufferedWriter[CommonConstants.OTHER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.OTHER_SPLIT_SIZE; i++) {
			try {
				buyersWriters[i] = IOUtils.createWriter(this.buyersPath + File.separator + i, CommonConstants.INDEX_BLOCK_SIZE);
				buyersIndexWriters[i] = IOUtils.createWriter(this.buyersPath + File.separator + i+ CommonConstants.INDEX_SUFFIX, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}
		
		this.goodsWriters = new ExtendBufferedWriter[CommonConstants.OTHER_SPLIT_SIZE];
		this.goodsIndexWriters = new ExtendBufferedWriter[CommonConstants.OTHER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.OTHER_SPLIT_SIZE; i++) {
			try {
				goodsWriters[i] = IOUtils.createWriter(this.goodsPath + File.separator + i, CommonConstants.INDEX_BLOCK_SIZE);
				goodsIndexWriters[i] = IOUtils.createWriter(this.goodsPath + File.separator + i + CommonConstants.INDEX_SUFFIX, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}

	}
	
	/**
	 * 索引创建目录
	 * @param storeFolders
	 */
	private void constructDir(Collection<String> storeFolders) {
		List<String> storeFoldersList = new ArrayList<>(storeFolders);
		
		// 4种查询的4种索引文件和买家、商品信息平均分到不同的路径上
		int len = storeFoldersList.size();
		int storeIndex = 0;

		this.query1Path = storeFoldersList.get(storeIndex) + File.separator + CommonConstants.QUERY1_PREFIX;
		File query1File = new File(query1Path);
		if (!query1File.exists()) {
			query1File.mkdirs();
		}
		storeIndex++;
		storeIndex %= len;

		this.query2Path = storeFoldersList.get(storeIndex) + File.separator + CommonConstants.QUERY2_PREFIX;
		File query2File = new File(query2Path);
		if (!query2File.exists()) {
			query2File.mkdirs();
		}
		storeIndex++;
		storeIndex %= len;

		this.query3Path = storeFoldersList.get(storeIndex) + File.separator + CommonConstants.QUERY3_PREFIX;
		File query3File = new File(query3Path);
		if (!query3File.exists()) {
			query3File.mkdirs();
		}
		storeIndex++;
		storeIndex %= len;

//		this.query4Path = storeFoldersList.get(storeIndex) + File.separator + CommonConstants.QUERY4_PREFIX;
//		File query4File = new File(query4Path);
//		if (!query4File.exists()) {
//			query4File.mkdirs();
//		}
//		storeIndex++;
//		storeIndex %= len;
		this.buyersPath = storeFoldersList.get(storeIndex) + File.separator + CommonConstants.BUYERS_PREFIX;
		File buyersFile = new File(buyersPath);
		if (!buyersFile.exists()) {
			buyersFile.mkdirs();
		}
		storeIndex++;
		storeIndex %= len;
		System.out.println(storeFoldersList.get(storeIndex)+",index:"+storeIndex);
		this.goodsPath = storeFoldersList.get(storeIndex) + File.separator + CommonConstants.GOODS_PREFIX;
		File goodsFile = new File(goodsPath);
		if (!goodsFile.exists()) {
			goodsFile.mkdirs();
		}
	}

	public Result queryOrder(long orderId, Collection<String> keys) {
		Row query = new Row();
		query.putKV("orderid", orderId);

		// Row orderData = orderDataSortedByOrder.get(new ComparableKeys(
		// comparableKeysOrderingByOrderId, query));
		// if (orderData == null) {
		// return null;
		// }
		Row orderData = null;
		int index = indexFor(hashWithDistrub(orderId), CommonConstants.ORDER_SPLIT_SIZE);
		
		String indexFile = this.query1Path + File.separator + index + CommonConstants.INDEX_SUFFIX;
		String orderFile = this.query1Path + File.separator + index;
//		query1Lock.lock();
		HashMap<String,String> indexMap = null;
		try(ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			indexMap = createMapFromLongLine(indexFileReader.readLine());
			String sOrderId = String.valueOf(orderId);
			// 查询不存在的orderId时直接返回
			if (!indexMap.containsKey(sOrderId)) {
				return null;
			}
			long offset = Long.parseLong(indexMap.get(sOrderId));
			try (RandomAccessFile orderFileReader = new RandomAccessFile(orderFile, "r")) {
				orderFileReader.seek(offset);
				String line = StringUtils.convertISOToUTF8(orderFileReader.readLine());
//				System.out.println(new String(line.getBytes("ISO-8859-1"), "UTF-8"));
				orderData = createKVMapFromLine(line);
				
			} catch (IOException e) {
				// 忽略
			} 
		} catch(IOException e) {
			
		}
		

//		finally {
//			query1Lock.unlock();
//		}
		if (orderData == null) {
			return null;
		}

		return createResultFromOrderData(orderData, createQueryKeys(keys));
	}
	
	private void closeWriter() {
		try {
			for (ExtendBufferedWriter bw : query1Writers) {
				bw.close();
			}
			for (ExtendBufferedWriter bw : query1IndexWriters) {
				bw.close();
			}
			for (ExtendBufferedWriter bw : query2Writers) {
				bw.close();
			}
			for (ExtendBufferedWriter bw : query2IndexWriters) {
				bw.close();
			}
			for (ExtendBufferedWriter bw : query3Writers) {
				bw.close();
			}
			for (ExtendBufferedWriter bw : query3IndexWriters) {
				bw.close();
			}
//			for (BufferedWriter bw : query4Writers) {
//				bw.close();
//			}
			
			for (ExtendBufferedWriter bw : buyersWriters) {
				bw.close();
			}
			for (ExtendBufferedWriter bw : buyersIndexWriters) {
				bw.close();
			}
			
			for (ExtendBufferedWriter bw : goodsWriters) {
				bw.close();
			}
			for (ExtendBufferedWriter bw : goodsIndexWriters) {
				bw.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * join操作，根据order订单中的buyerid和goodid进行join
	 * 
	 * @param orderData
	 * @param keys
	 * @return
	 */
	private ResultImpl createResultFromOrderData(Row orderData, Collection<String> keys) {
		Row buyerQuery = new Row(orderData.getKV("buyerid"));
		// Row buyerData = buyerDataStoredByBuyer.get(new
		// ComparableKeys(comparableKeysOrderingByBuyer, buyerQuery));
		Row buyerData = null;
		
		String buyerId = buyerQuery.getKV("buyerid").rawValue;
		int index = indexFor(hashWithDistrub(buyerId), CommonConstants.OTHER_SPLIT_SIZE);
		String buyerFile = this.buyersPath + File.separator + index;
		String buyerIndexFile = this.buyersPath + File.separator + index + CommonConstants.INDEX_SUFFIX;
		HashMap<String,String> indexMap = null;
		try(ExtendBufferedReader indexFileReader = IOUtils.createReader(buyerIndexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			indexMap = createMapFromLongLine(indexFileReader.readLine());
			long offset = Long.parseLong(indexMap.get(buyerId));
			try (RandomAccessFile buyerFileReader = new RandomAccessFile(buyerFile, "r")) {
				buyerFileReader.seek(offset);
				String line = StringUtils.convertISOToUTF8(buyerFileReader.readLine());
				buyerData = createKVMapFromLine(line);

			} catch (IOException e) {
				// 忽略
			} 
		} catch(IOException e) {
			
		}


		Row goodQuery = new Row(orderData.getKV("goodid"));
		// Row goodData = goodDataStoredByGood.get(new
		// ComparableKeys(comparableKeysOrderingByGood, goodQuery));
		Row goodData = null;
		String goodId = goodQuery.getKV("goodid").rawValue;
		index = indexFor(hashWithDistrub(goodId), CommonConstants.OTHER_SPLIT_SIZE);
		String goodFile = this.goodsPath + File.separator + index;
		String goodIndexFile = this.goodsPath + File.separator + index + CommonConstants.INDEX_SUFFIX;
		try(ExtendBufferedReader indexFileReader = IOUtils.createReader(goodIndexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			indexMap = createMapFromLongLine(indexFileReader.readLine());
			long offset = Long.parseLong(indexMap.get(goodId));
			try (RandomAccessFile goodFileReader = new RandomAccessFile(goodFile, "r")) {
				goodFileReader.seek(offset);
				String line = StringUtils.convertISOToUTF8(goodFileReader.readLine());;
				goodData = createKVMapFromLine(line);

			} catch (IOException e) {
				// 忽略
			} 
		} catch(IOException e) {
			
		}
		
		return ResultImpl.createResultRow(orderData, buyerData, goodData, createQueryKeys(keys));
	}

	private HashSet<String> createQueryKeys(Collection<String> keys) {
		if (keys == null) {
			return null;
		}
		return new HashSet<String>(keys);
	}

	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {

		final PriorityQueue<Row> buyerOrderQueue = new PriorityQueue<>(1000, new Comparator<Row>() {

			@Override
			public int compare(Row o1, Row o2) {
				// TODO Auto-generated method stub
				long o2Time;
				long o1Time;
				o1Time = o1.get("createtime").longValue;
				o2Time = o2.get("createtime").longValue;
				return o2Time - o1Time > 0 ? 1 : -1;
			}

		});
		int index = indexFor(hashWithDistrub(buyerid), CommonConstants.ORDER_SPLIT_SIZE);
//		System.out.println(index);
		String indexFile = this.query2Path + File.separator + index + CommonConstants.INDEX_SUFFIX;
		String orderFile = this.query2Path + File.separator + index;
//		query2Lock.lock();
		HashMap<String,String> indexMap = null;
		ArrayList<Long> recordOffSets = new ArrayList<>(1024);
		String start = buyerid + startTime;
		String end = buyerid + endTime;
//		System.out.println(recordOffSets.size());
//		System.out.println(indexFile);
		try(ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			indexMap = createMapFromLongLine(indexFileReader.readLine());
			for (Map.Entry<String, String> e : indexMap.entrySet()) {
				if (e.getKey().compareTo(start) >= 0 && e.getKey().compareTo(end) < 0) {
					recordOffSets.add(Long.parseLong(e.getValue()));
				}
			}
			
			if(recordOffSets.size() > 0) {
//			System.out.println(recordOffSets.size());
				Row kvMap;
				try (RandomAccessFile orderFileReader = new RandomAccessFile(orderFile, "r")) {
					for (long offset : recordOffSets) {
						orderFileReader.seek(offset);
						String line = StringUtils.convertISOToUTF8(orderFileReader.readLine());
		//				System.out.println(new String(line.getBytes("ISO-8859-1"), "UTF-8"));
						kvMap = createKVMapFromLine(line);
						buyerOrderQueue.offer(kvMap);
					}
					
				} catch (IOException e) {
					// 忽略
				} 
			}
		} catch(IOException e) {
			
		}
		
		return new Iterator<OrderSystem.Result>() {

			PriorityQueue<Row> o = buyerOrderQueue;
			//TreeMap<Long, Row> o = buyerOrderMap;

			public boolean hasNext() {
				return o != null && o.size() > 0;
			}

			public Result next() {
				if (!hasNext()) {
					return null;
				}
				Row orderData = buyerOrderQueue.poll();
				
				return createResultFromOrderData(orderData, null);
			}

			public void remove() {

			}
		};
	}

	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
		final Collection<String> queryKeys = keys;

		final PriorityQueue<Row> salerGoodsQueue = new PriorityQueue<>(1000, new Comparator<Row>() {

			@Override
			public int compare(Row o1, Row o2) {
				// TODO Auto-generated method stub
				long o2Time;
				long o1Time;
				o1Time = o1.get("orderid").longValue;
				o2Time = o2.get("orderid").longValue;
				return o1Time - o2Time > 0 ? 1 : -1;
			}

		});
		
		int index = indexFor(hashWithDistrub(goodid), CommonConstants.ORDER_SPLIT_SIZE);
		String indexFile = this.query3Path + File.separator + index + CommonConstants.INDEX_SUFFIX;
		String orderFile = this.query3Path + File.separator + index;
//		query3Lock.lock();
//		System.out.println("index:" + index);
		
		List<Long> recordOffSets = new ArrayList<>(1024);
		try(ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			recordOffSets = createListFromLongLine(indexFileReader.readLine(), goodid);

			if (recordOffSets.size() > 0) {
				Row kvMap;
				try (RandomAccessFile orderFileReader = new RandomAccessFile(orderFile, "r")) {
					for (long offset : recordOffSets) {
						orderFileReader.seek(offset);
						String line = StringUtils.convertISOToUTF8(orderFileReader.readLine());
		//				System.out.println(new String(line.getBytes("ISO-8859-1"), "UTF-8"));
						kvMap = createKVMapFromLine(line);
						salerGoodsQueue.offer(kvMap);
					}
					
				} catch (IOException e) {
					// 忽略
				} 
			}
		} catch(IOException e) {
			
		}
//		finally {
//			query3Lock.unlock();
//		}
		

		return new Iterator<OrderSystem.Result>() {

			final PriorityQueue<Row> o = salerGoodsQueue;

			public boolean hasNext() {
				return o != null && o.size() > 0;
			}

			public Result next() {
				if (!hasNext()) {
					return null;
				}
				Row orderData = o.poll();
				return createResultFromOrderData(orderData, createQueryKeys(queryKeys));
			}

			public void remove() {
				// ignore
			}
		};
	}

	public KeyValue sumOrdersByGood(String goodid, String key) {
		List<Row> ordersData = new ArrayList<>(1000);
		int index = indexFor(hashWithDistrub(goodid), CommonConstants.ORDER_SPLIT_SIZE);
		String orderFile = this.query3Path + File.separator + index;
		String indexFile = this.query3Path + File.separator + index + CommonConstants.INDEX_SUFFIX;
//		query4Lock.lock();
		List<Long> recordOffSets = new ArrayList<>(1024);
		try(ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			recordOffSets = createListFromLongLine(indexFileReader.readLine(), goodid);

			if(recordOffSets.size() > 0) {
				Row kvMap;
				try (RandomAccessFile orderFileReader = new RandomAccessFile(orderFile, "r")) {
					for (long offset : recordOffSets) {
						orderFileReader.seek(offset);
						String line = StringUtils.convertISOToUTF8(orderFileReader.readLine());
		//				System.out.println(new String(line.getBytes("ISO-8859-1"), "UTF-8"));
						kvMap = createKVMapFromLine(line);
						ordersData.add(kvMap);
					}
					
				} catch (IOException e) {
					// 忽略
				}
			}
		} catch(IOException e) {
			
		}
//		finally {
//			query4Lock.unlock();
//		}
		
		
		HashSet<String> queryingKeys = new HashSet<String>();
		queryingKeys.add(key);
		List<ResultImpl> allData = new ArrayList<ResultImpl>(ordersData.size());
		for (Row orderData : ordersData) {
			allData.add(createResultFromOrderData(orderData, queryingKeys));
		}

		// accumulate as Long
		try {
			boolean hasValidData = false;
			long sum = 0;
			for (ResultImpl r : allData) {
				KeyValue kv = r.get(key);
				if (kv != null) {
					sum += kv.valueAsLong();
					hasValidData = true;
				}
			}
			if (hasValidData) {
				return new KV(key, Long.toString(sum));
			}
		} catch (TypeException e) {
		}

		// accumulate as double
		try {
			boolean hasValidData = false;
			double sum = 0;
			for (ResultImpl r : allData) {
				KeyValue kv = r.get(key);
				if (kv != null) {
					sum += kv.valueAsDouble();
					hasValidData = true;
				}
			}
			if (hasValidData) {
				return new KV(key, Double.toString(sum));
			}
		} catch (TypeException e) {
		}

		return null;
	}
}
