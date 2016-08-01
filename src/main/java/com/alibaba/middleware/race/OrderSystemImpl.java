package com.alibaba.middleware.race;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedReader;
import com.alibaba.middleware.race.utils.ExtendBufferedWriter;
import com.alibaba.middleware.race.utils.IOUtils;
import com.alibaba.middleware.race.utils.IndexBuffer;
import com.alibaba.middleware.race.utils.SimpleLRUCache;
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
	
	private List<String> orderFiles;
	private List<String> goodFiles;
	private List<String> buyerFiles;
	
	private String query1Path;
	private String query2Path;
	private String query3Path;
//	private String query4Path;
	
	private String buyersPath;
	private String goodsPath;
	
	/**
	 * 这个数据记录每个文件的行当前写入的record数量，当大于INDEX_LINE_RECORDS的时候，换行
	 */
	private int[] query1LineRecords;
	private ExtendBufferedWriter[] query1IndexWriters;
//	private SimpleLRUCache<Long, String> query1Cache;
	
	
	private int[] query2LineRecords;
	private ExtendBufferedWriter[] query2IndexWriters;
//	private SimpleLRUCache<String, Map<Long,String>> query2Cache;
//	private SimpleLRUCache<String, List<Long>> query2Cache;
	
	
	private int[] query3LineRecords;
	private ExtendBufferedWriter[] query3IndexWriters;
//	private SimpleLRUCache<String, List<String>> query3Cache;
//	private BufferedWriter[] query4Writers;
//	private SimpleLRUCache<String, List<String>> query4Cache;
	
	private int[] buyerLineRecords;
	private ExtendBufferedWriter[] buyersIndexWriters;
	private HashMap<String,String> buyerMemoryIndexMap;
	private SimpleLRUCache<String, String> buyersCache;
	
	private int[] goodLineRecords;
	private ExtendBufferedWriter[] goodsIndexWriters;
	private SimpleLRUCache<String, String> goodsCache;
	private HashMap<String,String> goodMemoryIndexMap; 
	private AtomicInteger query1Count;
	private AtomicInteger query2Count;
	private AtomicInteger query3Count;
	private AtomicInteger query4Count;
	
//	private AtomicInteger q1CacheHit;
//	private AtomicInteger q2CacheHit;
//	private AtomicInteger q3CacheHit;
//	private AtomicInteger q4CacheHit;
	private AtomicInteger buyerCacheHit;
	private AtomicInteger goodCacheHit;
	
	private volatile boolean isConstructed;
	
	private boolean buyerGoodInMemory;

	/**
	 * KeyValue的实现类，代表一行中的某个key-value对 raw数据采用String来存储 之后根据情况返回对应的long获得double
	 * 
	 * @author immortalCockRoach
	 *
	 */
	public static class KV implements Comparable<KV>, KeyValue {
		String key;
		String rawValue;

		boolean isComparableLong = false;
		long longValue;

		 public KV(String key, String rawValue) {
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
	public static class Row extends HashMap<String, KV> {
		public Row() {
			super();
		}

		public Row(KV kv) {
			super();
			this.put(kv.key(), kv);
		}

		public KV getKV(String key) {
			KV kv = this.get(key);
			if (kv == null) {
				throw new RuntimeException(key + " is not exist");
			}
			return kv;
		}
		
		public KV removeKV(String key){
			KV kv = this.remove(key);
			return kv;
		}

		public Row putKV(String key, String value) {
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

		private ExtendBufferedWriter[] offSetwriters;
		private int[] indexLineRecords;
		private Collection<String> files;
		private CountDownLatch latch;
		private final int BUCKET_SIZE;
		private final int BLOCK_SIZE;
		private String[] identities;
		private int buildCount;
		private int mod;
		private IndexBuffer[] bufferArray;
		private boolean inMemory;
		
		public HashIndexCreator(String hashId, ExtendBufferedWriter[] offsetWriters,
				int[] indexLineRecords, Collection<String> files, int bUCKET_SIZE, int blockSize, CountDownLatch latch, String[] identities, boolean inMemory) {
			super();
			this.latch = latch;
			this.hashId = hashId;
			this.offSetwriters = offsetWriters;
			this.files = files;
			this.indexLineRecords = indexLineRecords;
			BUCKET_SIZE = bUCKET_SIZE;
			BLOCK_SIZE = blockSize;
			this.identities = identities;
			this.buildCount = 0;
			this.mod = 524288;
			this.bufferArray = new IndexBuffer[CommonConstants.INDEX_BUFFER_SIZE];
			this.inMemory = inMemory;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			int fileIndex = 0;
			for(String orderFile : this.files) {
				Row kvMap = null;
				KV orderKV = null;
				int index;
				ExtendBufferedWriter offsetBw;
				// 记录当前行的偏移
				long offset = 0L;
				// 记录当前行的总长度
				int length = 0;
				int readLines;
				try (ExtendBufferedReader reader = IOUtils.createReader(orderFile, BLOCK_SIZE)) {
					String line;
					while (true) {
						for (readLines = 0; readLines < CommonConstants.INDEX_BUFFER_SIZE; readLines++) {
							line = reader.readLine();
							if (line == null) {
								break;
							}
							StringBuilder offSetMsg = new StringBuilder();
							kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
							length = line.getBytes().length;
							
							// orderId一定存在且为long
							orderKV = kvMap.getKV(hashId);
							if (!inMemory) {
								index = indexFor(
										hashWithDistrub(hashId.equals("orderid") ? orderKV.longValue : orderKV.rawValue),
										BUCKET_SIZE);
								
								for(String e : identities) {
									offSetMsg.append(kvMap.getKV(e).rawValue) ;
								}
								offSetMsg.append(':');
								// 写入文件的index
								offSetMsg.append(fileIndex);
								offSetMsg.append(' ');
								offSetMsg.append(offset);
								offSetMsg.append(' ');
								offSetMsg.append(length);
								
								// 将对应index文件的行记录数++ 如果超过阈值则换行并清空
	
								this.indexLineRecords[index]++;
								if ( this.indexLineRecords[index] == CommonConstants.INDEX_LINE_RECORDS) {
									offSetMsg.append('\n');
									this.indexLineRecords[index] = 0;
								}
								this.bufferArray[readLines] = new IndexBuffer(index, offSetMsg.toString());
							} else {
								offSetMsg.append(fileIndex);
								offSetMsg.append(' ');
								offSetMsg.append(offset);
								offSetMsg.append(' ');
								offSetMsg.append(length);
								this.bufferArray[readLines] = new IndexBuffer(orderKV.rawValue, offSetMsg.toString());
							}
							
							offset += (length + 1);
							
							
							buildCount++;
							if ((buildCount & (mod - 1)) == 0) {
								System.out.println(hashId + "construct:" + buildCount);
							}
						}
						
						if (readLines == 0) {
							break;
						}
						int i = 0;
						if (!this.inMemory) {
							while (i < readLines) {
								
								offsetBw = offSetwriters[(int)(bufferArray[i].getIndex())];
								offsetBw.write(bufferArray[i].getLine());
								i++;
							}
						} else {
							while (i < readLines) {
								if (this.hashId.equals("goodid")) {								
									goodMemoryIndexMap.put((String)bufferArray[i].getIndex(), bufferArray[i].getLine());
								} else {								
									buyerMemoryIndexMap.put((String)bufferArray[i].getIndex(), bufferArray[i].getLine());
								}
								i++;
							}
						}
						
					}	
					fileIndex++;
					
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
		
		/**
		 * 根据order good buyer来构造结果 buyer和good可以为null,相当于不join
		 * 此处buyer和good可以为null是为了查询join的优化
		 * @param orderData
		 * @param buyerData
		 * @param goodData
		 * @param queryingKeys
		 * @return
		 */
		public static ResultImpl createResultRow(Row orderData, Row buyerData, Row goodData,
				Set<String> queryingKeys) {
			
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
			if(buyerData != null) {
				for (KV kv : buyerData.values()) {
					if (queryingKeys == null || queryingKeys.contains(kv.key)) {
						allkv.put(kv.key(), kv);
					}
				}
			}
			
			if (goodData != null) {
				for (KV kv : goodData.values()) {
					if (queryingKeys == null || queryingKeys.contains(kv.key)) {
						allkv.put(kv.key(), kv);
					}
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
	 * 根据参数新建新建文件 目录等操作
	 */
	public OrderSystemImpl() {
//		query1Lock = new ReentrantLock();
//		query2Lock = new ReentrantLock();
//		query3Lock = new ReentrantLock();
//		query4Lock = new ReentrantLock();
//		query1Cache = new SimpleLRUCache<>(32768);
		this.buyerGoodInMemory = true;
		
		this.query1LineRecords = new int[CommonConstants.ORDER_SPLIT_SIZE];
//		query2Cache = new SimpleLRUCache<>(16384);
		this.query2LineRecords = new int[CommonConstants.QUERY2_ORDER_SPLIT_SIZE];
//		query3Cache = new SimpleLRUCache<>(16384);
		this.query3LineRecords = new int[CommonConstants.ORDER_SPLIT_SIZE];
//		query4Cache = new SimpleLRUCache<>(16384);
		
		goodsCache = new SimpleLRUCache<>(65536);
		if (!buyerGoodInMemory) {
			this.goodLineRecords = new int[CommonConstants.OTHER_SPLIT_SIZE];
		} else {
			this.goodMemoryIndexMap = new HashMap<>(4194304, 1f);
		}
		buyersCache = new SimpleLRUCache<>(65536);
		if (!buyerGoodInMemory) {
			this.buyerLineRecords = new int[CommonConstants.OTHER_SPLIT_SIZE];
		}  else {
			this.buyerMemoryIndexMap = new HashMap<>(8388608, 1f);
		}
		isConstructed = false;
		
		query1Count = new AtomicInteger(0);
		query2Count = new AtomicInteger(0);
		query3Count = new AtomicInteger(0);
		query4Count = new AtomicInteger(0);
		
//		q1CacheHit = new AtomicInteger(0);
////		q2CacheHit = new AtomicInteger(0);
//		q3CacheHit = new AtomicInteger(0);
//		q4CacheHit = new AtomicInteger(0);
		buyerCacheHit = new AtomicInteger(0);
		goodCacheHit = new AtomicInteger(0);
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
//		long start = System.currentTimeMillis();
//		long orderid = 609670049;
//		System.out.println("\n查询订单号为" + orderid + "的订单");
//		List<String> keys = new ArrayList<>();
//		keys.add("description");
////		keys.add("buyerid");
//		System.out.println(os.queryOrder(orderid, keys));
//		System.out.println(System.currentTimeMillis()-start);
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
//		String buyerid = "tp-b0a2-fd0ca6720971";
//		long startTime = 1467791748;
//		long endTime = 1481816836;
////		
//		Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
//		
//		System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
//		while (it.hasNext()) {
////			it.next();
//			System.out.println(it.next());
//		}
//		System.out.println("time:"+(System.currentTimeMillis() - start));
//		//


//		String goodid = "aye-8837-3aca358bfad3";
//		String salerid = "almm-b250-b1880d628b9a";
//		System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
//		long start = System.currentTimeMillis();
//		List<String> keys = new ArrayList<>();
//		keys.add("address");
//		Iterator it = os.queryOrdersBySaler(salerid, goodid, keys);
////		System.out.println(System.currentTimeMillis()-start);
//		while (it.hasNext()) {
//			System.out.println(it.next());
////			it.next();
//		}
//		System.out.println(System.currentTimeMillis()-start);
		//
//		long start = System.currentTimeMillis();
//		String goodid = "dd-a27d-835565dfb080";
//		String attr = "a_b_3503";
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
		
		this.orderFiles = new ArrayList<>(orderFiles);
		this.buyerFiles = new ArrayList<>(buyerFiles);
		this.goodFiles = new ArrayList<>(goodFiles);
		long start = System.currentTimeMillis();
		constructDir(storeFolders);
		final long dir = System.currentTimeMillis();
		System.out.println("dir time:" + (dir-start));
		

		
		// 主要对第一阶段的index time进行限制
		final CountDownLatch latch = new CountDownLatch(1);
		new Thread() {
			@Override
			public void run() {
				
				constructWriterForIndexFile();
				long writer = System.currentTimeMillis();
				System.out.println("writer time:" + (writer - dir));
				
				constructHashIndex();
				long index = System.currentTimeMillis();
				System.out.println("index time:" + (index - writer));
				
				
				closeWriter();	
				long closeWriter = System.currentTimeMillis();
				System.out.println("close time:" + (closeWriter - index));
				System.out.println("construct KO");
				latch.countDown();
				isConstructed = true;
			}
		}.start();
		
		latch.await(59 * 60 + 45, TimeUnit.SECONDS);
		System.out.println("construct return,time:" + (System.currentTimeMillis() - start));
		

	}
	private void constructHashIndex() {
		// 5个线程各自完成之后 该函数才能返回
		CountDownLatch latch = new CountDownLatch(5);
		new Thread(new HashIndexCreator("orderid", query1IndexWriters, query1LineRecords,orderFiles, CommonConstants.ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch,new String[]{"orderid"}, false)).start();
		new Thread(new HashIndexCreator("buyerid", query2IndexWriters, query2LineRecords, orderFiles, CommonConstants.QUERY2_ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch,new String[]{"buyerid","createtime"}, false)).start();
		new Thread(new HashIndexCreator("goodid", query3IndexWriters, query3LineRecords ,orderFiles, CommonConstants.ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch, new String[]{"goodid"}, false)).start();
		// new Thread(new HashIndexCreator("goodid", query4Writers, orderFiles,
		// CommonConstants.ORDER_SPLIT_SIZE,latch)).start();

		
//		try {
//			latch.await();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		latch = new CountDownLatch(2);
		new Thread(new HashIndexCreator("buyerid", buyersIndexWriters, buyerLineRecords ,buyerFiles, CommonConstants.OTHER_SPLIT_SIZE,
				CommonConstants.OTHERFILE_BLOCK_SIZE, latch, new String[]{"buyerid"}, true)).start();
		new Thread(new HashIndexCreator("goodid", goodsIndexWriters, goodLineRecords, goodFiles, CommonConstants.OTHER_SPLIT_SIZE,
				CommonConstants.OTHERFILE_BLOCK_SIZE, latch, new String[]{"goodid"}, true)).start();
		
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private int hashWithDistrub(Object k) {
        int h = 0;
        h ^= k.hashCode();

        // This function ensures that hashCodes that differ only by
        // constant multiples at each bit position have a bounded
        // number of collisions (approximately 8 at default load factor).
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
//		return k.hashCode();
	    
	}
	
	private int indexFor(int hashCode,int length) {
		return hashCode & (length - 1);
	}
	/**
	 * 创建构造索引的writer
	 */
	private void constructWriterForIndexFile() {
		// 创建4种查询的4中索引文件和买家 商品信息的writer
		this.query1IndexWriters = new ExtendBufferedWriter[CommonConstants.ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.ORDER_SPLIT_SIZE; i++) {
			try {
				String file = this.query1Path + File.separator + i + CommonConstants.INDEX_SUFFIX;
				RandomAccessFile ran = new RandomAccessFile(file, "rw");
				ran.setLength(1024 * 1024 * 80);
				ran.close();
				query1IndexWriters[i] = IOUtils.createWriter(file, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}

		this.query2IndexWriters = new ExtendBufferedWriter[CommonConstants.QUERY2_ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.QUERY2_ORDER_SPLIT_SIZE; i++) {
			try {
				String file = this.query2Path + File.separator + i + CommonConstants.INDEX_SUFFIX;
				RandomAccessFile ran = new RandomAccessFile(file, "rw");
				ran.setLength(1024 * 1024 * 80);
				ran.close();
				query2IndexWriters[i] = IOUtils.createWriter(file, CommonConstants.INDEX_BLOCK_SIZE);
			} catch (IOException e) {

			}
		}
		
		this.query3IndexWriters = new ExtendBufferedWriter[CommonConstants.ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.ORDER_SPLIT_SIZE; i++) {
			try {
				String file = this.query3Path + File.separator + i + CommonConstants.INDEX_SUFFIX;
				RandomAccessFile ran = new RandomAccessFile(file, "rw");
				ran.setLength(1024 * 1024 * 80);
				ran.close();
				query3IndexWriters[i] = IOUtils.createWriter(file, CommonConstants.INDEX_BLOCK_SIZE);
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
		if (!buyerGoodInMemory) {
			this.buyersIndexWriters = new ExtendBufferedWriter[CommonConstants.OTHER_SPLIT_SIZE];
			for (int i = 0; i < CommonConstants.OTHER_SPLIT_SIZE; i++) {
				try {
					String file = this.buyersPath + File.separator + i+ CommonConstants.INDEX_SUFFIX;
					RandomAccessFile ran = new RandomAccessFile(file, "rw");
					ran.setLength(1024 * 1024 * 10);
					ran.close();
					buyersIndexWriters[i] = IOUtils.createWriter(file, CommonConstants.INDEX_BLOCK_SIZE);
				} catch (IOException e) {
	
				}
			}
		}
		if (!buyerGoodInMemory) {	
			this.goodsIndexWriters = new ExtendBufferedWriter[CommonConstants.OTHER_SPLIT_SIZE];
			for (int i = 0; i < CommonConstants.OTHER_SPLIT_SIZE; i++) {
				try {
					String file = this.goodsPath + File.separator + i + CommonConstants.INDEX_SUFFIX;
					RandomAccessFile ran = new RandomAccessFile(file, "rw");
					ran.setLength(1024 * 1024 * 10);
					ran.close();
					goodsIndexWriters[i] = IOUtils.createWriter(file, CommonConstants.INDEX_BLOCK_SIZE);
				} catch (IOException e) {
	
				}
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
		if (!buyerGoodInMemory) {
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
	}

	public Result queryOrder(long orderId, Collection<String> keys) {
		while (this.isConstructed == false) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		final long start = System.currentTimeMillis();
		int count  = query1Count.incrementAndGet();
		if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query1count:" + query1Count.get());	
		}
		Row query = new Row();
		query.putKV("orderid", orderId);


		Row orderData = null;
		

		int index = indexFor(hashWithDistrub(orderId), CommonConstants.ORDER_SPLIT_SIZE);
		String indexFile = this.query1Path + File.separator + index + CommonConstants.INDEX_SUFFIX;
		
//		query1Lock.lock();
//			HashMap<String,String> indexMap = null;
		String[] indexArray = null;
		try (ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			// 可能index文件就没有
			String sOrderId = String.valueOf(orderId);
			String line = indexFileReader.readLine();
			while (line != null) {

				if (line.startsWith(sOrderId)) {
					int p = line.indexOf(':');
					indexArray = StringUtils.getIndexInfo(line.substring(p + 1));
					break;
				}
				line = indexFileReader.readLine();
				
			}
			if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
				System.out.println("query1 index time:" + (System.currentTimeMillis() - start));
			}
			// 说明文件中没有这个orderId的信息
			if (indexArray == null) {
				System.out.println("query1 can't find order:");
				return null;
			}
			String file = this.orderFiles.get(Integer.parseInt(indexArray[0]));
			Long offset = Long.parseLong(indexArray[1]);
			byte[] content = new byte[Integer.valueOf(indexArray[2])];
			try (RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
				orderFileReader.seek(offset);
				orderFileReader.read(content);
				line = new String(content);
//				System.out.println(new String(line.getBytes("ISO-8859-1"), "UTF-8"));
//					System.out.println("order:"+line);
				orderData = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
//					query1Cache.put(orderId, line);
				
			} catch (IOException e) {
				// 忽略
			}
			if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
				System.out.println("query1 original data time:" + (System.currentTimeMillis() - start));
			}
		} catch(IOException e) {
			
		}
//		}
		

//		finally {
//			query1Lock.unlock();
//		}
//		System.out.println(orderData);
		if (orderData == null) {
			return null;
		}
		ResultImpl result= createResultFromOrderData(orderData, createQueryKeys(keys));
		if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query1 all time:" + (System.currentTimeMillis() - start));
		}
		return result;
	}
	
	private void closeWriter() {
		try {

			for (ExtendBufferedWriter bw : query1IndexWriters) {
				bw.close();
			}

			for (ExtendBufferedWriter bw : query2IndexWriters) {
				bw.close();
			}

			for (ExtendBufferedWriter bw : query3IndexWriters) {
				bw.close();
			}
//			for (BufferedWriter bw : query4Writers) {
//				bw.close();
//			}
			
			if (!buyerGoodInMemory) {
				for (ExtendBufferedWriter bw : buyersIndexWriters) {
					bw.close();
				}
			}
			
			if (!buyerGoodInMemory) {
				for (ExtendBufferedWriter bw : goodsIndexWriters) {
					bw.close();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	private Row getGoodRowFromOrderData(Row orderData) {
		// Row goodData = goodDataStoredByGood.get(new
		// ComparableKeys(comparableKeysOrderingByGood, goodQuery));
		Row goodData = null;
		String goodId = orderData.getKV("goodid").rawValue;
		String cachedString = goodsCache.get(goodId);
		if (cachedString != null) {
			goodData = StringUtils.createKVMapFromLine(cachedString, CommonConstants.SPLITTER);
			if (goodCacheHit.incrementAndGet() % CommonConstants.CACHE_PRINT_COUNT == 0) {
				System.out.println("good cache hit:" + goodCacheHit.get());
			}
		} else {
			String[] indexArray = null;
//			HashMap<String,String> indexMap = null;
			if (!this.buyerGoodInMemory) {
				int index = indexFor(hashWithDistrub(goodId), CommonConstants.OTHER_SPLIT_SIZE);
				String goodIndexFile = this.goodsPath + File.separator + index + CommonConstants.INDEX_SUFFIX;
				try(ExtendBufferedReader indexFileReader = IOUtils.createReader(goodIndexFile, CommonConstants.INDEX_BLOCK_SIZE)){
					String line = indexFileReader.readLine();
					while (line != null) {
						if (line.startsWith(goodId)) {
							int p = line.indexOf(':');
							indexArray = StringUtils.getIndexInfo(line.substring(p + 1));
							break;
						}
						line = indexFileReader.readLine();			
					}
				} catch(IOException e) {
					
				}
			} else {
				String line = this.goodMemoryIndexMap.get(goodId);
//				System.out.println(line);
				if (line != null) {
					indexArray = StringUtils.getIndexInfo(line);
				}
			}
			// 由于现在query4的求和可能直接查good信息，因此此处代表不存在对应的信息
			if (indexArray == null) {
				return null;
			}
			String file = this.goodFiles.get(Integer.parseInt(indexArray[0]));
			Long offset = Long.parseLong(indexArray[1]);
			byte[] content = new byte[Integer.valueOf(indexArray[2])];
			try (RandomAccessFile goodFileReader = new RandomAccessFile(file, "r")) {
				goodFileReader.seek(offset);
				goodFileReader.read(content);
				String line = new String(content);
				goodData = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
				goodsCache.put(goodId, line);
			} catch (IOException e) {
				// 忽略
			} 
		}
		return goodData;
	}
	
	private Row getBuyerRowFromOrderData(Row orderData) {
		Row buyerData = null;
		
		String buyerId = orderData.getKV("buyerid").rawValue;
		String cachedString = buyersCache.get(buyerId);
		if (cachedString != null) {
			buyerData = StringUtils.createKVMapFromLine(cachedString, CommonConstants.SPLITTER);
			if (buyerCacheHit.incrementAndGet() % CommonConstants.CACHE_PRINT_COUNT == 0) {
				System.out.println("buyer cache hit:" + buyerCacheHit.get());
			}
		} else {
			String[] indexArray = null;
			if (!this.buyerGoodInMemory) {
				int index = indexFor(hashWithDistrub(buyerId), CommonConstants.OTHER_SPLIT_SIZE);
				String buyerIndexFile = this.buyersPath + File.separator + index + CommonConstants.INDEX_SUFFIX;
				try (ExtendBufferedReader indexFileReader = IOUtils.createReader(buyerIndexFile, CommonConstants.INDEX_BLOCK_SIZE)){
					String line = indexFileReader.readLine();
					while (line != null) {
						if (line.startsWith(buyerId)) {
							int p = line.indexOf(':');
							indexArray = StringUtils.getIndexInfo(line.substring(p + 1));
							break;
						}
						line = indexFileReader.readLine();			
					}
					// 如果能查到其他信息 则对应的值buyer和order一定存在，此处不需要判断为null
	
				} catch(IOException e) {
					
				}
			} else {
				String line = this.buyerMemoryIndexMap.get(buyerId);
				if (line != null) {
					indexArray = StringUtils.getIndexInfo(line);
				}
			}
			String file = this.buyerFiles.get(Integer.parseInt(indexArray[0]));
			Long offset = Long.parseLong(indexArray[1]);
			byte[] content = new byte[Integer.valueOf(indexArray[2])];
			try (RandomAccessFile buyerFileReader = new RandomAccessFile(file, "r")) {
				buyerFileReader.seek(offset);
				buyerFileReader.read(content);
				String line = new String(content);
				buyerData = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
				buyersCache.put(buyerId, line);

			} catch (IOException e) {
				// 忽略
			} 
		}
		return buyerData;
	}
	/**
	 * join操作，根据order订单中的buyerid和goodid进行join
	 * 
	 * @param orderData
	 * @param keys
	 * @return
	 */
	private ResultImpl createResultFromOrderData(Row orderData, Collection<String> keys) {
		String tag = "all";
		Row buyerData = null;
		Row goodData = null;
		// keys为null或者keys 不止一个的时候和以前的方式一样 all join
		if (keys != null && keys.size() == 1) {
			tag = getKeyJoin((String)keys.toArray()[0]);
		}
//		System.out.println(tag);
		// all的话join两次 buyer或者good join1次 order不join
		if (tag.equals("buyer") || tag.equals("all")) {
			buyerData = getBuyerRowFromOrderData(orderData);
		}
		if (tag.equals("good") || tag.equals("all")) {
			goodData = getGoodRowFromOrderData(orderData);
		}
		return ResultImpl.createResultRow(orderData, buyerData, goodData, createQueryKeys(keys));
	}
	/**
	 * 判断key的join 只对单个key有效果 order代表不join
	 * buyer代表只join buyer表 good表示只join good表
	 * 返回all代表无法判断 全部join
	 * @param key
	 * @return
	 */
	private String getKeyJoin(String key) {
		if(key.startsWith("a_b_")) {
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
		
		if (key.equals("salerid") || key.equals("price") || key.equals("offprice") || key.equals("description") || key.equals("good_name")) {
			return "good";
		}
		
		return "all";
	}

	private HashSet<String> createQueryKeys(Collection<String> keys) {
		if (keys == null) {
			return null;
		}
		return new HashSet<String>(keys);
	}
	
	private Map<String,PriorityQueue<String[]>> createOrderDataAccessSequence(Collection<String> offsetRecords) {
		Map<String,PriorityQueue<String[]>> result = new HashMap<>(512);
		for(String e : offsetRecords) {
			int fileP = e.indexOf(' ');
			String fileIndex = e.substring(0, fileP);
			String offLenS = e.substring(fileP + 1);

			String[] offLenArray = new String[2];
			int offsetP = offLenS.indexOf(' ');
			offLenArray[0] = offLenS.substring(0, offsetP);
			offLenArray[1] = offLenS.substring(offsetP + 1);
			if (result.containsKey(fileIndex)) {
				result.get(fileIndex).offer(offLenArray);
			} else {
				PriorityQueue<String[]> resultQueue = new PriorityQueue<>(50, new Comparator<String[]>() {

					// 比较第一个String的大小即可
					@Override
					public int compare(String[] o1, String[] o2) {
						// TODO Auto-generated method stub
						return Long.parseLong(o1[0]) > Long.parseLong(o2[0])? 1 : -1 ;
					}

				});
				resultQueue.offer(offLenArray);
				result.put(fileIndex, resultQueue);
			}
		}
		return result;
	}

	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
		while (this.isConstructed == false) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		final long start = System.currentTimeMillis();
		int count = query2Count.incrementAndGet();
		if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query2 count:" + query2Count.get());
		}
		
//		final PriorityQueue<Row> buyerOrderQueue = new PriorityQueue<>(100, new Comparator<Row>() {
//
//			@Override
//			public int compare(Row o1, Row o2) {
//				// TODO Auto-generated method stub
//				long o2Time;
//				long o1Time;
//				o1Time = o1.get("createtime").longValue;
//				o2Time = o2.get("createtime").longValue;
//				return o2Time - o1Time > 0 ? 1 : -1;
//			}
//
//		});
		List<Row> buyerOrderResultList = new ArrayList<>(100);
		
		boolean validParameter = true;
		
		if (endTime <=0) {
			validParameter = false;
		}
		
		if (endTime <= startTime) {
			validParameter = false;
		}
	
		if (validParameter) {
			int index = indexFor(hashWithDistrub(buyerid), CommonConstants.QUERY2_ORDER_SPLIT_SIZE);
	//		
			String indexFile = this.query2Path + File.separator + index + CommonConstants.INDEX_SUFFIX;
						
			// 一个用户的所有order信息 key为createtime;value为file offset length
			List<String> buyerOrderList = new ArrayList<>(100);
	
			try (ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
				String line = indexFileReader.readLine();
				
				while (line != null) {
					// 获得一行中以<buyerid>开头的行
					if (line.startsWith(buyerid)) {
						int p = line.indexOf(':');
						String key = line.substring(0, p);
						Long createTime = Long.parseLong(key.substring(20));
						if (createTime >= startTime && createTime < endTime) {
							buyerOrderList.add(line.substring(p + 1));
						}
						
					}
					line = indexFileReader.readLine();
				}
				if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
					System.out.println("query2 index time:" + (System.currentTimeMillis() - start) + "size:" + buyerOrderList.size());
				}
				
				if (buyerOrderList.size() > 0) {
//					System.out.println(buyerOrderList.size());
					Row kvMap;
					Map<String,PriorityQueue<String[]>> buyerOrderAccessSequence = createOrderDataAccessSequence(buyerOrderList);
					for (Map.Entry<String, PriorityQueue<String[]>> e : buyerOrderAccessSequence.entrySet()) {
						String file = this.orderFiles.get(Integer.parseInt(e.getKey()));
	//					System.out.println("file:"+file);
						String[] sequence;
						try(RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
							sequence = e.getValue().poll();
							while(sequence != null) {
								
								Long offset = Long.parseLong(sequence[0]);
	//							System.out.println("offset:"+offset);
	//							System.out.println("lenth:"+Integer.valueOf(sequence[1]));
								byte[] content = new byte[Integer.valueOf(sequence[1])];
								orderFileReader.seek(offset);
								orderFileReader.read(content);
								line = new String(content);
		
								kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
								buyerOrderResultList.add(kvMap);
//								buyerOrderQueue.offer(kvMap);
								sequence = e.getValue().poll();
							}
							
						} 	
					}
	
					if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
						System.out.println("query2 original data time:" + (System.currentTimeMillis() - start) + "size:" + buyerOrderAccessSequence.size());
					}
				} else {
					System.out.println("query2 can't find order:" + buyerid + "," + startTime + "," + endTime);
				}
	
			} catch(IOException e) {
				
			}
		}
		// query2需要join good信息
		Row buyerRow = buyerOrderResultList.size() == 0 ? null : getBuyerRowFromOrderData(buyerOrderResultList.get(0));
		Comparator<Row> comparator = new Comparator<Row>() {

			@Override
			public int compare(Row o1, Row o2) {
				// TODO Auto-generated method stub
				long o2Time;
				long o1Time;
				o1Time = o1.get("createtime").longValue;
				o2Time = o2.get("createtime").longValue;
				return o2Time - o1Time > 0 ? 1 : -1;
			}

		};
		JoinOne joinResult = new JoinOne(buyerOrderResultList, buyerRow, comparator, "goodid", null);
		if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query2 join time:" + (System.currentTimeMillis() - start));
		}
		return joinResult;
//		return new Iterator<OrderSystem.Result>() {
//
//			PriorityQueue<Row> o = buyerOrderQueue;
//			Row buyerData = o.peek() == null ? null : getBuyerRowFromOrderData(o.peek());
//
//			public boolean hasNext() {
//				return o != null && o.size() > 0;
//			}
//
//			public Result next() {
//				if (!hasNext()) {
////					if (query2Count.get() % CommonConstants.QUERY_PRINT_COUNT ==0) {
////						System.out.println("query2 time:"+ (System.currentTimeMillis() - start));
////					}
//					return null;
//				}
//				Row orderData = buyerOrderQueue.poll();
//				Row goodData = getGoodRowFromOrderData(orderData);
//				return ResultImpl.createResultRow(orderData, buyerData, goodData, null);
//			}
//
//			public void remove() {
//
//			}
//		};
	}
	/**
	 * 用于0或者1次join时的简化操作
	 * @author immortalCockRoach
	 *
	 */
	private class JoinOne implements Iterator<OrderSystem.Result> {
		private List<Row> orderRows;
		private Row fixRow;
		// orderQueue和joinDataQueue的排序比较器
		private Comparator<Row> comparator;
		private PriorityQueue<Row> orderQueue;
		private Map<String, Row> joinDataMap;
		private Set<String> joinDataIndexSet;
		// "buyerid" 或者"goodid" null表示不需要join
		String joinId;
		Collection<String> queryKeys;
		public JoinOne(List<Row> orderRows,Row fixRow,Comparator<Row> comparator, String joinId, Collection<String> queryKeys) {
			this.orderRows = orderRows;
			this.fixRow = fixRow;
			this.comparator = comparator;
			this.joinId = joinId;
			this.queryKeys = queryKeys;
			this.orderQueue = new PriorityQueue<>(orderRows.size(), comparator);
			for (Row orderRow : orderRows) {
				orderQueue.offer(orderRow);
			}
			// 读取不同的good(buyer)Id 查找Row(不论是从cache还是通过memoryMap的索引去原始文件查找)组成一个map供之后join
			// 当orderRow为空或者query3最后得到的tag为order或者good的时候 不需要查询join
			if (joinId != null && orderRows.size() > 0) {
				joinDataIndexSet = new HashSet<>(orderRows.size());
				joinDataMap = new HashMap<>(orderRows.size());
				getUniqueDataIndex();
				// 当有没有现成的Row的时候
				if(joinDataIndexSet.size() > 0) {
					traverseOriginalFile();
				}
			}
		}
		/**
		 * 根据orderRow的信息查找出无重复的，且不在cache中的good/buyer的index信息
		 * 如果已经在cache中 直接放入Map
		 */
		private void getUniqueDataIndex() {
			// 首先尽量从cache中取Row 如果没有的话再去memoryMap中查找
			for (Row orderRow : orderRows) {
				String id = orderRow.getKV(joinId).rawValue;
				// 已经Map包含这个Row了就跳过
				if (!joinDataMap.containsKey(joinId)) {
					String cachedString;
					if (joinId.equals("buyerid")) {
						cachedString = buyersCache.get(id);
					} else {
						cachedString = goodsCache.get(id);
					}
					// 说明cache中有对应id的row信息，直接转换并加入Map
					if (cachedString != null) {
						Row goodData = StringUtils.createKVMapFromLine(cachedString, CommonConstants.SPLITTER);
						if (joinId.equals("buyerid")) {
							if (goodCacheHit.incrementAndGet() % CommonConstants.CACHE_PRINT_COUNT == 0) {
								System.out.println("good cache hit:" + goodCacheHit.get());
							}
						} else {
							if (buyerCacheHit.incrementAndGet() % CommonConstants.CACHE_PRINT_COUNT == 0) {
								System.out.println("buyer cache hit:" + buyerCacheHit.get());
							}
						}

						joinDataMap.put(id, goodData);
					} else { // cache中不包含时从memoryMap取原始数据的index信息,便于之后查找
						if (!joinDataIndexSet.contains(id)) {
							if (joinId.equals("buyerid")) {
								joinDataIndexSet.add(buyerMemoryIndexMap.get(id));
							} else {
								joinDataIndexSet.add(goodMemoryIndexMap.get(id));
							}
						}
					}
				}
			}
		}
		
		private void traverseOriginalFile() {
			// 此时得到的joinDataIndexSet 为无重复的good/buyer的index信息
			Map<String,PriorityQueue<String[]>> originalDataAccessSequence = createOrderDataAccessSequence(joinDataIndexSet);
			for (Map.Entry<String, PriorityQueue<String[]>> e : originalDataAccessSequence.entrySet()) {
				String file = null;
				if (joinId.equals("buyerid")) {
					file = buyerFiles.get(Integer.parseInt(e.getKey()));
				} else {
					file = goodFiles.get(Integer.parseInt(e.getKey()));
				}
//				System.out.println("file:"+file);
				String[] sequence;
				String line;
				try(RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
					sequence = e.getValue().poll();
					while(sequence != null) {
						
						Long offset = Long.parseLong(sequence[0]);

						byte[] content = new byte[Integer.valueOf(sequence[1])];
						orderFileReader.seek(offset);
						orderFileReader.read(content);
						line = new String(content);

						Row kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
						if (joinId.equals("buyerid")) {
							String id = kvMap.getKV("buyerid").rawValue;
							joinDataMap.put(id, kvMap);
							buyersCache.put(id, line);
						} else {
							String id = kvMap.getKV("goodid").rawValue;
							joinDataMap.put(id, kvMap);
							goodsCache.put(id, line);
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
			if (joinId == null){
				return ResultImpl.createResultRow(orderQueue.poll(), fixRow, null, createQueryKeys(queryKeys));
			} else {
				// 需要join的时候从Map中拉取
				Row orderRow = orderQueue.poll();
				Row dataRow = joinDataMap.get(orderRow.getKV(joinId).rawValue);
				return ResultImpl.createResultRow(orderRow, fixRow, dataRow, createQueryKeys(queryKeys));
			}	
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub	
		}
	}

	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
		while (this.isConstructed == false) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		final long start = System.currentTimeMillis();
		int count = query3Count.incrementAndGet();
		if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query3 count:" + query3Count.get());
		}
		String tag = keys != null && keys.size() == 1 ? getKeyJoin((String)keys.toArray()[0]): "all";
//		final PriorityQueue<Row> salerGoodsQueue = new PriorityQueue<>(512, new Comparator<Row>() {
//
//			@Override
//			public int compare(Row o1, Row o2) {
//				// TODO Auto-generated method stub
//				long o2Time;
//				long o1Time;
//				o1Time = o1.get("orderid").longValue;
//				o2Time = o2.get("orderid").longValue;
//				return o1Time - o2Time > 0 ? 1 : -1;
//			}
//
//		});
		List<Row> salerGoodsList = new ArrayList<>(512);
		final Collection<String> queryKeys = keys;

		int index = indexFor(hashWithDistrub(goodid), CommonConstants.ORDER_SPLIT_SIZE);
		String indexFile = this.query3Path + File.separator + index + CommonConstants.INDEX_SUFFIX;
//			cachedStrings = new ArrayList<>(100);
		List<String> offsetRecords = new ArrayList<>(512);
		try (ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			String line = indexFileReader.readLine();
			
			while (line != null) {
				// 获得一行中以<goodid>开头的行
//					offsetRecords.addAll(StringUtils.createListFromLongLineWithKey(line, goodid, CommonConstants.SPLITTER));
				if (line.startsWith(goodid)) {
					int p = line.indexOf(':');
					offsetRecords.add(line.substring(p + 1));
				}
				line = indexFileReader.readLine();
			}
			if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
				System.out.println("query3 index time:" + (System.currentTimeMillis() - start) + "size:" +offsetRecords.size());
			}
			
			if (offsetRecords.size() > 0 ) {
//				System.out.println(offsetRecords.size());
				Row kvMap;
				Map<String,PriorityQueue<String[]>> buyerOrderAccessSequence = createOrderDataAccessSequence(offsetRecords);
				for (Map.Entry<String, PriorityQueue<String[]>> e : buyerOrderAccessSequence.entrySet()) {
					String file = this.orderFiles.get(Integer.parseInt(e.getKey()));
//					System.out.println("file:"+file);
					String[] sequence;
					try(RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
						sequence = e.getValue().poll();
						while(sequence != null) {
							
							Long offset = Long.parseLong(sequence[0]);
//							System.out.println("offset:"+offset);
//							System.out.println("lenth:"+Integer.valueOf(sequence[1]));
							byte[] content = new byte[Integer.valueOf(sequence[1])];
							orderFileReader.seek(offset);
							orderFileReader.read(content);
							line = new String(content);
	
							kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
							// buyer的话需要join
							salerGoodsList.add(kvMap);
//							salerGoodsQueue.offer(kvMap);
							sequence = e.getValue().poll();
						}
						
					} 	
				}

				if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
					System.out.println("query3 original data time:" + (System.currentTimeMillis() - start) + "size:" + buyerOrderAccessSequence.size());
				}
			} else {
				System.out.println("query3 can't find order:");
			}

		} catch (IOException e) {
			
		}
		
		// query3至多只需要join buyer信息
		Row goodRow = salerGoodsList.size() == 0 ? null : getGoodRowFromOrderData(salerGoodsList.get(0));
		Comparator<Row> comparator = new Comparator<Row>() {

			@Override
			public int compare(Row o1, Row o2) {
				// TODO Auto-generated method stub
				long o2Time;
				long o1Time;
				o1Time = o1.get("orderid").longValue;
				o2Time = o2.get("orderid").longValue;
				return o1Time - o2Time > 0 ? 1 : -1;
			}

		};
//		 查询3可能不需要join的buyer
		String joinTable = tag.equals("buyer") || tag.equals("all") ? "buyerid" : null;
		JoinOne joinResult = new JoinOne(salerGoodsList, goodRow, comparator, joinTable, createQueryKeys(queryKeys));
		if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query3 join time:" + (System.currentTimeMillis() - start));
		}
		return joinResult;
//		return new Iterator<OrderSystem.Result>() {
//
//			final PriorityQueue<Row> o = salerGoodsQueue;
//			// 使用orderData(任意一个都对应同一个good信息)去查找对应的goodData
//			final Row goodData = o.peek() == null ? null : getGoodRowFromOrderData(o.peek());
//			final String tag = queryKeys != null && queryKeys.size() == 1 ? getKeyJoin((String)(queryKeys.toArray()[0])) : "all";
//			public boolean hasNext() {
//				return o != null && o.size() > 0;
//			}
//
//			public Result next() {
//				if (!hasNext()) {
//
//					return null;
//				}
//				Row orderData = o.poll();
//				Row buyerData = null;
//				// 当时all或者buyer的时候才去join buyer
//				if (tag.equals("buyer") || tag.equals("all")) {
////					System.out.println("join");
//					buyerData = getBuyerRowFromOrderData(orderData);
//				}
//				// 由于此时不需要join buyer 因此为null
//				return ResultImpl.createResultRow(orderData, buyerData, goodData, createQueryKeys(queryKeys));
//			}
//
//			public void remove() {
//				// ignore
//			}
//		};
	}

	public KeyValue sumOrdersByGood(String goodid, String key) {
		while (this.isConstructed == false) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		long start = System.currentTimeMillis();
		int count = query4Count.incrementAndGet();
		if (count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query4 count:" + query4Count.get());	
		}
		// 快速处理 减少不必要的查询的join开销
		String tag = "all";
		// 当为good表字段时快速处理
		if (key.equals("price") || key.equals("offprice") || key.startsWith("a_g_")) {
			return getGoodSumFromGood(goodid, key);
		} else if (key.startsWith("a_b_")) { //表示需要join buyer和order
			tag = "buyer";
		} else if (key.equals("amount") || key.startsWith("a_o_")) { //表示只需要order数据
			tag = "order";
		}
		
		List<Row> ordersData = new ArrayList<>(512);
		

		int index = indexFor(hashWithDistrub(goodid), CommonConstants.ORDER_SPLIT_SIZE);
		String indexFile = this.query3Path + File.separator + index + CommonConstants.INDEX_SUFFIX;
//			cachedStrings = new ArrayList<>(100);
		List<String> offsetRecords = new ArrayList<>(512);
		try(ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			String line = indexFileReader.readLine();
			
			while (line != null) {
				// 获得一行中以<goodid>开头的行
//					offsetRecords.addAll(StringUtils.createListFromLongLineWithKey(line, goodid, CommonConstants.SPLITTER));
				if (line.startsWith(goodid)) {
					int p = line.indexOf(':');
					offsetRecords.add(line.substring(p + 1));
				}
				line = indexFileReader.readLine();
			}
			if (count % CommonConstants.QUERY_PRINT_COUNT ==0) {
				System.out.println("query4 index time:"+ (System.currentTimeMillis() - start) + "size:" + offsetRecords.size());
			}
			
			if (offsetRecords.size() > 0 ) {
//				System.out.println(offsetRecords.size());
				Row kvMap;
				Map<String,PriorityQueue<String[]>> buyerOrderAccessSequence = createOrderDataAccessSequence(offsetRecords);
				for (Map.Entry<String, PriorityQueue<String[]>> e : buyerOrderAccessSequence.entrySet()) {
					String file = this.orderFiles.get(Integer.parseInt(e.getKey()));
//					System.out.println("file:"+file);
					String[] sequence;
					try(RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
						sequence = e.getValue().poll();
						while(sequence != null) {
							
							Long offset = Long.parseLong(sequence[0]);

							byte[] content = new byte[Integer.valueOf(sequence[1])];
							orderFileReader.seek(offset);
							orderFileReader.read(content);
							line = new String(content);
	
							kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
							ordersData.add(kvMap);
							sequence = e.getValue().poll();
						}
						
					} 	
				}

				if (count % CommonConstants.QUERY_PRINT_COUNT ==0) {
					System.out.println("query4 original time:"+ (System.currentTimeMillis() - start) + "size:" + buyerOrderAccessSequence.size());
				}
			} else {
				System.out.println("query4 can't find order:");
			}
			
		} catch (IOException e) {
			
		}

		// 如果不存在对应的order 直接返回null
		if (ordersData.size() == 0) {
			return null;
		}
		HashSet<String> queryingKeys = new HashSet<String>();
		queryingKeys.add(key);
		
		List<ResultImpl> allData = new ArrayList<ResultImpl>(ordersData.size());
		// query4至多只需要join buyer信息
		for (Row orderData : ordersData) {
			Row buyerData = null;
			if (tag.equals("buyer") || tag.equals("all")) {
				buyerData = getBuyerRowFromOrderData(orderData);
			}
			allData.add(ResultImpl.createResultRow(orderData, buyerData, null, queryingKeys));
		}

		if (count % CommonConstants.QUERY_PRINT_COUNT ==0) {
			System.out.println("query4 all time:"+ (System.currentTimeMillis() - start));
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
	
	/**
	 * 当good不存在这个属性的时候，直接返回null
	 * 否则计算出good的Order数 然后相乘并返回即可
	 * @param goodId
	 * @param key
	 * @return
	 */
	private KeyValue getGoodSumFromGood(String goodId, String key) {
		int index = indexFor(hashWithDistrub(goodId), CommonConstants.ORDER_SPLIT_SIZE);
		Row orderData = new Row();
		orderData.putKV("goodid", goodId);
		// 去good的indexFile中查找goodid对应的全部信息
		Row goodData = getGoodRowFromOrderData(orderData);
		// 说明goodid对应的good就不存在 直接返回null
		if (goodData == null) {
			return null;
		}
		// 说明good记录没有这个字段或者没有这个 直接返回null
		KV kv = goodData.get(key);
		if (kv == null) {
			return null;
		}
		// 说明这个rawvalue不是long也不是double
		Object value = StringUtils.parseStringToNumber(kv.rawValue);
		if (value == null) {
			return null;
		}
		String indexFile = this.query3Path + File.separator + index + CommonConstants.INDEX_SUFFIX;
		int count = 0;
		// 此处只是统计goodId对应的order的个数
		try(ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)){
			String line = indexFileReader.readLine();
			
			while (line != null) {
				
				if (line.startsWith(goodId)) {
					// 表示数量+1
					count++;
				}
				line = indexFileReader.readLine();
			}
		} catch (IOException e) {
			
		}
		// 判断具体类型 相乘并返回
		if (value instanceof Long) {
			Long result = ((long)value) * count;
			return new KV(key, Long.toString(result));
		} else {
			Double result = ((double)value) * count;
			return new KV(key, Double.toString(result));
		}
	}
}
