package com.alibaba.middleware.race;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedReader;
import com.alibaba.middleware.race.utils.ExtendBufferedWriter;
import com.alibaba.middleware.race.utils.IOUtils;
import com.alibaba.middleware.race.utils.IndexBuffer;
import com.alibaba.middleware.race.utils.IndexFileTuple;
import com.alibaba.middleware.race.utils.MetaTuple;
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
	
	private ExecutorService multiQueryPool2;
	private ExecutorService multiQueryPool3;
	private ExecutorService multiQueryPool4;
	
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
	
//	private int[] buyerLineRecords;
	private ExtendBufferedWriter[] buyersIndexWriters;
	private HashMap<String,MetaTuple> buyerMemoryIndexMap;
//	private SimpleLRUCache<String, MetaTuple> buyersCache;
	
//	private int[] goodLineRecords;
	private ExtendBufferedWriter[] goodsIndexWriters;
//	private SimpleLRUCache<String, String> goodsCache;
	private HashMap<String,MetaTuple> goodMemoryIndexMap; 
	private AtomicInteger query1Count;
	private AtomicInteger query2Count;
	private AtomicInteger query3Count;
	private AtomicInteger query4Count;
	
//	private AtomicInteger q1CacheHit;
//	private AtomicInteger q2CacheHit;
//	private AtomicInteger q3CacheHit;
//	private AtomicInteger q4CacheHit;
//	private AtomicInteger buyerCacheHit;
//	private AtomicInteger goodCacheHit;
	
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
	/**
	 * order的索引文件的创建方法 query 2 3 4的有2级索引，并保证1级索引的按buyerid或者goodid进行group
	 * @author immortalCockRoach
	 *
	 */
	class OrderHashIndexCreator implements Runnable{
		private String hashId;

		private ExtendBufferedWriter[] offSetwriters;
		private int[] indexLineRecords;
		private Collection<String> files;
		private CountDownLatch latch;
		private int bucketSize;
		private int buildCount;
		private int mod;
		private IndexBuffer[] bufferArray;
		private boolean byteValueFormat;
		private HashSet<String> identitiesSet;
		
		public OrderHashIndexCreator(String hashId, ExtendBufferedWriter[] offsetWriters,
				int[] indexLineRecords, Collection<String> files, int bUCKET_SIZE, int blockSize, CountDownLatch latch, String[] identities, boolean byteValueFormat) {
			super();
			this.latch = latch;
			this.hashId = hashId;
			this.offSetwriters = offsetWriters;
			this.files = files;
			this.bucketSize = bUCKET_SIZE;
			this.indexLineRecords = indexLineRecords;
			this.identitiesSet = new HashSet<>(Arrays.asList(identities));
			this.buildCount = 0;
			this.mod = 524288;
			this.bufferArray = new IndexBuffer[CommonConstants.INDEX_BUFFER_SIZE];
			this.byteValueFormat = byteValueFormat; 
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
				try (ExtendBufferedReader reader = IOUtils.createReader(orderFile, CommonConstants.ORDERFILE_BLOCK_SIZE)) {
					String line;
					while (true) {
						for (readLines = 0; readLines < CommonConstants.INDEX_BUFFER_SIZE; readLines++) {
							line = reader.readLine();
							if (line == null) {
								break;
							}
							StringBuilder offSetMsg = new StringBuilder();
							kvMap = StringUtils.createKVMapFromLineWithSet(line, CommonConstants.SPLITTER, this.identitiesSet);
							length = line.getBytes().length;
							
							// orderId一定存在且为long
							orderKV = kvMap.getKV(hashId);
							index = indexFor(
									hashWithDistrub(hashId.equals("orderid") ? orderKV.longValue : orderKV.rawValue),bucketSize);
							
							// 此处是rawValue还是longValue没区别
							offSetMsg.append(orderKV.rawValue) ;
							
							offSetMsg.append(':');
							// 对于query2 加入createtime
							if (hashId.equals("buyerid")) {
								offSetMsg.append(kvMap.getKV("createtime").longValue);
								offSetMsg.append(' ');
							}
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

						while (i < readLines) {
							
							offsetBw = offSetwriters[(int)(bufferArray[i].getIndex())];
							offsetBw.write(bufferArray[i].getLine());
							i++;
						}
						
						
					}	
					fileIndex++;
					
				} catch (IOException e) {
					// 忽略
				}
			}
			// 需要对query 2 3的索引进行group合并入大文件并写入goodIndexMap和buyerIndexMap
			if (this.byteValueFormat) {
				if (hashId.equals("goodid")) {
					// 首先关闭流 然后对索引进行排序
					closeWriter3();
					goodSeconderyIndex();
				} else {
					closeWriter2();
					buyerSeconderyIndex();
					
				}
			} else {
				closeWriter1();
			}
			this.latch.countDown();
		}
		private void buyerSeconderyIndex() {
			// query3 4的文件按buyerid进行group
			String orderedIndex = query2Path + File.separator + CommonConstants.INDEX_SUFFIX;
			Long offset = 0L;
			try (BufferedOutputStream orderIndexWriter = new BufferedOutputStream(new FileOutputStream(orderedIndex))) {
				for (int i = 0; i<= CommonConstants.QUERY2_ORDER_SPLIT_SIZE - 1; i++) {
					String indexFile = query2Path + File.separator + i + CommonConstants.INDEX_SUFFIX;
					// 对每个买家的记录进行group的map
//					System.out.println(indexFile);
					Map<String,List<byte[]>> groupedBuyerOrders = new HashMap<>(8192, 1f);
					try (ExtendBufferedReader orderIndexReader = IOUtils.createReader(indexFile, CommonConstants.ORDERFILE_BLOCK_SIZE)) {
						String line  = orderIndexReader.readLine();
						while (line != null) {
							// buyerid定长20
							String buyerId = line.substring(0, 20);
							byte[] content = StringUtils.getBuyerByteArray(line.substring(21));
							if (groupedBuyerOrders.containsKey(buyerId)) {
								groupedBuyerOrders.get(buyerId).add(content);
							} else {
								List<byte[]> buyerOrdersList = new ArrayList<>(50);
								buyerOrdersList.add(content);
								groupedBuyerOrders.put(buyerId, buyerOrdersList);
							}
							line = orderIndexReader.readLine();
						}
					} catch (Exception e) {
						// TODO: handle exception
					}
					for (Map.Entry<String, List<byte[]>> e : groupedBuyerOrders.entrySet()) {
						
						List<byte[]> list = e.getValue();
						MetaTuple buyerTuple = new MetaTuple(offset, list.size());
						// 内存二级索引 buyerid-tuple
						buyerMemoryIndexMap.put(e.getKey(), buyerTuple);
						// 挨个写入有序的索引文件
						for(byte[] bytes : list) {
							orderIndexWriter.write(bytes);
							// buyer的有序索引中一个记录长度为24byte
							offset += 24;
						}
					}
				}
			} catch (IOException e) {
				
			}
		}
		private void goodSeconderyIndex() {
			// query3 4的文件按goodid进行group
			String orderedIndex = query3Path + File.separator + CommonConstants.INDEX_SUFFIX;
			Long offset = 0L;
			try (BufferedOutputStream orderIndexWriter = new BufferedOutputStream(new FileOutputStream(orderedIndex))) {
				for (int i = 0; i<= CommonConstants.QUERY3_ORDER_SPLIT_SIZE - 1; i++) {
					String indexFile = query3Path + File.separator + i + CommonConstants.INDEX_SUFFIX;
					// 对每个good的记录进行group的map
//					System.out.println(indexFile);
					Map<String,List<byte[]>> groupedGoodOrders = new HashMap<>(4096, 1f);
					try (ExtendBufferedReader orderIndexReader = IOUtils.createReader(indexFile, CommonConstants.ORDERFILE_BLOCK_SIZE)) {
						String line  = orderIndexReader.readLine();
						while (line != null) {
							// goodId不定长 所以需要按:分割
							int p = line.indexOf(':');
							String goodId = line.substring(0, p);
							byte[] content = StringUtils.getGoodByteArray(line.substring(p + 1));
							if (groupedGoodOrders.containsKey(goodId)) {
								groupedGoodOrders.get(goodId).add(content);
							} else {
								List<byte[]> goodOrdersList = new ArrayList<>(100);
								goodOrdersList.add(content);
								groupedGoodOrders.put(goodId, goodOrdersList);
							}
							line = orderIndexReader.readLine();
						}
					} catch (Exception e) {
						// TODO: handle exception
					}
					for (Map.Entry<String, List<byte[]>> e : groupedGoodOrders.entrySet()) {
						
						List<byte[]> list = e.getValue();
						MetaTuple goodTuple = new MetaTuple(offset, list.size());
						// 内存二级索引 goodId-tuple
						goodMemoryIndexMap.put(e.getKey(), goodTuple);
						// 挨个写入有序的索引文件
						for(byte[] bytes : list) {
							orderIndexWriter.write(bytes);
							// good的有序索引中一个记录长度为16byte
							offset += 16;
						}
					}
				}
			} catch (IOException e) {
				
			}
		}
	}
	/**
	 * good和buyer的索引创建方式 直接放在内存中
	 * @author immortalCockRoach
	 *
	 */
	class OtherHashIndexCreator implements Runnable {
		private String hashId;
		private Collection<String> files;
		private CountDownLatch latch;
		private HashSet<String> identitiesSet;
		private int buildCount;
		private int mod;
		public OtherHashIndexCreator(String hashId, Collection<String> files, CountDownLatch latch,
				String[] identities) {
			super();
			this.hashId = hashId;
			this.files = files;
			this.latch = latch;
			this.identitiesSet = new HashSet<>(Arrays.asList(identities));
			this.buildCount = 0;
			this.mod = 524288;
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			int fileIndex = 0;
			for(String orderFile : this.files) {
				System.out.println(orderFile);
				Row kvMap = null;
				// 记录当前行的偏移
				long offset = 0L;
				String id;
				// 记录当前行的总长度
				int length = 0;
				try (ExtendBufferedReader reader = IOUtils.createReader(orderFile, CommonConstants.OTHERFILE_BLOCK_SIZE)) {
					String line = reader.readLine();
					while (line != null) {
//							StringBuilder offSetMsg = new StringBuilder();
							kvMap = StringUtils.createKVMapFromLineWithSet(line, CommonConstants.SPLITTER, this.identitiesSet);
							length = line.getBytes().length;
							
							// orderId一定存在且为long
							id = kvMap.getKV(hashId).rawValue;
//							offSetMsg.append(fileIndex);
//							offSetMsg.append(' ');
//							offSetMsg.append(offset);
//							offSetMsg.append(' ');
//							offSetMsg.append(length);
							if (hashId.equals("goodid")) {
								MetaTuple goodTuple = goodMemoryIndexMap.get(id);
								goodTuple.setFileIndex(fileIndex);
								goodTuple.setOriginalOffset(offset);
								goodTuple.setOriginalLength(length);
							} else {
								MetaTuple buyerTuple = buyerMemoryIndexMap.get(id);
								buyerTuple.setFileIndex(fileIndex);
								buyerTuple.setOriginalOffset(offset);
								buyerTuple.setOriginalLength(length);
							}
			
							offset += (length + 1);
							
							
							buildCount++;
							if ((buildCount & (mod - 1)) == 0) {
								System.out.println(hashId + "construct:" + buildCount);
							}	
							line = reader.readLine();
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
		
		this.query1LineRecords = new int[CommonConstants.QUERY1_ORDER_SPLIT_SIZE];
//		query2Cache = new SimpleLRUCache<>(16384);
		this.query2LineRecords = new int[CommonConstants.QUERY2_ORDER_SPLIT_SIZE];
//		query3Cache = new SimpleLRUCache<>(16384);
		this.query3LineRecords = new int[CommonConstants.QUERY3_ORDER_SPLIT_SIZE];
//		query4Cache = new SimpleLRUCache<>(16384);
		
//		goodsCache = new SimpleLRUCache<>(65536);
		if (!buyerGoodInMemory) {
//			this.goodLineRecords = new int[CommonConstants.OTHER_SPLIT_SIZE];
		} else {
			this.goodMemoryIndexMap = new HashMap<>(4194304, 1f);
		}
//		buyersCache = new SimpleLRUCache<>(65536);
		if (!buyerGoodInMemory) {
//			this.buyerLineRecords = new int[CommonConstants.OTHER_SPLIT_SIZE];
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
//		buyerCacheHit = new AtomicInteger(0);
//		goodCacheHit = new AtomicInteger(0);
		
		multiQueryPool2 = Executors.newFixedThreadPool(8);
		multiQueryPool3 = Executors.newFixedThreadPool(8);
		multiQueryPool4 = Executors.newFixedThreadPool(8);
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
		OrderSystemImpl os = new OrderSystemImpl();
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
//////		
//		Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
//		
//		System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
//		while (it.hasNext()) {
////			it.next();
//			System.out.println(it.next());
//		}
//		System.out.println("time:"+(System.currentTimeMillis() - start));
		//


//		String goodid = "aye-8837-3aca358bfad3";
//		String salerid = "almm-b250-b1880d628b9a";
//		System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
//		long start = System.currentTimeMillis();
//		List<String> keys = new ArrayList<>();
//		keys.add("address");
//		Iterator it = os.queryOrdersBySaler(salerid, goodid, keys);
////		System.out.println(System.currentTimeMillis()-start);
//		while (it.hasNext()) {
////			System.out.println(it.next());
//			it.next();
//		}
//		System.out.println(System.currentTimeMillis()-start);
		//
		long start = System.currentTimeMillis();
		String goodid = "dd-a27d-835565dfb080";
		String attr = "a_b_3503";
		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		System.out.println(os.sumOrdersByGood(goodid, attr));
		System.out.println(System.currentTimeMillis() -start);
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
		os.close();
		
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
		CountDownLatch latch = new CountDownLatch(3);
		new Thread(new OrderHashIndexCreator("orderid", query1IndexWriters, query1LineRecords,orderFiles, CommonConstants.QUERY1_ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch,new String[]{"orderid"}, false)).start();
		new Thread(new OrderHashIndexCreator("buyerid", query2IndexWriters, query2LineRecords, orderFiles, CommonConstants.QUERY2_ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch,new String[]{"buyerid","createtime"}, true)).start();
		new Thread(new OrderHashIndexCreator("goodid", query3IndexWriters, query3LineRecords ,orderFiles, CommonConstants.QUERY3_ORDER_SPLIT_SIZE,
				CommonConstants.ORDERFILE_BLOCK_SIZE, latch, new String[]{"goodid"}, true)).start();
		// new Thread(new HashIndexCreator("goodid", query4Writers, orderFiles,
		// CommonConstants.ORDER_SPLIT_SIZE,latch)).start();

		
		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("ORDER INDEX OK");
		latch = new CountDownLatch(2);
		new Thread(new OtherHashIndexCreator("buyerid", buyerFiles, latch, new String[]{"buyerid"})).start();
		new Thread(new OtherHashIndexCreator("goodid", goodFiles, latch, new String[]{"goodid"} )).start();
		
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
		this.query1IndexWriters = new ExtendBufferedWriter[CommonConstants.QUERY1_ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.QUERY1_ORDER_SPLIT_SIZE; i++) {
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
		
		this.query3IndexWriters = new ExtendBufferedWriter[CommonConstants.QUERY3_ORDER_SPLIT_SIZE];
		for (int i = 0; i < CommonConstants.QUERY3_ORDER_SPLIT_SIZE; i++) {
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
			System.out.println("query1count:" + count);	
		}
		Row query = new Row();
		query.putKV("orderid", orderId);


		Row orderData = null;
		

		int index = indexFor(hashWithDistrub(orderId), CommonConstants.QUERY1_ORDER_SPLIT_SIZE);
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
			System.out.println("query1 join time:" + (System.currentTimeMillis() - start));
		}
		return result;
	}
	
	private void closeWriter1() {
		try {

			for (ExtendBufferedWriter bw : query1IndexWriters) {
				bw.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void closeWriter2() {
		try {
			for (ExtendBufferedWriter bw : query2IndexWriters) {
				bw.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void closeWriter3() {
		try {
			for (ExtendBufferedWriter bw : query3IndexWriters) {
				bw.close();
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
		MetaTuple goodTuple = this.goodMemoryIndexMap.get(goodId);
//				System.out.println(line);
		
		// 由于现在query4的求和可能直接查good信息，因此此处代表不存在对应的信息
		if (goodTuple == null) {
			return null;
		}
		String file = this.goodFiles.get(goodTuple.getFileIndex());
		byte[] content = new byte[goodTuple.getOriginalLength()];
		try (RandomAccessFile goodFileReader = new RandomAccessFile(file, "r")) {
			goodFileReader.seek(goodTuple.getOriginalOffset());
			goodFileReader.read(content);
			String line = new String(content);
			goodData = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
//				goodsCache.put(goodId, line);
		} catch (IOException e) {
			// 忽略
		} 
		return goodData;
	}
	
	private Row getBuyerRowFromOrderData(Row orderData) {
		Row buyerData = null;
		
		String buyerId = orderData.getKV("buyerid").rawValue;

		MetaTuple buyerTuple = this.buyerMemoryIndexMap.get(buyerId);
//		if (buyerTuple != null) {
//			indexArray = StringUtils.getIndexInfo(line);
//		}
		
		String file = this.buyerFiles.get(buyerTuple.getFileIndex());
//		Long offset = Long.parseLong(buyerTuple.getIndexOffset());
		byte[] content = new byte[buyerTuple.getOriginalLength()];
		try (RandomAccessFile buyerFileReader = new RandomAccessFile(file, "r")) {
			buyerFileReader.seek(buyerTuple.getOriginalOffset());
			buyerFileReader.read(content);
			String line = new String(content);
			buyerData = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
//			buyersCache.put(buyerId, line);

		} catch (IOException e) {
			// 忽略
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
	
	private Map<Integer,PriorityQueue<IndexFileTuple>> createOrderDataAccessSequence(Collection<byte[]> offsetRecords) {
		Map<Integer, PriorityQueue<IndexFileTuple>> result = new HashMap<>(64);
		for(byte[] e : offsetRecords) {
			IndexFileTuple tuple = new IndexFileTuple(e);
			int fileIndex = tuple.getFileIndex();
			if (result.containsKey(tuple.getFileIndex())) {
				result.get(fileIndex).offer(tuple);
			} else {
				PriorityQueue<IndexFileTuple> resultQueue = new PriorityQueue<>(50, new Comparator<IndexFileTuple>() {

					// 比较第一个String的大小即可
					@Override
					public int compare(IndexFileTuple o1,IndexFileTuple o2) {
						// TODO Auto-generated method stub
						return o1.getOffset() > o2.getOffset() ? 1 : -1 ;
					}

				});
				resultQueue.offer(tuple);
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
		int q2count = query2Count.incrementAndGet();
		if (q2count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query2 count:" + q2count);
		}
		
		List<Row> buyerOrderResultList = new ArrayList<>(100);
		
		boolean validParameter = true;
		
		if (endTime <=0) {
			validParameter = false;
		}
		
		if (endTime <= startTime) {
			validParameter = false;
		}
		
		if (startTime > 11668409867L) {
			validParameter = false;
		}
		
		if (endTime <= 1468385553L ) {
			validParameter = false;
		}
		
		MetaTuple buyerTuple = this.buyerMemoryIndexMap.get(buyerid);
		if (buyerTuple == null) {
			validParameter = false;
		}
		List<byte[]> buyerOrderList = new ArrayList<>(100);
		if (validParameter) {
//			int index = indexFor(hashWithDistrub(buyerid), CommonConstants.QUERY2_ORDER_SPLIT_SIZE);
			String indexFile = this.query2Path + File.separator + CommonConstants.INDEX_SUFFIX;
	
			try (RandomAccessFile indexFileReader = new RandomAccessFile(indexFile, "r")){
				indexFileReader.seek(buyerTuple.getIndexOffset());
				int count = buyerTuple.getCount();
				for (int i = 0; i<= count - 1; i++) {
					long createTime = indexFileReader.readLong();
					if (createTime >= startTime && createTime < endTime) {
						byte[] content = new byte[16];
						indexFileReader.read(content);
						buyerOrderList.add(content);
					} else {
						// 如果初始的long不符合 跳过剩下的16byte
						indexFileReader.skipBytes(16);
					}
					
				}
				
			} catch(IOException e) {
				
			}
			if (q2count % CommonConstants.QUERY_PRINT_COUNT == 0) {
				System.out.println("query2 index time:" + (System.currentTimeMillis() - start) + "size:" + buyerOrderList.size());
			}
		}
		if (buyerOrderList.size() > 0) {
//			System.out.println(buyerOrderList.size());
//			Row kvMap;
			Map<Integer,PriorityQueue<IndexFileTuple>> buyerOrderAccessSequence = createOrderDataAccessSequence(buyerOrderList);
			List<Future<List<Row>>> result = new ArrayList<>();
			for (Map.Entry<Integer, PriorityQueue<IndexFileTuple>> e : buyerOrderAccessSequence.entrySet()) {

				OrderDataSearch search = new OrderDataSearch(e.getKey(), e.getValue());
				result.add(multiQueryPool2.submit(search));				
			}
			for (Future<List<Row>> f: result) {
				try {
					List<Row> list = f.get();
					if (list != null) {
						for(Row row : list) {
							buyerOrderResultList.add(row);
						}
					}
				} catch (InterruptedException | ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			if (q2count % CommonConstants.QUERY_PRINT_COUNT == 0) {
				System.out.println("query2 original data time:" + (System.currentTimeMillis() - start) + "size:" + buyerOrderAccessSequence.size());
			}
		} else {
			System.out.println("query2 can't find order:" + buyerid + "," + startTime + "," + endTime);
		}
//		for (Row row :buyerOrderResultList) {
//			System.out.println(row);
//		}
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
		if (q2count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query2 join time:" + (System.currentTimeMillis() - start));
		}
		return joinResult;
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
		private List<byte[]> joinDataIndexList;
		// "buyerid" 或者"goodid" null表示不需要join
		String joinId;
		Collection<String> queryKeys;
		public JoinOne(List<Row> orderRows,Row fixRow,Comparator<Row> comparator, String joinId, Collection<String> queryKeys) {
			this.orderRows = orderRows;
			this.fixRow = fixRow;
			this.comparator = comparator;
			this.joinId = joinId;
			this.queryKeys = queryKeys;
			this.orderQueue = new PriorityQueue<>(64, comparator);
			for (Row orderRow : orderRows) {
				orderQueue.offer(orderRow);
			}
			// 读取不同的good(buyer)Id 查找Row(不论是从cache还是通过memoryMap的索引去原始文件查找)组成一个map供之后join
			// 当orderRow为空或者query3最后得到的tag为order或者good的时候 不需要查询join
			if (joinId != null && orderRows.size() > 0) {
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
		 * 根据orderRow的信息查找出无重复的，且不在cache中的good/buyer的index信息
		 * 如果已经在cache中 直接放入Map
		 */
		private void getUniqueDataIndex() {
			// 首先尽量从cache中取Row 如果没有的话再去memoryMap中查找
			for (Row orderRow : orderRows) {
				String id = orderRow.getKV(joinId).rawValue;
				// 已经Map包含这个Row了就跳过
				if (!joinDataMap.containsKey(id)) {
					if (joinId.equals("goodid")) {
						joinDataIndexList.add(goodMemoryIndexMap.get(id).getOriginalByte());
					} else {
						joinDataIndexList.add(buyerMemoryIndexMap.get(id).getOriginalByte());
					}
				}
			}
		}
		
		private void traverseOriginalFile() {
			// 此时得到的joinDataIndexSet 为无重复的good/buyer的index信息
			Map<Integer, PriorityQueue<IndexFileTuple>> originalDataAccessSequence = createOrderDataAccessSequence(joinDataIndexList);
			for (Map.Entry<Integer, PriorityQueue<IndexFileTuple>> e : originalDataAccessSequence.entrySet()) {
				String file = null;
				if (joinId.equals("buyerid")) {
					file = buyerFiles.get(e.getKey());
				} else {
					file = goodFiles.get(e.getKey());
				}
//				System.out.println("file:"+file);
				IndexFileTuple sequence;
				String line;
				try(RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
					sequence = e.getValue().poll();
					while(sequence != null) {
						
						long offset = sequence.getOffset();

						byte[] content = new byte[sequence.getLength()];
						orderFileReader.seek(offset);
						orderFileReader.read(content);
						line = new String(content);

						Row kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
						if (joinId.equals("buyerid")) {
							String id = kvMap.getKV("buyerid").rawValue;
							joinDataMap.put(id, kvMap);
						} else {
							String id = kvMap.getKV("goodid").rawValue;
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
	
	private class OrderDataSearch implements Callable<List<Row>> {
		
		private int fileIndex;
		private PriorityQueue<IndexFileTuple> sequenceQueue;
		
		public OrderDataSearch(int fileIndex, PriorityQueue<IndexFileTuple> sequenceQueue) {
			this.fileIndex = fileIndex;
			this.sequenceQueue = sequenceQueue;
		}
		@Override
		public List<Row> call() throws Exception {
			// 这个sequence不可能是负数
			List<Row> result = new ArrayList<>(sequenceQueue.size());
			String file = orderFiles.get(fileIndex);
//			System.out.println("file:"+file);
			IndexFileTuple sequence;
			try(RandomAccessFile orderFileReader = new RandomAccessFile(file, "r")) {
				sequence = sequenceQueue.poll();
				while(sequence != null) {
					
					long offset = sequence.getOffset();
//					System.out.println("offset:"+offset);
//					System.out.println("lenth:"+Integer.valueOf(sequence[1]));
					byte[] content = new byte[sequence.getLength()];
					orderFileReader.seek(offset);
					orderFileReader.read(content);
					String line = new String(content);

					Row kvMap = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);
					// buyer的话需要join
					result.add(kvMap);
//					salerGoodsQueue.offer(kvMap);
					sequence = sequenceQueue.poll();
				}
				
			} 	
			return result;
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
		int q3count = query3Count.incrementAndGet();
		if (q3count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query3 count:" + query3Count.get());
		}
		boolean validParameter = true;
		MetaTuple goodTuple = this.goodMemoryIndexMap.get(goodid);
		if (goodTuple == null) {
			validParameter = false;
		}
		String tag = keys != null && keys.size() == 1 ? getKeyJoin((String)keys.toArray()[0]): "all";

		List<Row> salerGoodsList = new ArrayList<>(100);
		final Collection<String> queryKeys = keys;
		List<byte[]> offsetRecords = new ArrayList<>(100);
		if (validParameter) {
			String indexFile = this.query3Path + File.separator + CommonConstants.INDEX_SUFFIX;
	
			try (RandomAccessFile indexFileReader = new RandomAccessFile(indexFile, "r")){
				indexFileReader.seek(goodTuple.getIndexOffset());
				int count = goodTuple.getCount();
				for (int i = 0; i<= count - 1; i++) {
					byte[] content = new byte[16];
					indexFileReader.read(content);
					offsetRecords.add(content);
					
				}
				if (q3count % CommonConstants.QUERY_PRINT_COUNT == 0) {
					System.out.println("query3 index time:" + (System.currentTimeMillis() - start) + "size:" +offsetRecords.size());
				}
				
			} catch (IOException e) {
				
			}
		}
		if (offsetRecords.size() > 0 ) {
//			System.out.println(offsetRecords.size());
//			Row kvMap;
			Map<Integer,PriorityQueue<IndexFileTuple>> buyerOrderAccessSequence = createOrderDataAccessSequence(offsetRecords);
			List<Future<List<Row>>> result = new ArrayList<>();
			for (Map.Entry<Integer, PriorityQueue<IndexFileTuple>> e : buyerOrderAccessSequence.entrySet()) {

				OrderDataSearch search = new OrderDataSearch(e.getKey(), e.getValue());
				result.add(multiQueryPool3.submit(search));
			}
			for (Future<List<Row>> f: result) {
				try {
					List<Row> list = f.get();
					if (list != null) {
						for(Row row : list) {
							salerGoodsList.add(row);
						}
					}
				} catch (InterruptedException | ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			if (q3count % CommonConstants.QUERY_PRINT_COUNT == 0) {
				System.out.println("query3 original data time:" + (System.currentTimeMillis() - start) + "size:" + buyerOrderAccessSequence.size());
			}
		} else {
			System.out.println("query3 can't find order:");
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
		if (q3count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query3 join time:" + (System.currentTimeMillis() - start));
		}
		return joinResult;
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
		int q4count = query4Count.incrementAndGet();
		if (q4count % CommonConstants.QUERY_PRINT_COUNT == 0) {
			System.out.println("query4 count:" + q4count);	
		}
		// 快速处理 减少不必要的查询的join开销
		MetaTuple goodTuple = this.goodMemoryIndexMap.get(goodid);
		if (goodTuple == null) {
			// 说明tuple中没有这个 
			return null;
		}
		String tag = "all";
		// 当为good表字段时快速处理
		if (key.equals("price") || key.equals("offprice") || key.startsWith("a_g_")) {
			return getGoodSumFromGood(goodid, key);
		} else if (key.startsWith("a_b_")) { //表示需要join buyer和order
			tag = "buyer";
		} else if (key.equals("amount") || key.startsWith("a_o_")) { //表示只需要order数据
			tag = "order";
		} else {
			// 不可求和的字段直接返回
			return null;
		}
		
		List<Row> ordersData = new ArrayList<>(100);
		
		String indexFile = this.query3Path + File.separator + CommonConstants.INDEX_SUFFIX;
//			cachedStrings = new ArrayList<>(100);
		List<byte[]> offsetRecords = new ArrayList<>(100);
		try(RandomAccessFile indexFileReader = new RandomAccessFile(indexFile, "r")){
			indexFileReader.seek(goodTuple.getIndexOffset());
			int count = goodTuple.getCount();
			for (int i = 0; i<= count - 1; i++) {
				byte[] content = new byte[16];
				indexFileReader.read(content);
				offsetRecords.add(content);
				
			}
			if (q4count % CommonConstants.QUERY_PRINT_COUNT ==0) {
				System.out.println("query4 index time:"+ (System.currentTimeMillis() - start) + "size:" + offsetRecords.size());
			}
			
		} catch (IOException e) {
			
		}
		
		if (offsetRecords.size() > 0 ) {
//			System.out.println(offsetRecords.size());
//			Row kvMap;
			Map<Integer,PriorityQueue<IndexFileTuple>> buyerOrderAccessSequence = createOrderDataAccessSequence(offsetRecords);
			List<Future<List<Row>>> result = new ArrayList<>();
			for (Map.Entry<Integer, PriorityQueue<IndexFileTuple>> e : buyerOrderAccessSequence.entrySet()) {

				OrderDataSearch search = new OrderDataSearch(e.getKey(), e.getValue());
				result.add(multiQueryPool4.submit(search));
			}
			for (Future<List<Row>> f: result) {
				try {
					List<Row> list = f.get();
					if (list != null) {
						for(Row row : list) {
							ordersData.add(row);
						}
					}
				} catch (InterruptedException | ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			if (q4count % CommonConstants.QUERY_PRINT_COUNT ==0) {
				System.out.println("query4 original time:"+ (System.currentTimeMillis() - start) + "size:" + buyerOrderAccessSequence.size());
			}
		} else {
			System.out.println("query4 can't find order:");
		}
		// 如果不存在对应的order 直接返回null
		if (ordersData.size() == 0) {
			return null;
		}
		HashSet<String> queryingKeys = new HashSet<String>();
		queryingKeys.add(key);
		
		List<Result> allData = new ArrayList<>(ordersData.size());
		// query4至多只需要join buyer信息
		if(tag.equals("order")) { // 说明此时只有orderData 不需要join
			for (Row orderData : ordersData) {
//				Row buyerData = null;
//				if (tag.equals("buyer") || tag.equals("all")) {
//					buyerData = getBuyerRowFromOrderData(orderData);
//				}
				allData.add(ResultImpl.createResultRow(orderData, null, null, queryingKeys));
			}
		} else { // buyer或者all
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
			// 不需要join good信息
			JoinOne joinResult = new JoinOne(ordersData, null, comparator, "buyerid", queryingKeys);

			while (joinResult.hasNext()) {
				allData.add(joinResult.next());
			}
			if (q4count % CommonConstants.QUERY_PRINT_COUNT == 0) {
				System.out.println("query4 join time:" + (System.currentTimeMillis() - start));
			}
		}

//		if (q4count % CommonConstants.QUERY_PRINT_COUNT ==0) {
//			System.out.println("query4 all time:"+ (System.currentTimeMillis() - start));
//		}
		// accumulate as Long
		try {
			boolean hasValidData = false;
			long sum = 0;
			for (Result r : allData) {
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
			for (Result r : allData) {
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
		
		// 说明是可求和类型
		int count = this.goodMemoryIndexMap.get(goodId).getCount();
		// 判断具体类型 相乘并返回
		if (value instanceof Long) {
			Long result = ((long)value) * count;
			return new KV(key, Long.toString(result));
		} else {
			Double result = ((double)value) * count;
			return new KV(key, Double.toString(result));
		}
	}
	
	private void close() {
		multiQueryPool2.shutdown();
		multiQueryPool3.shutdown();
		multiQueryPool4.shutdown();
	}
}
