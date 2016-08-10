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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.helper.BuyerIndexOperater;
import com.alibaba.middleware.race.helper.GoodIndexOperater;
import com.alibaba.middleware.race.helper.OrderHashIndexCreator;
import com.alibaba.middleware.race.helper.OtherHashIndexCreator;
import com.alibaba.middleware.race.utils.CommonConstants;
import com.alibaba.middleware.race.utils.ExtendBufferedReader;
import com.alibaba.middleware.race.utils.ExtendBufferedWriter;
import com.alibaba.middleware.race.utils.HashUtils;
import com.alibaba.middleware.race.utils.IOUtils;
import com.alibaba.middleware.race.utils.IndexFileTuple;
import com.alibaba.middleware.race.utils.MetaTuple;
import com.alibaba.middleware.race.utils.StringUtils;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem, BuyerIndexOperater, GoodIndexOperater {

	private List<String> orderFiles;
	private List<String> goodFiles;
	private List<String> buyerFiles;

	private String query1Path;
	private String query2Path;
	private String query3Path;

	private ExecutorService multiQueryPool2;
	private ExecutorService multiQueryPool3;
	private ExecutorService multiQueryPool4;

	private ExtendBufferedWriter[] query1IndexWriters;
	private ExtendBufferedWriter[] query2IndexWriters;
	private ExtendBufferedWriter[] query3IndexWriters;

	private HashMap<String, MetaTuple> buyerMemoryIndexMap;
	private HashMap<String, MetaTuple> goodMemoryIndexMap;

	private AtomicInteger query1Count;
	private AtomicInteger query2Count;
	private AtomicInteger query3Count;
	private AtomicInteger query4Count;

	private volatile boolean isConstructed;

	/**
	 * 根据参数新建新建文件 目录等操作
	 */
	public OrderSystemImpl() {

		isConstructed = false;

		query1Count = new AtomicInteger(0);
		query2Count = new AtomicInteger(0);
		query3Count = new AtomicInteger(0);
		query4Count = new AtomicInteger(0);

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
		// long start = System.currentTimeMillis();
		// long orderid = 609670049;
		// System.out.println("\n查询订单号为" + orderid + "的订单");
		// List<String> keys = new ArrayList<>();
		// keys.add("description");
		//// keys.add("buyerid");
		// System.out.println(os.queryOrder(orderid, keys));
		// System.out.println(System.currentTimeMillis()-start);
		// System.out.println("\n查询订单号为" + orderid +
		// "的订单，查询的keys为空，返回订单，但没有kv数据");
		// System.out.println(os.queryOrder(orderid, new ArrayList<String>()));

		// System.out.println("\n查询订单号为" + orderid + "的订单的contactphone,
		// buyerid,foo, done, price字段");
		// List<String> queryingKeys = new ArrayList<String>();
		// queryingKeys.add("contactphone");
		// queryingKeys.add("buyerid");
		// queryingKeys.add("foo");
		// queryingKeys.add("done");
		// queryingKeys.add("price");
		// Result result = os.queryOrder(orderid, queryingKeys);
		// System.out.println(result);
		// System.out.println("\n查询订单号不存在的订单");
		// result = os.queryOrder(1111, queryingKeys);
		// if (result == null) {
		// System.out.println(1111 + " order not exist");
		// }
		// System.out.println(System.currentTimeMillis() - start);
		// long start = System.currentTimeMillis();
		// String buyerid = "tp-b0a2-fd0ca6720971";
		// long startTime = 1467791748;
		// long endTime = 1481816836;
		//////
		// Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime,
		// buyerid);
		//
		// System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
		// while (it.hasNext()) {
		//// it.next();
		// System.out.println(it.next());
		// }
		// System.out.println("time:"+(System.currentTimeMillis() - start));
		//

		// String goodid = "aye-8837-3aca358bfad3";
		// String salerid = "almm-b250-b1880d628b9a";
		// System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid +
		// "的订单");
		// long start = System.currentTimeMillis();
		// List<String> keys = new ArrayList<>();
		// keys.add("address");
		// Iterator it = os.queryOrdersBySaler(salerid, goodid, keys);
		//// System.out.println(System.currentTimeMillis()-start);
		// while (it.hasNext()) {
		//// System.out.println(it.next());
		// it.next();
		// }
		// System.out.println(System.currentTimeMillis()-start);
		//
		long start = System.currentTimeMillis();
		String goodid = "dd-a27d-835565dfb080";
		String attr = "a_b_3503";
		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		System.out.println(os.sumOrdersByGood(goodid, attr));
		System.out.println(System.currentTimeMillis() - start);
		// String goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
		// String attr = "app_order_33_0";
		// System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		// System.out.println(os.sumOrdersByGood(goodid, attr));
		//
		// attr = "done";
		// System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		// KeyValue sum = os.sumOrdersByGood(goodid, attr);
		// if (sum == null) {
		// System.out.println("由于该字段是布尔类型，返回值是null");
		// }
		//
		// attr = "foo";
		// System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		// sum = os.sumOrdersByGood(goodid, attr);
		// if (sum == null) {
		// System.out.println("由于该字段不存在，返回值是null");
		// }
		os.close();

	}

	public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException, InterruptedException {

		this.orderFiles = new ArrayList<>(orderFiles);
		this.buyerFiles = new ArrayList<>(buyerFiles);
		this.goodFiles = new ArrayList<>(goodFiles);
		long start = System.currentTimeMillis();
		constructDir(storeFolders);
		final long dir = System.currentTimeMillis();
		System.out.println("dir time:" + (dir - start));

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
		// 3个线程各自完成之后 该函数才能返回
		CountDownLatch latch = new CountDownLatch(3);
		new Thread(new OrderHashIndexCreator("orderid", query1Path ,query1IndexWriters, orderFiles,
				CommonConstants.QUERY1_ORDER_SPLIT_SIZE, CommonConstants.ORDERFILE_BLOCK_SIZE, latch,
				new String[] { "orderid" }, false, null)).start();
		new Thread(new OrderHashIndexCreator("buyerid", query2Path,query2IndexWriters, orderFiles,
				CommonConstants.QUERY2_ORDER_SPLIT_SIZE, CommonConstants.ORDERFILE_BLOCK_SIZE, latch,
				new String[] { "buyerid", "createtime" }, true, this)).start();
		new Thread(new OrderHashIndexCreator("goodid", query3Path, query3IndexWriters, orderFiles,
				CommonConstants.QUERY3_ORDER_SPLIT_SIZE, CommonConstants.ORDERFILE_BLOCK_SIZE, latch,
				new String[] { "goodid" }, true, this)).start();

		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("ORDER INDEX OK");
		latch = new CountDownLatch(2);
		new Thread(new OtherHashIndexCreator("buyerid", buyerFiles, latch, new String[] { "buyerid" }, this)).start();
		new Thread(new OtherHashIndexCreator("goodid", goodFiles, latch, new String[] { "goodid" }, this)).start();

		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

	}

	/**
	 * 索引创建目录
	 * 
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
		int count = query1Count.incrementAndGet();
		if ((count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
			System.out.println("query1count:" + count);
		}
		Row query = new Row();
		query.putKV("orderid", orderId);

		Row orderData = null;

		int index = HashUtils.indexFor(HashUtils.hashWithDistrub(orderId), CommonConstants.QUERY1_ORDER_SPLIT_SIZE);
		String indexFile = this.query1Path + File.separator + index + CommonConstants.INDEX_SUFFIX;

		String[] indexArray = null;
		try (ExtendBufferedReader indexFileReader = IOUtils.createReader(indexFile, CommonConstants.INDEX_BLOCK_SIZE)) {
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
			if ((count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
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
				orderData = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);

			} catch (IOException e) {
				// 忽略
			}
			if ((count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
				System.out.println("query1 original data time:" + (System.currentTimeMillis() - start));
			}
		} catch (IOException e) {

		}

		if (orderData == null) {
			return null;
		}
		ResultImpl result = createResultFromOrderData(orderData, createQueryKeys(keys));
		if ((count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
			System.out.println("query1 join time:" + (System.currentTimeMillis() - start));
		}
		return result;
	}

	private Row getGoodRowFromOrderData(Row orderData) {

		Row goodData = null;
		String goodId = orderData.getKV("goodid").rawValue;
		MetaTuple goodTuple = this.goodMemoryIndexMap.get(goodId);

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
			// goodsCache.put(goodId, line);
		} catch (IOException e) {
			// 忽略
		}
		return goodData;
	}

	private Row getBuyerRowFromOrderData(Row orderData) {
		Row buyerData = null;
		String buyerId = orderData.getKV("buyerid").rawValue;
		MetaTuple buyerTuple = this.buyerMemoryIndexMap.get(buyerId);

		String file = this.buyerFiles.get(buyerTuple.getFileIndex());
		byte[] content = new byte[buyerTuple.getOriginalLength()];
		try (RandomAccessFile buyerFileReader = new RandomAccessFile(file, "r")) {
			buyerFileReader.seek(buyerTuple.getOriginalOffset());
			buyerFileReader.read(content);
			String line = new String(content);
			buyerData = StringUtils.createKVMapFromLine(line, CommonConstants.SPLITTER);

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
			tag = getKeyJoin((String) keys.toArray()[0]);
		}
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
	 * 判断key的join 只对单个key有效果,根据具体的key决定join的表 order代表不join buyer代表只join buyer表
	 * good表示只join good表 返回all代表无法判断 全部join
	 * 
	 * @param key
	 * @return
	 */
	private String getKeyJoin(String key) {
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

	private HashSet<String> createQueryKeys(Collection<String> keys) {
		if (keys == null) {
			return null;
		}
		return new HashSet<String>(keys);
	}

	private Map<Integer, PriorityQueue<IndexFileTuple>> createOrderDataAccessSequence(
			Collection<byte[]> offsetRecords) {
		Map<Integer, PriorityQueue<IndexFileTuple>> result = new HashMap<>(64);
		for (byte[] e : offsetRecords) {
			IndexFileTuple tuple = new IndexFileTuple(e);
			int fileIndex = tuple.getFileIndex();
			if (result.containsKey(tuple.getFileIndex())) {
				result.get(fileIndex).offer(tuple);
			} else {
				PriorityQueue<IndexFileTuple> resultQueue = new PriorityQueue<>(50, new Comparator<IndexFileTuple>() {

					// 比较第一个String的大小即可
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

	@Override
	public void createGoodIndex() {
		// TODO Auto-generated method stub
		goodMemoryIndexMap = new HashMap<>(4194304, 1f);
	}

	@Override
	public void addTupleToGoodIndex(String key, MetaTuple goodTuple) {
		// TODO Auto-generated method stub
		goodMemoryIndexMap.put(key, goodTuple);
	}

	@Override
	public MetaTuple getGoodTupleByGoodId(String goodId) {
		// TODO Auto-generated method stub
		return goodMemoryIndexMap.get(goodId);
	}

	@Override
	public void createBuyerIndex() {
		// TODO Auto-generated method stub
		buyerMemoryIndexMap = new HashMap<>(8388608, 1f);
	}

	@Override
	public void addTupleToBuyerIndex(String key, MetaTuple buyerTuple) {
		// TODO Auto-generated method stub
		buyerMemoryIndexMap.put(key, buyerTuple);
	}

	@Override
	public MetaTuple getBuyerTupleByBuyerId(String buyerId) {
		// TODO Auto-generated method stub
		return buyerMemoryIndexMap.get(buyerId);
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
		if ((q2count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
			System.out.println("query2 count:" + q2count);
		}

		List<Row> buyerOrderResultList = new ArrayList<>(100);

		boolean validParameter = true;

		if (endTime <= 0) {
			validParameter = false;
		}

		if (endTime <= startTime) {
			validParameter = false;
		}

		if (startTime > 11668409867L) {
			validParameter = false;
		}

		if (endTime <= 1468385553L) {
			validParameter = false;
		}

		MetaTuple buyerTuple = this.buyerMemoryIndexMap.get(buyerid);
		if (buyerTuple == null) {
			validParameter = false;
		}
		List<byte[]> buyerOrderList = new ArrayList<>(100);
		if (validParameter) {
			// int index = indexFor(hashWithDistrub(buyerid),
			// CommonConstants.QUERY2_ORDER_SPLIT_SIZE);
			String indexFile = this.query2Path + File.separator + CommonConstants.INDEX_SUFFIX;

			try (RandomAccessFile indexFileReader = new RandomAccessFile(indexFile, "r")) {
				indexFileReader.seek(buyerTuple.getIndexOffset());
				int count = buyerTuple.getCount();
				for (int i = 0; i <= count - 1; i++) {
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

			} catch (IOException e) {

			}
			if ((q2count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
				System.out.println(
						"query2 index time:" + (System.currentTimeMillis() - start) + "size:" + buyerOrderList.size());
			}
		}
		if (buyerOrderList.size() > 0) {
			// System.out.println(buyerOrderList.size());
			// Row kvMap;
			Map<Integer, PriorityQueue<IndexFileTuple>> buyerOrderAccessSequence = createOrderDataAccessSequence(
					buyerOrderList);
			List<Future<List<Row>>> result = new ArrayList<>();
			for (Map.Entry<Integer, PriorityQueue<IndexFileTuple>> e : buyerOrderAccessSequence.entrySet()) {

				OrderDataSearch search = new OrderDataSearch(e.getKey(), e.getValue());
				result.add(multiQueryPool2.submit(search));
			}
			for (Future<List<Row>> f : result) {
				try {
					List<Row> list = f.get();
					if (list != null) {
						for (Row row : list) {
							buyerOrderResultList.add(row);
						}
					}
				} catch (InterruptedException | ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			if ((q2count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
				System.out.println("query2 original data time:" + (System.currentTimeMillis() - start) + "size:"
						+ buyerOrderAccessSequence.size());
			}
		} else {
			System.out.println("query2 can't find order:" + buyerid + "," + startTime + "," + endTime);
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
		if ((q2count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
			System.out.println("query2 join time:" + (System.currentTimeMillis() - start));
		}
		return joinResult;
	}

	/**
	 * 用于0或者1次join时的简化操作
	 * 
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

		public JoinOne(List<Row> orderRows, Row fixRow, Comparator<Row> comparator, String joinId,
				Collection<String> queryKeys) {
			this.orderRows = orderRows;
			this.fixRow = fixRow;
			this.comparator = comparator;
			this.joinId = joinId;
			this.queryKeys = queryKeys;
			this.orderQueue = new PriorityQueue<>(64, comparator);
			for (Row orderRow : orderRows) {
				orderQueue.offer(orderRow);
			}
			// 读取不同的good(buyer)Id
			// 查找Row(不论是从cache还是通过memoryMap的索引去原始文件查找)组成一个map供之后join
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
		 * 根据orderRow的信息查找出无重复的，且不在cache中的good/buyer的index信息 如果已经在cache中 直接放入Map
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
			Map<Integer, PriorityQueue<IndexFileTuple>> originalDataAccessSequence = createOrderDataAccessSequence(
					joinDataIndexList);
			for (Map.Entry<Integer, PriorityQueue<IndexFileTuple>> e : originalDataAccessSequence.entrySet()) {
				String file = null;
				if (joinId.equals("buyerid")) {
					file = buyerFiles.get(e.getKey());
				} else {
					file = goodFiles.get(e.getKey());
				}
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
			if (joinId == null) {
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
		if ((q3count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
			System.out.println("query3 count:" + q3count);
		}
		boolean validParameter = true;
		MetaTuple goodTuple = this.goodMemoryIndexMap.get(goodid);
		if (goodTuple == null) {
			validParameter = false;
		}
		String tag = keys != null && keys.size() == 1 ? getKeyJoin((String) keys.toArray()[0]) : "all";

		List<Row> salerGoodsList = new ArrayList<>(100);
		final Collection<String> queryKeys = keys;
		List<byte[]> offsetRecords = new ArrayList<>(100);
		if (validParameter) {
			String indexFile = this.query3Path + File.separator + CommonConstants.INDEX_SUFFIX;

			try (RandomAccessFile indexFileReader = new RandomAccessFile(indexFile, "r")) {
				indexFileReader.seek(goodTuple.getIndexOffset());
				int count = goodTuple.getCount();
				for (int i = 0; i <= count - 1; i++) {
					byte[] content = new byte[16];
					indexFileReader.read(content);
					offsetRecords.add(content);

				}
				if ((q3count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
					System.out.println("query3 index time:" + (System.currentTimeMillis() - start) + "size:"
							+ offsetRecords.size());
				}

			} catch (IOException e) {

			}
		}
		if (offsetRecords.size() > 0) {
			// System.out.println(offsetRecords.size());
			// Row kvMap;
			Map<Integer, PriorityQueue<IndexFileTuple>> buyerOrderAccessSequence = createOrderDataAccessSequence(
					offsetRecords);
			List<Future<List<Row>>> result = new ArrayList<>();
			for (Map.Entry<Integer, PriorityQueue<IndexFileTuple>> e : buyerOrderAccessSequence.entrySet()) {

				OrderDataSearch search = new OrderDataSearch(e.getKey(), e.getValue());
				result.add(multiQueryPool3.submit(search));
			}
			for (Future<List<Row>> f : result) {
				try {
					List<Row> list = f.get();
					if (list != null) {
						for (Row row : list) {
							salerGoodsList.add(row);
						}
					}
				} catch (InterruptedException | ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			if ((q3count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
				System.out.println("query3 original data time:" + (System.currentTimeMillis() - start) + "size:"
						+ buyerOrderAccessSequence.size());
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
		// 查询3可能不需要join的buyer
		String joinTable = tag.equals("buyer") || tag.equals("all") ? "buyerid" : null;
		JoinOne joinResult = new JoinOne(salerGoodsList, goodRow, comparator, joinTable, createQueryKeys(queryKeys));
		if ((q3count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
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
		if ((q4count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
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
		} else if (key.startsWith("a_b_")) { // 表示需要join buyer和order
			tag = "buyer";
		} else if (key.equals("amount") || key.startsWith("a_o_")) { // 表示只需要order数据
			tag = "order";
		} else {
			// 不可求和的字段直接返回
			return null;
		}

		List<Row> ordersData = new ArrayList<>(100);

		String indexFile = this.query3Path + File.separator + CommonConstants.INDEX_SUFFIX;
		// cachedStrings = new ArrayList<>(100);
		List<byte[]> offsetRecords = new ArrayList<>(100);
		try (RandomAccessFile indexFileReader = new RandomAccessFile(indexFile, "r")) {
			indexFileReader.seek(goodTuple.getIndexOffset());
			int count = goodTuple.getCount();
			for (int i = 0; i <= count - 1; i++) {
				byte[] content = new byte[16];
				indexFileReader.read(content);
				offsetRecords.add(content);

			}
			if ((q4count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
				System.out.println(
						"query4 index time:" + (System.currentTimeMillis() - start) + "size:" + offsetRecords.size());
			}

		} catch (IOException e) {

		}

		if (offsetRecords.size() > 0) {
			// System.out.println(offsetRecords.size());
			// Row kvMap;
			Map<Integer, PriorityQueue<IndexFileTuple>> buyerOrderAccessSequence = createOrderDataAccessSequence(
					offsetRecords);
			List<Future<List<Row>>> result = new ArrayList<>();
			for (Map.Entry<Integer, PriorityQueue<IndexFileTuple>> e : buyerOrderAccessSequence.entrySet()) {

				OrderDataSearch search = new OrderDataSearch(e.getKey(), e.getValue());
				result.add(multiQueryPool4.submit(search));
			}
			for (Future<List<Row>> f : result) {
				try {
					List<Row> list = f.get();
					if (list != null) {
						for (Row row : list) {
							ordersData.add(row);
						}
					}
				} catch (InterruptedException | ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			if ((q4count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
				System.out.println("query4 original time:" + (System.currentTimeMillis() - start) + "size:"
						+ buyerOrderAccessSequence.size());
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
		if (tag.equals("order")) { // 说明此时只有orderData 不需要join
			for (Row orderData : ordersData) {
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
			if ((q4count & (CommonConstants.QUERY_PRINT_COUNT - 1)) == 0) {
				System.out.println("query4 join time:" + (System.currentTimeMillis() - start));
			}
		}

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
	 * 当good不存在这个属性的时候，直接返回null 否则计算出good的Order数 然后相乘并返回即可
	 * 
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
			Long result = ((long) value) * count;
			return new KV(key, Long.toString(result));
		} else {
			Double result = ((double) value) * count;
			return new KV(key, Double.toString(result));
		}
	}

	private void close() {
		multiQueryPool2.shutdown();
		multiQueryPool3.shutdown();
		multiQueryPool4.shutdown();
	}
}
