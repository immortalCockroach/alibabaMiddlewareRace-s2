package com.alibaba.middleware.race;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {

	static private String booleanTrueValue = "true";
	static private String booleanFalseValue = "false";

//	final List<String> comparableKeysOrderingByOrderId;
//
//	final List<String> comparableKeysOrderingByBuyerCreateTimeOrderId;
//	final List<String> comparableKeysOrderingBySalerGoodOrderId;
//	final List<String> comparableKeysOrderingByGood;
//	final List<String> comparableKeysOrderingByGoodOrderId;
//	final List<String> comparableKeysOrderingByBuyer;

	private Collection<String> orderFiles;
	private Collection<String> goodFiles;
	private Collection<String> buyerFiles;

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
	static private class Row extends HashMap<String, KV> {
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
	 * order表的四种组织方式 针对4中查询
	 */
//	// 查询1
//	TreeMap<ComparableKeys, Row> orderDataSortedByOrder = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//	// 查询2
//	TreeMap<ComparableKeys, Row> orderDataSortedByBuyerCreateTime = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//	// 查询3
//	TreeMap<ComparableKeys, Row> orderDataSortedBySalerGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//	// 查询4
//	TreeMap<ComparableKeys, Row> orderDataSortedByGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//
//	/**
//	 * 买家和商品表的组织方式 用于查询的group
//	 */
//	// 买家信息表（按buyerId排序）
//	TreeMap<ComparableKeys, Row> buyerDataStoredByBuyer = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//	// 商品信息表（按goodId排序）
//	TreeMap<ComparableKeys, Row> goodDataStoredByGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();

	public OrderSystemImpl() {
//		comparableKeysOrderingByOrderId = new ArrayList<String>();
//		comparableKeysOrderingByBuyerCreateTimeOrderId = new ArrayList<String>();
//		comparableKeysOrderingBySalerGoodOrderId = new ArrayList<String>();
//		comparableKeysOrderingByGood = new ArrayList<String>();
//		comparableKeysOrderingByGoodOrderId = new ArrayList<String>();
//		comparableKeysOrderingByBuyer = new ArrayList<String>();
//		// 查询1
//		comparableKeysOrderingByOrderId.add("orderid");
//
//		// 查询2
//		comparableKeysOrderingByBuyerCreateTimeOrderId.add("buyerid");
//		comparableKeysOrderingByBuyerCreateTimeOrderId.add("createtime");
//		comparableKeysOrderingByBuyerCreateTimeOrderId.add("orderid");
//
//		// 查询3
//		comparableKeysOrderingBySalerGoodOrderId.add("salerid");
//		comparableKeysOrderingBySalerGoodOrderId.add("goodid");
//		comparableKeysOrderingBySalerGoodOrderId.add("orderid");
//
//		// 查询4
//		comparableKeysOrderingByGoodOrderId.add("goodid");
//		comparableKeysOrderingByGoodOrderId.add("orderid");
//
//		comparableKeysOrderingByGood.add("goodid");
//
//		comparableKeysOrderingByBuyer.add("buyerid");

	}

	public static void main(String[] args) throws IOException, InterruptedException {
		// init order system
		List<String> orderFiles = new ArrayList<String>();
		List<String> buyerFiles = new ArrayList<String>();

		List<String> goodFiles = new ArrayList<String>();
		List<String> storeFolders = new ArrayList<String>();

		orderFiles.add("order_records.txt");
		buyerFiles.add("buyer_records.txt");
		goodFiles.add("good_records.txt");
		storeFolders.add("./");

		OrderSystem os = new OrderSystemImpl();
		os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

		// 用例
//		 long orderid = 2982388;
//		 System.out.println("\n查询订单号为" + orderid + "的订单");
//		 System.out.println(os.queryOrder(orderid, null));
//		
//		 System.out.println("\n查询订单号为" + orderid +
//		 "的订单，查询的keys为空，返回订单，但没有kv数据");
//		 System.out.println(os.queryOrder(orderid, new ArrayList<String>()));
//		
//		 System.out.println("\n查询订单号为" + orderid + "的订单的contactphone, buyerid,foo, done, price字段");
//		 List<String> queryingKeys = new ArrayList<String>();
//		 queryingKeys.add("contactphone");
//		 queryingKeys.add("buyerid");
//		 queryingKeys.add("foo");
//		 queryingKeys.add("done");
//		 queryingKeys.add("price");
//		 Result result = os.queryOrder(orderid, queryingKeys);
//		 System.out.println(result);
//		 System.out.println("\n查询订单号不存在的订单");
//		 result = os.queryOrder(1111, queryingKeys);
//		 if (result == null) {
//		 System.out.println(1111 + " order not exist");
//		 }
//		long start = System.currentTimeMillis();
//		String buyerid = "tb_a99a7956-974d-459f-bb09-b7df63ed3b80";
//		long startTime = 1471025622;
//		long endTime = 1471219509;
//		System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
//		Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
//		while (it.hasNext()) {
//			System.out.println(it.next());
//		}
//		System.out.println(System.currentTimeMillis()-start);
		//
//		String goodid = "good_842195f8-ab1a-4b09-a65f-d07bdfd8f8ff";
//		String salerid = "almm_47766ea0-b8c0-4616-b3c8-35bc4433af13";
//		System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
//		long start = System.currentTimeMillis();
//		Iterator it = os.queryOrdersBySaler(salerid, goodid, null);
//		System.out.println(System.currentTimeMillis()-start);
//		while (it.hasNext()) {
//			System.out.println(it.next());
//		}
		//
		String goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
		String attr = "app_order_33_0";
		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		System.out.println(os.sumOrdersByGood(goodid, attr));

		attr = "done";
		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		KeyValue sum = os.sumOrdersByGood(goodid, attr);
		if (sum == null) {
			System.out.println("由于该字段是布尔类型，返回值是null");
		}

		attr = "foo";
		System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
		sum = os.sumOrdersByGood(goodid, attr);
		if (sum == null) {
			System.out.println("由于该字段不存在，返回值是null");
		}
	}

	private BufferedReader createReader(String file) throws FileNotFoundException {
		return new BufferedReader(new FileReader(file), 1024 * 1024);
	}

	/**
	 * 将一行的数据按照"\t"分割并使用":"构建成map
	 * 
	 * @param line
	 * @return
	 */
	private Row createKVMapFromLine(String line) {
		String[] kvs = line.split("\t");
		Row kvMap = new Row();
		for (String rawkv : kvs) {
			int p = rawkv.indexOf(':');
			String key = rawkv.substring(0, p);
			String value = rawkv.substring(p + 1);
			if (key.length() == 0 || value.length() == 0) {
				throw new RuntimeException("Bad data:" + line);
			}
			KV kv = new KV(key, value);
			kvMap.put(kv.key(), kv);
		}
		return kvMap;
	}

	private abstract class DataFileHandler {
		abstract void handleRow(Row row);

		void handle(Collection<String> files) throws IOException {
			for (String file : files) {
				BufferedReader bfr = createReader(file);
				try {
					String line = bfr.readLine();
					while (line != null) {
						Row kvMap = createKVMapFromLine(line);
						handleRow(kvMap);
						line = bfr.readLine();
					}
				} finally {
					bfr.close();
				}
			}
		}
	}

	public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException, InterruptedException {

		this.orderFiles = orderFiles;
		this.buyerFiles = buyerFiles;
		this.goodFiles = goodFiles;
		// Handling goodFiles
//		new DataFileHandler() {
//			@Override
//			void handleRow(Row row) {
//				goodDataStoredByGood.put(new ComparableKeys(comparableKeysOrderingByGood, row), row);
//			}
//		}.handle(goodFiles);
//
//		// Handling orderFiles
//		new DataFileHandler() {
//			@Override
//			void handleRow(Row row) {
//				KV goodid = row.getKV("goodid");
//				Row goodData = goodDataStoredByGood.get(new ComparableKeys(comparableKeysOrderingByGood, row));
//				if (goodData == null) {
//					throw new RuntimeException("Bad data! goodid " + goodid.rawValue + " not exist in good files");
//				}
//				KV salerid = goodData.get("salerid");
//				row.put("salerid", salerid);
//
//				orderDataSortedByOrder.put(new ComparableKeys(comparableKeysOrderingByOrderId, row), row);
//				orderDataSortedByBuyerCreateTime
//						.put(new ComparableKeys(comparableKeysOrderingByBuyerCreateTimeOrderId, row), row);
//				orderDataSortedBySalerGood.put(new ComparableKeys(comparableKeysOrderingBySalerGoodOrderId, row), row);
//				orderDataSortedByGood.put(new ComparableKeys(comparableKeysOrderingByGoodOrderId, row), row);
//			}
//		}.handle(orderFiles);
//
//		// Handling buyerFiles
//		new DataFileHandler() {
//			@Override
//			void handleRow(Row row) {
//				buyerDataStoredByBuyer.put(new ComparableKeys(comparableKeysOrderingByBuyer, row), row);
//			}
//		}.handle(buyerFiles);
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
		boolean find = false;
		for (String orderFile : this.orderFiles) {
			try (BufferedReader reader = createReader(orderFile)) {
				String line = reader.readLine();
				while (line != null) {
					Row kvMap = createKVMapFromLine(line);
					// orderId一定存在且为long
					if (orderId == kvMap.getKV("orderid").longValue) {
						orderData = kvMap;
						find = true;
						break;
					}
					line = reader.readLine();
				}
				if (find == true) {
					break;
				}
			} catch (IOException e) {
				// 忽略
			}
		}
		if (orderData == null) {
			return null;
		}

		return createResultFromOrderData(orderData, createQueryKeys(keys));
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
		boolean find = false;
		for (String buyerFile : this.buyerFiles) {
			try (BufferedReader reader = createReader(buyerFile)) {
				String line = reader.readLine();
				while (line != null) {
					Row kvMap = createKVMapFromLine(line);
					// orderId一定存在且为long
					if (buyerQuery.get("buyerid").rawValue.equals(kvMap.getKV("buyerid").rawValue)) {
						buyerData = kvMap;
						find = true;
						break;
					}
					line = reader.readLine();
				}
				if (find == true) {
					break;
				}
			} catch (IOException e) {
				// 忽略
			}
		}

		Row goodQuery = new Row(orderData.getKV("goodid"));
		// Row goodData = goodDataStoredByGood.get(new
		// ComparableKeys(comparableKeysOrderingByGood, goodQuery));
		Row goodData = null;
		find = false;
		for (String goodFile : this.goodFiles) {
			try (BufferedReader reader = createReader(goodFile)) {
				String line = reader.readLine();
				while (line != null) {
					Row kvMap = createKVMapFromLine(line);
					// orderId一定存在且为long
					if (goodQuery.get("goodid").rawValue.equals(kvMap.getKV("goodid").rawValue)) {
						goodData = kvMap;
						find = true;
						break;
					}
					line = reader.readLine();
				}
				if (find == true) {
					break;
				}
			} catch (IOException e) {
				// 忽略
			}
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
		for (String orderFile : this.orderFiles) {
			try (BufferedReader reader = createReader(orderFile)) {
				String line = reader.readLine();
				while (line != null) {
					Row kvMap = createKVMapFromLine(line);
					// orderId一定存在且为long
					long createTime = kvMap.get("createtime").longValue;
					if (kvMap.get("buyerid").rawValue.equals(buyerid) && createTime >= startTime && createTime < endTime) {
						buyerOrderQueue.offer(kvMap);
						//buyerOrderMap.put(createTime, kvMap);
					}
					line = reader.readLine();
				}
			} catch (IOException e) {
				// 忽略
			}
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
		for (String orderFile : this.orderFiles) {
			try (BufferedReader reader = createReader(orderFile)) {
				String line = reader.readLine();
				while (line != null) {
					Row kvMap = createKVMapFromLine(line);
					// orderId一定存在且为long
					if (kvMap.get("goodid").rawValue.equals(goodid)) {
						salerGoodsQueue.offer(kvMap);
						//buyerOrderMap.put(createTime, kvMap);
					}
					line = reader.readLine();
				}
			} catch (IOException e) {
				// 忽略
			}
		}

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
		for (String orderFile : this.orderFiles) {
			try (BufferedReader reader = createReader(orderFile)) {
				String line = reader.readLine();
				while (line != null) {
					Row kvMap = createKVMapFromLine(line);
					// orderId一定存在且为long
					if (kvMap.get("goodid").rawValue.equals(goodid)) {
						ordersData.add(kvMap);
						//buyerOrderMap.put(createTime, kvMap);
					}
					line = reader.readLine();
				}
			} catch (IOException e) {
				// 忽略
			}
		}
		
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
