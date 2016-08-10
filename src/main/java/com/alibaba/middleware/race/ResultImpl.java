package com.alibaba.middleware.race;

import java.util.Set;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystem.TypeException;

/**
 * 代表一行的数据 其中对Row进行了再次的封装(包含了orderid和真正的long的Row)，其中的key为orderId
 * 
 * @author immortalCockRoach
 *
 */
public class ResultImpl implements Result {
	private long orderid;
	private Row kvMap;

	private ResultImpl(long orderid, Row kv) {
		this.orderid = orderid;
		this.kvMap = kv;
	}
	
	/**
	 * 根据order good buyer来构造结果
	 * buyer和good可以为null,相当于不join
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
