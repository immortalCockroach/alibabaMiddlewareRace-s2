package com.alibaba.middleware.race;

import java.util.HashMap;


/**
 * Row为一行的数据，使用一个Map来存储 key为每行数据中的key,而value为每行数据中的k-v对
 * 
 * @author immortalCockRoach
 *
 */
@SuppressWarnings("serial")
public class Row extends HashMap<String, KV> {
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
