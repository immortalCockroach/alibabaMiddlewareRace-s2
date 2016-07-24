package com.alibaba.middleware.race.utils;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
/**
 * 带同步锁的LRUCache
 * @author immortalCockRoach
 *
 * @param <K>
 * @param <V>
 */
public class SimpleLRUCache<K, V> {
	private final int MAX_CACHE_SIZE;
	private Entry<K, V> first;
	private Entry<K, V> last;

	private HashMap<K, Entry<K, V>> hashMap;
	private Lock lock;
	public SimpleLRUCache(int cacheSize) {
        MAX_CACHE_SIZE = cacheSize;
        lock = new ReentrantLock();
        hashMap = new HashMap<K, Entry<K, V>>();
    }

	public void put(K key, V value) {
		lock.lock();
		try{
			Entry<K, V> entry = getEntry(key);
			if (entry == null) {
				if (hashMap.size() >= MAX_CACHE_SIZE) {
					hashMap.remove(last.key);
					removeLast();
				}
				entry = new Entry<K, V>();
				entry.key = key;
			}
			entry.value = value;
			moveToFirst(entry);
			hashMap.put(key, entry);
		} finally {
			lock.unlock();
		}
	}

	public V get(K key) {
		lock.lock();
		try{
			Entry<K, V> entry = getEntry(key);
			if (entry == null)
				return null;
			moveToFirst(entry);
			return entry.value;
		} finally {
			lock.unlock();
		}
	}

	public void remove(K key) {
		lock.lock();
		try{
			Entry<K, V> entry = getEntry(key);
			if (entry != null) {
				if (entry.pre != null)
					entry.pre.next = entry.next;
				if (entry.next != null)
					entry.next.pre = entry.pre;
				if (entry == first)
					first = entry.next;
				if (entry == last)
					last = entry.pre;
			}
			hashMap.remove(key);
		} finally {
			lock.unlock();
		}
	}

	private void moveToFirst(Entry<K, V> entry) {
		if (entry == first)
			return;
		if (entry.pre != null)
			entry.pre.next = entry.next;
		if (entry.next != null)
			entry.next.pre = entry.pre;
		if (entry == last)
			last = last.pre;

		if (first == null || last == null) {
			first = last = entry;
			return;
		}

		entry.next = first;
		first.pre = entry;
		first = entry;
		entry.pre = null;
	}

	private void removeLast() {
		if (last != null) {
			last = last.pre;
			if (last == null)
				first = null;
			else
				last.next = null;
		}
	}

	private Entry<K, V> getEntry(K key) {
		return hashMap.get(key);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Entry<K, V> entry = first;
		while (entry != null) {
			sb.append(String.format("%s:%s ", entry.key, entry.value));
			entry = entry.next;
		}
		return sb.toString();
	}

	class Entry<K, V> {
		public Entry<K, V> pre;
		public Entry<K, V> next;
		public K key;
		public V value;
	}
}
