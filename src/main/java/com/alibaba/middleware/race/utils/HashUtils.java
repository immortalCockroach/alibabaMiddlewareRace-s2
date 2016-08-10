package com.alibaba.middleware.race.utils;

public class HashUtils {
	public static int hashWithDistrub(Object k) {
		return k.hashCode();
	}

	public static int indexFor(int hashCode, int length) {
		return hashCode & (length - 1);
	}
}
