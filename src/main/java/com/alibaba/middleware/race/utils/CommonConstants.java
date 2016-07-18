package com.alibaba.middleware.race.utils;

public class CommonConstants {
	// 每次读取的块大小 用于BufferedReader和Writer的初始化
	public static final int BLOCK_SIZE = 1024 * 10;

	public static final String NEW_LINE = System.getProperty("line.separator");
	
	// order文件被切割的分数 保持2^n
	public static final int SPLIT_SIZE = 16;
	
	public static final String QUERY1_PREFIX = "query1";
	public static final String QUERY2_PREFIX = "query2";
	public static final String QUERY3_PREFIX = "query3";
	public static final String QUERY4_PREFIX = "query4";
	
	public static final String BUYERS_PREFIX = "buyers";
	public static final String GOODS_PREFIX = "goods";
	
}
