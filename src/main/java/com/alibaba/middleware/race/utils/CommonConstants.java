package com.alibaba.middleware.race.utils;

public class CommonConstants {
	// 每次读取的块大小 用于BufferedReader和Writer的初始化
	
	// 索引文件的BLOCK大小
	public static final int INDEX_BLOCK_SIZE = 1024 * 10;
	
	// order文件的大小
	public static final int ORDERFILE_BLOCK_SIZE = 1024 * 40;
	
	// 买家和商品文件的大小
	public static final int OTHERFILE_BLOCK_SIZE = 1024 * 20;

	public static final String NEW_LINE = System.getProperty("line.separator");
	
	// order文件被切割的份数 保持2^n
	public static final int QUERY1_ORDER_SPLIT_SIZE = 4096;
	
	public static final int QUERY2_ORDER_SPLIT_SIZE = 1024;
	
	public static final int QUERY3_ORDER_SPLIT_SIZE = 1024;
	
	
	public static final String INDEX_SUFFIX = "index";	
	public static final String QUERY1_PREFIX = "query1";
	public static final String QUERY2_PREFIX = "query2";
	public static final String QUERY3_PREFIX = "query3";
	public static final String QUERY4_PREFIX = "query4";
	
	public static final String BUYERS_PREFIX = "buyers";
	public static final String GOODS_PREFIX = "goods";
	
	public static final char SPLITTER = '\t';
	
	public static final int QUERY_PRINT_COUNT = 256;
	
	public static final int CACHE_PRINT_COUNT = 256;
	
	public static String booleanTrueValue = "true";
	public static String booleanFalseValue = "false";
	
}
