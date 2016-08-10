package com.alibaba.middleware.race.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.alibaba.middleware.race.OrderSystemImpl.KV;
import com.alibaba.middleware.race.OrderSystemImpl.Row;

/**
 * Utils of string
 */
public class StringUtils {

	/**
	 * 改进版的切割方法 使用indexOf和循环来切分字符串，该方法针对普通文件的切割为Row
	 * 
	 * @param line
	 * @param splitch
	 * @return
	 */
	public static Row createKVMapFromLine(String line, char splitch) {
		int splitIndex;
		if (line.charAt(line.length() - 1) == splitch) {
			line = line.substring(0, line.length() - 1);
		}
		Row kvMap = new Row();
		String splitted;
		splitIndex = line.indexOf(splitch);
		int p;
		String key;
		String value;
		while (splitIndex != -1) {
			splitted = line.substring(0, splitIndex);
			line = line.substring(splitIndex + 1);
			p = splitted.indexOf(':');
			key = splitted.substring(0, p);
			value = splitted.substring(p + 1);
			// if (key.length() == 0 || value.length() == 0) {
			// throw new RuntimeException("Bad data:" + line);
			// }
			KV kv = new KV(key, value);
			kvMap.put(kv.key(), kv);

			splitIndex = line.indexOf(splitch);
		}
		splitted = line;
		p = splitted.indexOf(':');
		key = splitted.substring(0, p);
		value = splitted.substring(p + 1);
		// if (key.length() == 0 || value.length() == 0) {
		// throw new RuntimeException("Bad data:" + line);
		// }
		KV kv = new KV(key, value);
		kvMap.put(kv.key(), kv);
		return kvMap;
	}
	
	public static Row createKVMapFromLineWithSet2(String line, char splitch, HashSet<String> set) {
		
		int size = set.size();
		int len = line.length();
		Row kvMap = new Row();

		int currentPosition = 0;
		int start = 0;
		int middle;
		int end;
//		String value;
		char[] content = line.toCharArray();
		while (currentPosition < len && size > 0) {
			start = currentPosition;
			while (currentPosition < len && content[currentPosition] != ':') {
				currentPosition++;
			}
			middle = currentPosition;
			
			while (currentPosition < len && content[currentPosition] != splitch) {
				currentPosition++;
			}
			end = currentPosition;
			String key = new String(content,start, middle - start);
			if (set.contains(key)) {
//				kvMap.put(key, new String(content, middle + 1, end - middle));
				KV kv = new KV(key, new String(content, middle + 1, end - middle - 1));
				kvMap.put(kv.key(), kv);
				size--;
			}
			currentPosition++;
			
		}

		return kvMap;
	}
	
	public static  String[] getIndexInfo(String lineSegment) {
		String[] indexInfo = new String[3];
		for(int i = 0; i<= 1; i++) {
			int p = lineSegment.indexOf(' ');
			indexInfo[i] = lineSegment.substring(0, p);
			lineSegment = lineSegment.substring(p + 1);
		}
		indexInfo[2] = lineSegment;
		return indexInfo;
	}

	/**
	 * 将字符串转换为long或者double，无法转换的之后返回null
	 * @param value
	 * @return
	 */
	public static Object parseStringToNumber(String value) {
		
		try {
			long longValue = Long.parseLong(value);
			return longValue;
		} catch(NumberFormatException e) {
			
		}
		try {
			double doubleValue = Double.parseDouble(value);
			return doubleValue;
		} catch(NumberFormatException e) {
			
		}
		return null;
	}
	
	public static byte[] getBuyerByteArray(String buyerContent) {
		// createtime fileIndex offsetLen
		String[] content = buyerContent.split(" ");
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bo);
		
		try {
			dos.writeLong(Long.parseLong(content[0]));
			dos.writeInt(Integer.parseInt(content[1]));
			dos.writeLong(Long.parseLong(content[2]));
			dos.writeInt(Integer.parseInt(content[3]));
		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bo.toByteArray();
	}
	
	public static byte[] getGoodByteArray(String goodContent) {
		String[] content = goodContent.split(" ");
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bo);
		
		try {
			dos.writeInt(Integer.parseInt(content[0]));
			dos.writeLong(Long.parseLong(content[1]));
			dos.writeInt(Integer.parseInt(content[2]));
		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bo.toByteArray();
	}
}
