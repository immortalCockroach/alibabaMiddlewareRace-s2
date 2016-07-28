package com.alibaba.middleware.race.utils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.alibaba.middleware.race.OrderSystemImpl.KV;
import com.alibaba.middleware.race.OrderSystemImpl.Row;

/**
 * Utils of string
 */
public class StringUtils {

	/**
	 * Splits a string into lines
	 * 
	 * @param stringToSplit
	 *            String contains a set of lines of textual content
	 * @return List of Strings where each string is a line.
	 */
	public static List<String> tokenise(String stringToSplit) {
		List<String> lines = new ArrayList<String>();

		if (StringUtils.isBlank(stringToSplit))
			return lines;
		// Use regular expression representing new lines for Unix and Windows to
		// split.
		String[] strArray = stringToSplit.split("\\r?\\n");

		if (strArray == null || strArray.length == 0)
			return lines;
		else
			return Arrays.asList(strArray);
	}

	public static String convertISOToUTF8(String origin) {
		try {
			String result = new String(origin.getBytes("ISO-8859-1"), "UTF-8");
			return result;
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static boolean isBlank(final String str) {
		int strLen;
		if (str == null || (strLen = str.length()) == 0) {
			return true;
		}
		for (int i = 0; i < strLen; i++) {
			if (Character.isWhitespace(str.charAt(i)) == false) {
				return false;
			}
		}
		return true;
	}

	public static String substring(final String str, int start) {
		if (str == null) {
			return null;
		}
		// handle negatives, which means last n characters
		if (start < 0) {
			start = str.length() + start; // remember start is negative
		}

		if (start < 0) {
			start = 0;
		}
		if (start > str.length()) {
			return "";
		}
		return str.substring(start);
	}

	public static int lastIndexOf(final String str, final String searchStr) {
		if (str == null || searchStr == null) {
			return -1;
		}
		return str.lastIndexOf(searchStr);

	}

	public static String removeEnd(String str, String remove) {
		if (str == null || remove == null) {
			return str;
		}
		if (str.endsWith(remove)) {
			return str.substring(0, str.length() - remove.length());
		}
		return str;
	}

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
	 * 将一行切割为HashMap 用于查询1和join操作
	 * @param longLine
	 * @param splitch
	 * @return
	 */
	public static HashMap<String, String> createMapFromLongLine(String longLine, char splitch) {
		int splitIndex;
		if (longLine.charAt(longLine.length() - 1) == splitch) {
			longLine = longLine.substring(0, longLine.length() - 1);
		}
		String splitted;
		splitIndex = longLine.indexOf(splitch);
		int p;
		String key;
		String value;
		HashMap<String, String> result = new HashMap<>(1024);
		while (splitIndex != -1) {
			splitted = longLine.substring(0, splitIndex);
			longLine = longLine.substring(splitIndex + 1);
			p = splitted.indexOf(':');
			key = splitted.substring(0, p);
			value = splitted.substring(p + 1);
			// if (key.length() == 0 || value.length() == 0) {
			// throw new RuntimeException("Bad data:" + line);
			// }
			result.put(key, value);

			splitIndex = longLine.indexOf(splitch);
		}
		splitted = longLine;
		p = splitted.indexOf(':');
		key = splitted.substring(0, p);
		value = splitted.substring(p + 1);
		// if (key.length() == 0 || value.length() == 0) {
		// throw new RuntimeException("Bad data:" + line);
		// }
		result.put(key, value);

		return result;
	}

	/**
	 * 特定前缀头切割的map 用于query2查询一个buyer的order信息
	 * @param longLine
	 * @param k
	 * @param splitch
	 * @return
	 */
	public static HashMap<String, String> createMapFromLongLineWithPrefixKey(String longLine, String k, char splitch) {
		int splitIndex;
		if (longLine.charAt(longLine.length() - 1) == splitch) {
			longLine = longLine.substring(0, longLine.length() - 1);
		}
		String splitted;
		splitIndex = longLine.indexOf(splitch);
		int p;
		String key;
		HashMap<String, String> result = new HashMap<>(1024);

		while (splitIndex != -1) {
			splitted = longLine.substring(0, splitIndex);
			longLine = longLine.substring(splitIndex + 1);
			p = splitted.indexOf(':');
			key = splitted.substring(0, p);
			if (key.startsWith(k)) {
				result.put(key, splitted.substring(p + 1));
			}
			splitIndex = longLine.indexOf(splitch);
		}
		splitted = longLine;
		p = splitted.indexOf(':');
		key = splitted.substring(0, p);
		if (key.startsWith(k)) {
			result.put(key, splitted.substring(p + 1));
		}

		return result;
	}

	/**
	 * 取出一行中key固定的值组成list,针对index的offSet
	 * @param longLine
	 * @param k
	 * @param splitch
	 * @return
	 */
	public static List<String> createListFromLongLineWithKey(String longLine, String k, char splitch) {
		int splitIndex;
		if (longLine.charAt(longLine.length() - 1) == splitch) {
			longLine = longLine.substring(0, longLine.length() - 1);
		}
		String splitted;
		splitIndex = longLine.indexOf(splitch);
		int p;
		String key;
		List<String> result = new ArrayList<>(1024);

		while (splitIndex != -1) {
			splitted = longLine.substring(0, splitIndex);
			longLine = longLine.substring(splitIndex + 1);
			p = splitted.indexOf(':');
			key = splitted.substring(0, p);
			if (key.startsWith(k)) {
				result.add(splitted.substring(p + 1));
			}
			splitIndex = longLine.indexOf(splitch);
		}
		splitted = longLine;
		p = splitted.indexOf(':');
		key = splitted.substring(0, p);
		if (key.startsWith(k)) {
			result.add(splitted.substring(p + 1));
		}
		// for (String rawkv : kvs) {
		// int p = rawkv.indexOf(':');
		// String key = rawkv.substring(0, p);
		// if (k.equals(key)) {
		// result.add(Long.parseLong(rawkv.substring(p + 1)));
		// }
		// }

		return result;
	}

	/**
	 * Converts an array of bytes into a string with the indicated character set
	 * enumeration
	 * 
	 * @param array
	 * @param charsetName
	 * @return
	 */
	public static String convert(byte[] array, String charsetName) {
		if (array == null || array.length == 0) {
			System.err.println("As the input byte array is empty for conversion, empty string will be returned.");
			return new String();
		} else
			return new String(array, Charset.forName(charsetName));
	}
}
