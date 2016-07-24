package com.alibaba.middleware.race.utils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
