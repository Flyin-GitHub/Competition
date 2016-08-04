package com.felix;

/**
 * this class provide some useful util.
 * 
 */
public class Utils {
	/**
	 * Convert a byte array which is specified a length to int.
	 * 
	 * @param array
	 * @param len
	 * @return
	 */

	static int byte2Int(byte[] array, int len) {
		int sign = 1;
		int start = 0;

		if ('-' == array[0]) {
			sign = -1;
			start = 1;
		}

		int value = array[start];

		for (int i = start + 1; i < len; i++) {
			value += ((array[i] << 3) + (array[i] << 1));
		}
		return sign * value;

	}

	/**
	 * Convert a byte array which is specified a length to int.
	 * 
	 * @param array
	 * @param len
	 * @return
	 */
	static int byte2Int(byte[] array) {
		int sign = 1;
		int start = 0;

		if ('-' == array[0]) {
			sign = -1;
			start = 1;
		}

		int value = array[start];

		for (int i = start; array[i] != '\0'; i++) {
			value += ((array[i] << 3) + (array[i] << 1));
		}
		return sign * value;

	}
}
