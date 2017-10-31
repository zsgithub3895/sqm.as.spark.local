package com.sihuatech.sqm.spark.util;

public class SplitCommon {
	public static String[] split(String line){
		char s = 0x7F;
		String[] arr = line.split(String.valueOf(s),-1);
		return arr;
	}
}
