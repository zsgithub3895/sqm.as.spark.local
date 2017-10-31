package com.sihuatech.sqm.spark.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PropHelper {
	public static final Logger log = LoggerFactory.getLogger(PropHelper.class);
	public static Properties props = new Properties();
	static {
		File conf = new File("config.properties");
		InputStream input = null;
		InputStreamReader reader = null;
		if (conf.exists()) {
			try {
				input = new FileInputStream(conf);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			input = PropHelper.class.getClassLoader().getResourceAsStream("config.properties");
		}

		if (input != null) {
			try {
				reader = new InputStreamReader(input, "UTF-8");
				props.load(reader);
				reader.close();
				input.close();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				reader = null;
				input = null;
			}
		}
	}

	public static String getProperty(String name) {
		return props.getProperty(name);
	}

	public static boolean check(String name) {
		if (props.containsKey(name) && !"".equals(props.getProperty(name).trim()))
			return true;
		return false;
	}

	public static void print() {
		Iterator<Map.Entry<Object, Object>> it = props.entrySet().iterator();
		String s = "Propertes : \n";
		while (it.hasNext()) {
			Map.Entry<Object, Object> entry = it.next();
			s += (String) entry.getKey() + " = " + (String) entry.getValue()
					+ "\n";
		}
		log.info(s);
	}
	
	public static void main(String[] args){
		System.out.println(PropHelper.getProperty("redis.server.url"));
	}
}
