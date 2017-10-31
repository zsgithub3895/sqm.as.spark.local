package com.sihuatech.sqm.spark.redis;

import java.io.Serializable;

@SuppressWarnings("serial")
public class RedisEntity implements Serializable {
	private String host;

	private int port;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

}
