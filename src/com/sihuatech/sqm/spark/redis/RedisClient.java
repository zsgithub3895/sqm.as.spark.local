package com.sihuatech.sqm.spark.redis;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;

import com.sihuatech.sqm.spark.util.PropHelper;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class RedisClient {
	private List<RedisEntity> redisEntityList = new ArrayList<RedisEntity>();
	private JedisCluster jedisCluster;

	private static final Logger logger = Logger.getLogger(RedisClient.class);

	/**
	 * 获取连接，可以重试一次
	 * 
	 * @return
	 */
	private JedisCluster getJedisClusterInstance() {
		try {
			this.getJedisCluster().get("".getBytes());
			return this.getJedisCluster();
		} catch (Exception e) {
			logger.error("获取redis连接失败！重新连接", e);
			initJedisCluster();
		}
		try {
			this.getJedisCluster().get("".getBytes());
			return this.getJedisCluster();
		} catch (Exception e) {
			logger.error("获取redis连接失败!", e);
		}
		return null;
	}

	/**
	 * 
	 * @param key
	 *            (String-String)
	 * @return
	 * @throws IOException
	 */
	public String get(String key) throws IOException {
		JedisCluster jedisCluster = null;
		try {
			jedisCluster = this.getJedisClusterInstance();
			return jedisCluster.get(key);
		} catch (Exception e) {
			logger.error("redis处理出错", e);
		}
		return null;
	}

	/**
	 * 
	 * @param key
	 *            (String-String)
	 * @param object
	 * @return
	 */
	public boolean set(String key, String object) {
		try {
			JedisCluster jedisCluster = this.getJedisClusterInstance();
			jedisCluster.set(key, object.toString());
			return true;
		} catch (Exception e) {
			logger.error("获取redis连接失败!", e);
		}
		return false;
	}

	/**
	 * 
	 * object要实现Serializable接口
	 * 
	 * @param key
	 * @param object
	 *            分为String和Object，Object要序列化
	 * @return
	 * @throws IOException
	 * @see
	 */
	public boolean setObject(String key, Object object) {
		if (object instanceof Serializable) {
			JedisCluster jedisCluster = null;
			try {
				jedisCluster = this.getJedisClusterInstance();
				jedisCluster.set(key.getBytes(), SerializeUtil.serialize(object));
				return true;
			} catch (Exception e) {
				logger.error("获取redis连接失败!", e);
			}
			return false;
		} else {
			logger.error("object没有implements Serializable接口");
			return false;
		}
	}
	
	/**
	 * 
	 *  object要实现Serializable接口
	 * @param key
	 * @param object 分为String和Object，Object要序列化
	 * @return
	 * @throws IOException 
	 * @see
	 */
	public boolean setObjectAndTime(String key, Object object,int time){
		if (object instanceof Serializable) {
			JedisCluster jedisCluster = null;
			try {
				jedisCluster = this.getJedisClusterInstance();
				jedisCluster.set(key.getBytes(), SerializeUtil.serialize(object));
				if(1 == jedisCluster.expire(key, time)){
					return true;
				}
				return false;
			} catch (Exception e) {
				logger.error("获取redis连接失败!", e);
			} 
			return false;
		} else {
			logger.error("object没有implements Serializable接口");
			return false;
		}
	}
	
	/**
	 * time 单位为秒
	 * @param key
	 * @param object
	 * @param time
	 * @return
	 */
	public boolean setAndTime(String key, String object,int time) {
		try {
			JedisCluster jedisCluster = this.getJedisClusterInstance();
			jedisCluster.set(key, object.toString());
			if(1 == jedisCluster.expire(key, time)){
				return true;
			}
			return false;
		} catch (Exception e) {
			logger.error("获取redis连接失败!", e);
		}
		return false;
	}


	/**
	 * 
	 * @param key
	 *            (bytes-bytes)
	 * @return
	 * @throws IOException
	 */
	public Object getObject(String key) throws IOException {
		JedisCluster jedisCluster = null;
		try {
			jedisCluster = this.getJedisClusterInstance();
			byte[] value = jedisCluster.get(key.getBytes());
			return SerializeUtil.unserialize(value);
		} catch (Exception e) {
			logger.error("redis处理出错", e);
		}
		return null;
	}

	/**
	 * 
	 * @param key
	 *            (bytes-bytes)
	 * @return
	 * @throws IOException
	 */
	public boolean existObject(String key) throws IOException {
		JedisCluster jedisCluster = null;
		try {
			jedisCluster = this.getJedisClusterInstance();
			if (jedisCluster.exists(key.getBytes())) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			logger.error("redis处理出错", e);
		}
		return false;
	}

	/**
	 * 
	 * @param key
	 *            (String-String)
	 * @return
	 * @throws IOException
	 */
	public boolean exist(String key) throws IOException {
		JedisCluster jedisCluster = null;
		try {
			jedisCluster = this.getJedisClusterInstance();
			if (jedisCluster.exists(key)) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			logger.error("redis处理出错", e);
		}
		return false;
	}

	/**
	 * 
	 * @param key
	 *            (String-Object)
	 * @return
	 */
	public Boolean delObject(String key) {
		JedisCluster binaryJedisCluster = null;
		try {
			binaryJedisCluster = this.getJedisClusterInstance();
			binaryJedisCluster.del(key.getBytes());
			return true;
		} catch (Exception e) {
			logger.error("redis处理出错", e);
		}
		return false;
	}

	/**
	 * 
	 * @param key
	 *            (String-String)
	 * @return
	 */
	public Boolean del(String key) {
		JedisCluster binaryJedisCluster = null;
		try {
			binaryJedisCluster = this.getJedisClusterInstance();
			binaryJedisCluster.del(key);
			return true;
		} catch (Exception e) {
			logger.error("redis处理出错", e);
		}
		return false;
	}

	/**
	 * 初始化
	 * 
	 * @throws Exception
	 */
	public void init() throws Exception {
		try {
			String server = PropHelper.getProperty("redis.server.url");
			String[] url = server.split(",");
			for (int i = 0; i < url.length; i++) {
				RedisEntity redisEntity = new RedisEntity();
				redisEntity.setHost(url[i].split(":")[0]);
				redisEntity.setPort(Integer.parseInt(url[i].split(":")[1]));
				redisEntityList.add(redisEntity);
			}
			initJedisCluster();
		} catch (Exception e) {
			throw new Exception("redis地址格式不正确", e);
		}
	}

	public void initJedisCluster() {
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		GenericObjectPoolConfig jedisPoolConfig = new GenericObjectPoolConfig();
		// 控制一个pool最多有多少个可用的的jedis实例
		jedisPoolConfig.setMaxTotal(200);
		// 最大能够保持空闲状态的对象数
		jedisPoolConfig.setMaxIdle(30);

		for (RedisEntity entity : redisEntityList) {
			jedisClusterNodes.add(new HostAndPort(entity.getHost(), entity.getPort()));
		}
		String timeout = PropHelper.getProperty("redis.timeout");
		jedisCluster = new JedisCluster(jedisClusterNodes, Integer.parseInt(timeout), jedisPoolConfig);
		setJedisCluster(jedisCluster);
	}

	private JedisCluster getJedisCluster() {
		return jedisCluster;
	}

	private void setJedisCluster(JedisCluster jedisCluster) {
		this.jedisCluster = jedisCluster;
	}

}
