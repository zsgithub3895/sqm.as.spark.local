package com.sihuatech.sqm.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.sihuatech.sqm.spark.bean.EPGResponseBean;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

/**
 * EPG响应指标分析-平均响应时长，单位微秒
 * @author chuql 2016年6月29日
 */
public final class EPGResponseAnalysis {
	private static Logger logger = Logger.getLogger(EPGResponseAnalysis.class);
	private static final String PERIOD = "1";
	//平均响应时长单位微秒
	private static Map<String,String> epgAvgMap = new HashMap<String,String>();
	private static Map<String,String> epgTimeDelayMap = new HashMap<String,String>();

	@SuppressWarnings("serial")
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Usage: EPGResponseAnalysis <group> <topics> <numThreads>");
			System.exit(1);
		}
		String zkQuorum = PropHelper.getProperty("zk.quorum");
		String epgResponseTime = PropHelper.getProperty("EPG_RESPONSE_TIME");
		if(null != epgResponseTime && !"".equals(epgResponseTime)){
			logger.info(String.format("EPG响应指标执行参数 : \n group : %s \n topic : %s \n numThread : %s \n duration : %s minute",zkQuorum,
					args[0],args[1],args[2],epgResponseTime));
			SparkConf conf = new SparkConf().setAppName("EPGResponseAnalysis");
			JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(Long.valueOf(epgResponseTime)));
			
			//此参数为接收Topic的线程数，并非Spark分析的分区数
			int numThreads = Integer.parseInt(args[2]);
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
			String[] topics = args[1].split(",");
			for (String topic : topics) {
				topicMap.put(topic, numThreads);
			}
			JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, args[0],
					topicMap);

			JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
				@Override
				public String call(Tuple2<String, String> tuple2) {
					return tuple2._2();
				}
			});
			// 校验日志，过滤不符合条件的记录
			JavaDStream<String> filterLines = lines.filter(new Function<String,Boolean>(){
				@Override
				public Boolean call(String line) throws Exception {
					String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
					if(lineArr.length < 13){
						return false;
					}else if(!"6".equals(lineArr[0])){
						return false;
					}else if (StringUtils.isBlank(lineArr[2].trim())) {
						return false;
					}else if (StringUtils.isBlank(lineArr[3].trim())) {
						return false;
					}else if (StringUtils.isBlank(lineArr[8].trim())) {
						return false;
					}else if (StringUtils.isBlank(lineArr[11].trim())) {
						return false;
					}else if (StringUtils.isBlank(lineArr[12].trim())) {
						return false;
					}
					return true;
				}
			});
			
			filterLines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void call(JavaRDD<String> rdd, Time time) {
					String[] columns = {"provinceID","cityID","platform", "deviceProvider", "fwVersion"};
					SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());

					JavaRDD<EPGResponseBean> rowRDD = rdd.map(new Function<String, EPGResponseBean>() {
						public EPGResponseBean call(String line) {
							String[] lineArr = line.split(String.valueOf((char)0x7F), -1);
							EPGResponseBean record = new EPGResponseBean();
							record.setProvinceID(lineArr[3].trim());
							record.setCityID(lineArr[4].trim());
							record.setPlatform(lineArr[2].trim());
							record.setDeviceProvider(lineArr[5].trim());
							record.setFwVersion(lineArr[6].trim());
							record.setPageType(lineArr[8].trim());
							record.setRequests(Long.valueOf(lineArr[11].trim()));
							record.setHttpRspTime(Long.valueOf(lineArr[12].trim()));
							record.setkPIUTCSec(lineArr[9].trim());
							return record;
						}
					});
					String periodTime = DateUtil.getRTPeriodTime();
					//注册临时表
					DataFrame epgDataFrame = sqlContext.createDataFrame(rowRDD, EPGResponseBean.class);
					epgDataFrame.registerTempTable("epgresponse");
					
					long total = epgDataFrame.count();//计算单位时间段内记录总数
					logger.info("+++[EPG]EPG响应日志记录数：" + total);
					if(total > 0){
						sparkEPGAvgByDimension(sqlContext, columns);
						sparkEPGTimeDelay(sqlContext, columns);
					}
					
                    //保存分析数据
					if(epgAvgMap.size() > 0){
						save(periodTime);
						//清空本次分析数据
						epgAvgMap.clear();
					}
					if(epgTimeDelayMap.size() > 0){
						saveTimeDelay(periodTime);
						//清空本次分析数据
						epgTimeDelayMap.clear();
					}
				}
			});
			jssc.start();
			jssc.awaitTermination();
			
		}else{
			logger.info("请配置每隔多长时间取播放请求日志！！");
		}
	}

	private static void save(String time){
		logger.info("保存分析数据到存储系统开始");
		logger.info("保存到Redis");
		IndexToRedis.epgToRedisOnLong(epgAvgMap, time, PERIOD);
		logger.info("保存分析数据到存储系统结束");
	}
	
	private static void saveTimeDelay(String time){
		logger.info("EPG时延分布保存分析数据到存储系统开始");
		logger.info("保存到Redis");
		IndexToRedis.epgTimeDelayToRedis(epgTimeDelayMap, time);
		logger.info("EPG时延分布保存分析数据到存储系统结束");
	}
	
	private static void sparkEPGAvgByDimension(SQLContext sqlContext, String[] columns) {
        int num = 0;
        while (num < Math.pow(2, columns.length)) {
            StringBuffer selectSb = new StringBuffer();
            StringBuffer groupSb = new StringBuffer();
            
            for (int i = 0; i < columns.length; i++) {
                // 二进制从低位到高位取值
                int bit = (num >> i) & 1;
                // numStr += bit;
                if (bit == 1) {
                    selectSb.append(columns[i]).append(",");
                    groupSb.append(columns[i]).append(",");
                } else {
                    selectSb.append("'ALL'").append(",");
                }
            }
            StringBuffer sqlSb = new StringBuffer();
            sqlSb.append("select pageType,kPIUTCSec,sum(requests * httpRspTime) as allHttpRspTime,sum(requests) as allRequest,").append(selectSb.substring(0, selectSb.length() - 1)).append(" from epgresponse");
            if (groupSb.length() > 0) {
                sqlSb.append(" GROUP BY pageType,kPIUTCSec").append(groupSb.substring(0, groupSb.length() - 1));
            } else {
            	sqlSb.append(" GROUP BY pageType,kPIUTCSec");
            }
            num++;
            DataFrame df = sqlContext.sql(sqlSb.toString());
            Row[] stateRow = df.collect();
            if (stateRow == null || stateRow.length == 0) {
    			return;
    		} else {
    			for (Row row : stateRow) {
    				if(0 != row.getLong(3)){
	    				String key = row.getString(4) + "#" + row.getString(5) + "#" + row.getString(6) + "#" + row.getString(7) + "#" + row.getString(8)+ "\t" + row.getString(0) +"\t" + row.getString(1) ;
	    				String value = row.getLong(2) + "#" + row.getLong(3);
	    				epgAvgMap.put(key, value);
    				}
    			}
    		}
        }
    }
	private static void sparkEPGTimeDelay(SQLContext sqlContext, String[] columns) {
        int num = 0;
        while (num < Math.pow(2, columns.length)) {
            StringBuffer selectSb = new StringBuffer();
            StringBuffer groupSb = new StringBuffer();
            
            for (int i = 0; i < columns.length; i++) {
                // 二进制从低位到高位取值
                int bit = (num >> i) & 1;
                // numStr += bit;
                if (bit == 1) {
                    selectSb.append(columns[i]).append(",");
                    groupSb.append(columns[i]).append(",");
                } else {
                    selectSb.append("'ALL'").append(",");
                }
            }
            StringBuffer sqlSb = new StringBuffer();
            sqlSb.append("select pageType,kPIUTCSec,sum(case when httpRspTime >= 0 and httpRspTime < 100000 then requests else 0 end) as range1,"
            		+ "sum(case when httpRspTime >= 100000 and httpRspTime < 300000 then requests else 0 end) as range2,"
            		+ "sum(case when httpRspTime >= 300000 and httpRspTime < 2000000 then requests else 0 end) as range3,"
            		+ "sum(case when httpRspTime >= 2000000 then requests else 0 end) as range4,").append(selectSb.substring(0, selectSb.length() - 1)).append(" from epgresponse");
            if (groupSb.length() > 0) {
                sqlSb.append(" GROUP BY pageType,kPIUTCSec,").append(groupSb.substring(0, groupSb.length() - 1));
            } else {
            	sqlSb.append(" GROUP BY pageType,kPIUTCSec");
            }
            num++;
            DataFrame df = sqlContext.sql(sqlSb.toString());
            Row[] stateRow = df.collect();
            if (stateRow == null || stateRow.length == 0) {
    			return;
    		} else {
    			for (Row row : stateRow) {
    				String key = row.getString(6) + "#" + row.getString(7) + "#" + row.getString(8) + "#" + row.getString(9) + "#" + row.getString(10)+ "\t" + row.getString(0) + "\t" + row.getString(1);
    				String value = row.getLong(2) + "#" + row.getLong(3) + "#" + row.getLong(4) + "#" + row.getLong(5);
    				epgTimeDelayMap.put(key, value);
    			}
    		}
        }
    }
}
