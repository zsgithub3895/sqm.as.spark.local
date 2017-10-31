package com.sihuatech.sqm.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.GroupedData;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.sihuatech.sqm.spark.bean.EventTrendBean;
import com.sihuatech.sqm.spark.bean.ImageLoadBean;
import com.sihuatech.sqm.spark.util.PropHelper;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * 图片加载指标分析-及时率
 * 图片加载指标分析-成功率
 * @author chuql 2016年6月29日
 */
public final class ImageLoadAnalysis {
	private static Logger logger = Logger.getLogger(ImageLoadAnalysis.class);
	private static Map<String,String> rateMap = new HashMap<String,String>();
	private final static int DEFAULT_NUM_PARTITIONS = 100;
	private final static long DEFAULT_BATCH_DURATION = 1;
	private static int numPartitions = 0;
	private static AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
	private static String[] dimensions = { "provinceId","cityId","platform","deviceProvider","fwVersion"};
	
	@SuppressWarnings({ "serial" })
	public static void main(String[] args) {
		if (null == args || args.length < 2) {
			System.err.println("Usage: ImageLoadAnalysis <topics> <numPartitions>\n" +
			          "  <topics> is a list of one or more kafka topics to consume from\n"
			          + " <numPartitions> 重新设置Rdd的分区数量\n\n");
			return;
		}
	   // 获取参数
		String topics = args[0];
		String _numPartitions = args[1];
		String brokers = PropHelper.getProperty("broker.quorum");
		String _batchDuration = PropHelper.getProperty("IMAGE_LOAD_TIME");
		logger.info("parameters...\n"
				+ "brokers:" + brokers + "\n"
				+ "topics:" + topics + "\n"
				+ "batchDuration:" + _batchDuration + "\n"
				+ "numPartitions:" + _numPartitions + "\n");
		// 转换参数提供使用
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		long batchDuration = NumberUtils.toLong(_batchDuration, DEFAULT_BATCH_DURATION);
		numPartitions = NumberUtils.toInt(_numPartitions, DEFAULT_NUM_PARTITIONS);
		// spark任务初始化
		SparkConf sparkConf = new SparkConf().setAppName("ImageLoadAnalysis");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaStreamingContext jssc = new JavaStreamingContext(ctx, Durations.minutes(batchDuration));
		logger.info("开始读取内容：");
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
				jssc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParams,
				topicsSet
		);
		
		// 获取kafka的offset
		JavaPairDStream<String, String> tmpMessages = messages.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
			@Override
			public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) {
				OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
				offsetRanges.set(offsets);
				return rdd;
			}
		});
		
		JavaDStream<String> lines = tmpMessages.map(new Function<Tuple2<String, String>, String>() {
				@Override
				public String call(Tuple2<String, String> tuple2) {
					return tuple2._2();
				}
			});
			// 校验日志，过滤不符合条件的记录
			JavaDStream<String> filterLines = lines.filter(new Function<String,Boolean>(){
				@Override
				public Boolean call(String line) throws Exception {
					String[] lineArr = line.split(String.valueOf((char)0x7F),-1);
					if(lineArr.length < 10){
						return false;
					}else if(!"7".equals(lineArr[0])){
						return false;
					}else if("".equals(lineArr[8].trim())){
						return false;
					}else if("".equals(lineArr[10].trim())){
						return false;
					}else if("".equals(lineArr[11].trim())){
						return false;
					}
					return true;
				}
			});
			
			filterLines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
				@Override
				public void call(JavaRDD<String> rdd, Time time) {
					SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
					JavaRDD<ImageLoadBean> rowRDD = rdd.map(new Function<String, ImageLoadBean>() {
						public ImageLoadBean call(String line) {
							String[] lineArr = line.split(String.valueOf((char)0x7F),-1);
							String kpiUtcSec = lineArr[8].trim().substring(0, 12);
							ImageLoadBean record = new ImageLoadBean();
							record.setProvinceId(lineArr[3].trim());
							record.setCityId(lineArr[4].trim());
							record.setPlatform(lineArr[2].trim());
							record.setDeviceProvider(lineArr[5].trim());
							record.setFwVersion(lineArr[6].trim());
							record.setKpiUtcSec(kpiUtcSec);
							record.setRequests(Long.valueOf(lineArr[10].trim()));
							record.setPageDurSucCnt(Long.valueOf(lineArr[12].trim()));
							record.setPageDurLE3sCnt(Long.valueOf(lineArr[13].trim()));
							return record;
						}
					});
					logger.debug("+++[ImageLoadBean]抽象JavaBean结束，过滤NullObject开始");
					// 过滤对象为空
					JavaRDD<ImageLoadBean> rowRDD2 = rowRDD.filter(new Function<ImageLoadBean, Boolean>() {
						private static final long serialVersionUID = 1L;
						@Override
						public Boolean call(ImageLoadBean imageLoadBean) throws Exception {
							if (null == imageLoadBean) {
								return false;
							}
							return true;
						}
					});
					int tmpNumPartitions = rowRDD2.getNumPartitions();
					logger.info("rowRDD分区数：" + tmpNumPartitions);
					JavaRDD<ImageLoadBean> resRDD = null;
					if (tmpNumPartitions < numPartitions) {
						resRDD = rowRDD2.repartition(numPartitions);
					} else if (tmpNumPartitions > numPartitions){
						resRDD = rowRDD2.coalesce(numPartitions);
					} else {
						resRDD = rowRDD2;
					}
					logger.info("resRDD分区数：" + resRDD.getNumPartitions());
					logger.debug("+++[ImageLoadBean]过滤NullObject结束");
					DataFrame imageDataFrame = sqlContext.createDataFrame(resRDD, ImageLoadBean.class);
					imageDataFrame.registerTempTable("imageload");
					
					long total = imageDataFrame.count();//计算单位时间段内记录总数
					logger.info("+++[ImageLoad]图片加载日志记录数：" + total);
					if(total > 0){
						GroupedData data = imageDataFrame.cube(dimensions[0], dimensions[1],dimensions[2],dimensions[3],dimensions[4],"kpiUtcSec");
						Map<String,String> map = new LinkedHashMap<String,String>();
						map.put("pageDurSucCnt", "sum");
						map.put("pageDurLE3sCnt", "sum");
						map.put("requests", "sum");
						data.agg(map).filter("kpiUtcSec is not null").show();
						Row[] stateRow =data.agg(map).filter("kpiUtcSec is not null").collect();
						if (stateRow == null || stateRow.length == 0) {
			    			return;
			    		} else {
			    			for (Row row : stateRow) {
			    				if(row == null || row.size() == 0){
									continue;
								}
								String  pID = row.getString(0);
								String  cID = row.getString(1);
								String  pf = row.getString(2);
								String  dp = row.getString(3);
								String  fv = row.getString(4);
								if(null == pID){
									pID  = "ALL";
								}
								if(null == cID){
									cID  = "ALL";
								}
								if(null == pf){
									pf  = "ALL";
								}
								if(null == dp){
									dp  = "ALL";
								}
								if(null == fv){
									fv  = "ALL";
								}
								String key = pID + "#"+ cID + "#" + pf +"#"+ dp + "#" + fv + "\t"  + row.getString(5);
				    			String value = row.getLong(6)+ "#" + row.getLong(7)+ "#" +row.getLong(8);
				    			//logger.info("+++++++++++++++++++++++++++++++"+key+"|"+row.getLong(7));
				    			rateMap.put(key, value);
			    			}
			    		}
						save();
					}
				}
			});
			jssc.start();
			jssc.awaitTermination();
	}
	
	public static void tmpTableDimensions(SQLContext sqlContext, String[] dimension) {
		String sql = "select sum(pageDurSucCnt) as scnt,sum(pageDurLE3sCnt) as cnt,sum(requests) as total,provinceId,cityId,platform,deviceProvider,fwVersion,kpiUtcSec from imageload group by provinceId,cityId,platform,deviceProvider,fwVersion,kpiUtcSec";
		logger.debug("+++[ImageLoadBean]预分析开始，SQL：\n" + sql);
		DataFrame secondDataFrame = sqlContext.sql(sql);
		secondDataFrame.registerTempTable("cacheImageLoadBean");
		sqlContext.cacheTable("cacheImageLoadBean");
		logger.debug("+++[ImageLoadBean]分析开始");
		sparkImageByDimension(sqlContext, dimension);
		logger.debug("+++[ImageLoadBean]分析结束，分析结果记录数如下...\n");
		sqlContext.uncacheTable("cacheImageLoadBean");
	}
	private static void save(){
		/* 图片加载分析指标及时率写入Redis */
		logger.info("图片加载及时率和加载成功率 存储开始mapSize="+rateMap.size());
		 //保存分析数据
		if(rateMap != null && rateMap.size() > 0){
			IndexToRedis.imageToRedis(rateMap);
			//清空本次分析数据
			rateMap.clear();
		}
	}
	
	private static void sparkImageByDimension(SQLContext sqlContext, String[] columns) {
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
            sqlSb.append("select sum(cnt),sum(scnt),sum(total),").append(selectSb.toString()).append(" kpiUtcSec ").append(" from cacheImageLoadBean GROUP BY kpiUtcSec ");
            if (groupSb.length() > 0) {
                sqlSb.append(",").append(groupSb.substring(0, groupSb.length() - 1));
            }
            num++;
            DataFrame df = sqlContext.sql(sqlSb.toString());
            Row[] stateRow = df.collect();
            if (stateRow == null || stateRow.length == 0) {
    			return;
    		} else {
    			for (Row row : stateRow) {
    				if(0 != row.getLong(2)){
	    				 String key = row.getString(3) + "#" + row.getString(4)+ "#" + row.getString(5)+ "#" + row.getString(6)+ "#" + row.getString(7)+"\t"+row.getString(8);
	    				 String value = row.getLong(0)+ "#" + row.getLong(1)+ "#" +row.getLong(2);
	    				 rateMap.put(key, value);
    				}
    			}
    		}
        }
    }
}
