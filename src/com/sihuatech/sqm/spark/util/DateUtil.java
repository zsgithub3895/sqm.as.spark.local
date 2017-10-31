package com.sihuatech.sqm.spark.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.sihuatech.sqm.spark.common.Constant;

public class DateUtil {
    private static final Logger logger = Logger.getLogger(DateUtil.class);
    private static final String formatter = "yyyy-MM-dd HH:mm:ss";

    public static String getCurrentDateTime(String formatter) {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatter);
        return simpleDateFormat.format(date);
    }

    public static String formatDate(Date date,String formatter) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatter);
        return simpleDateFormat.format(date);
    }
    public static String getCurrentDateTime() {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatter);
        return simpleDateFormat.format(date);
    }

    public static Long dateTimeToLong(String date) {
        SimpleDateFormat sdf = new SimpleDateFormat(formatter);
        Date resDate = null;
        try {
            resDate = sdf.parse(date);
        } catch (ParseException e) {
            logger.error("将日期解析成Long型异常!", e);
            return (long) -1;
        }
        if (resDate != null) {
            return resDate.getTime();
        } else {
            return (long) -1;
        }
    }

    public static Long dateTimeToLong(String date, String formatter) {
        SimpleDateFormat sdf = new SimpleDateFormat(formatter);
        Date resDate = null;
        try {
            resDate = sdf.parse(date);
        } catch (ParseException e) {
            logger.error("将日期解析成Long型异常!", e);
            return (long) -1;
        }
        if (resDate != null) {
            return resDate.getTime();
        } else {
            return (long) -1;
        }
    }
    public static Date formatDateTime(String date,String formatter) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatter);
        try {
            return simpleDateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static Date addDay(Date date, int day) {
        return DateUtil.add(date, day, "day");
    }
    public static Date add(Date date, int num, String mode) {
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        if (mode.equalsIgnoreCase("month")) {
            calendar.add(Calendar.MONTH, num);
        } else {
            calendar.add(Calendar.DAY_OF_MONTH, num);
        }
        return calendar.getTime();
    }
    public static String timeToCron(String time){
        String result=null;
        if(time!=null){
            String str[]=time.split(":");
            if(str==null|str.length<3){
                return null;
            }
            String hh=str[0];
            String mi=str[1];
            String ss=str[2];
            result=String.format("%s %s %s * * ? *", ss,mi,hh);
        }
        return result;
    }
    public static int dateDiff(String fromDate, String toDate) throws ParseException {
        int days = 0;
        SimpleDateFormat df = new SimpleDateFormat(formatter);
        Date from = df.parse(fromDate);
        Date to = df.parse(toDate);
        days = (int) Math.abs((to.getTime() - from.getTime()) / (60 * 60 * 1000));
        return days;
    }
    public static int dayDiff(String fromDate, String toDate,String formatter) throws ParseException {
        int days = 0;
        SimpleDateFormat df = new SimpleDateFormat(formatter);
        Date from = df.parse(fromDate);
        Date to = df.parse(toDate);
        //开始时间和结束时间之间有多少天
        days = (int) Math.abs((to.getTime() - from.getTime()) / (1000*60 * 60 * 24));
        return days;
    }
    public static int getDiffSeconds(String fromDate, String toDate) {
        try {
            int seconds = 0;
            SimpleDateFormat df = new SimpleDateFormat(formatter);
            Date from = df.parse(fromDate);
            Date to = df.parse(toDate);
            seconds = (int) Math.abs((to.getTime() - from.getTime()) / 1000);
            return seconds;
        }catch (Exception ex){
            ex.printStackTrace();
            return -1;
        }
    }
    public static int getSeconds(String date, String formatter){
        try {
            int seconds = 0;
            SimpleDateFormat df = new SimpleDateFormat(formatter);
            Date to = df.parse(date);

            Calendar fromDate = Calendar.getInstance();
            fromDate.setTime(to);
            fromDate.set(Calendar.HOUR_OF_DAY, 0);
            fromDate.set(Calendar.MINUTE, 0);
            fromDate.set(Calendar.SECOND, 0);
            fromDate.set(Calendar.MILLISECOND, 0);
            seconds = (int) Math.abs((to.getTime() - fromDate.getTime().getTime()) / 1000);
            return seconds;
        }catch (Exception ex){
            ex.printStackTrace();
            return -1;
        }
    }
    public static String getDateBySeconds(String baseTime,int seconds){
        try {
            int sencond=seconds%60;
            int minute=(seconds%3600)/60;
            int hour=seconds/3600;

            Date current = DateUtil.formatDateTime(baseTime,formatter);
            Calendar fromDate = Calendar.getInstance();
            fromDate.setTime(current);
            fromDate.set(Calendar.HOUR_OF_DAY, hour);
            fromDate.set(Calendar.MINUTE, minute);
            fromDate.set(Calendar.SECOND, sencond);
            fromDate.set(Calendar.MILLISECOND, 0);
            return formatDate(fromDate.getTime(),formatter);
        }catch (Exception ex){
            ex.printStackTrace();
            return null;
        }
    }
    public static int getSeconds(String date) {
        return getSeconds(date,formatter);
    }


    public static String addDay(String date,String formatter, int day) {
        return formatDate(DateUtil.add(formatDateTime(date,formatter), day, "day"),formatter);
    }
    
    /**
     * 根据period获取当前时间往前回溯的有效时间
     * 因为离线分析不是分析当前时段的数据，而是前一个有效时段的数据
     * @param period
     * @return
     */
    public static Calendar getBackdatedTime(Calendar tmpc, String period) {
    	Calendar c = Calendar.getInstance();
    	c.setTime(tmpc.getTime());
		if (period.equalsIgnoreCase("15MIN")) {
			// 时间维度15分钟,当前时间向前30分钟
			c.add(Calendar.MINUTE, -30);
		} else if (period.equalsIgnoreCase("HOUR")) {
			c.add(Calendar.HOUR, -1);
		} else if (period.equalsIgnoreCase("DAY") || period.equalsIgnoreCase("PEAK")) {
			//天
		    c.add(Calendar.DAY_OF_MONTH, -1);
		} else if (period.equalsIgnoreCase("WEEK")) {
			 c.add(Calendar.WEEK_OF_MONTH, -1);
			//周  
		} else if (period.equalsIgnoreCase("MONTH")) {
			//月
			c.add(Calendar.MONTH, -1);
		} else if (period.equalsIgnoreCase("QUARTER")) {
			//季   一年共4个季度 ，第一季度：1月-3月   第二季度：4月-6月  第三季度：7月-9月   第四季度：10月-12月  	  			
			c.add(Calendar.MONTH, -3);
		} else if (period.equalsIgnoreCase("HALFYEAR")) {
			//半年  一年分为2个半年，上半年：1月-6月   下半年：7-12月
			c.add(Calendar.MONTH, -6);
		} else if (period.equalsIgnoreCase("YEAR")) {
			//年
			c.add(Calendar.YEAR, -1);
		} else{
			return null;
		}
    	return c;
    }
    
    public static Calendar getBackdatedTime(String period) {
    	Calendar c = Calendar.getInstance();
    	return getBackdatedTime(c, period);
    }
    
    /**
     * 根据用户给定的时间返回日历对象，用户可任意指定yyyyMMddHHmmdd格式，可从后往前省略
     * @param time
     * @return
     */
    public static Calendar getAppointedTime(String time) {
    	Calendar c = Calendar.getInstance();
    	int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
    	switch (time.length()/2) {
    		case(7):second = Integer.valueOf(time.substring(12, 14));
    		case(6):minute = Integer.valueOf(time.substring(10, 12));
    		case(5):hour = Integer.valueOf(time.substring(8, 10));
    		case(4):day = Integer.valueOf(time.substring(6, 8));
    		case(3):month = Integer.valueOf(time.substring(4, 6));
    		case(2):year = Integer.valueOf(time.substring(0, 4));
    		default:;
    	}
    	c.set(year, month-1,day,hour,minute,second);
    	return c;
    }

	/**
	 * 根据当前时间获取文件路径 
	 * @param filePrefix
	 *            文件名前缀,eg filePrefix=evqm-imageLoad-device-agent
	 * @param period
	 *            时间维度
	 * @return period = 15MIN  返回/2016/07/19/evqm-imageLoad-device-agent-17-00-15minute
	 *         period = HOUR  返回/2016/07/19/evqm-imageLoad-device-agent-17-
	 */
	public static String getPathPattern(String filePrefix, String period, Calendar c) {
		StringBuffer sb = new StringBuffer();
		int year = c.get(Calendar.YEAR);
		int month = c.get(Calendar.MONTH) + 1;
		int day = c.get(Calendar.DAY_OF_MONTH);
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		// 年
		sb.append(Constant.DIRECTORY_DELIMITER).append(year).append(Constant.DIRECTORY_DELIMITER);
		if(period.equalsIgnoreCase("YEAR")){
			sb.append("*").append(Constant.DIRECTORY_DELIMITER).append("*").append(Constant.DIRECTORY_DELIMITER);
			return sb.toString();
		}
		//半年
		if (period.equalsIgnoreCase("HALFYEAR")) {
			if(month >= 1 && month <= 6){
				sb.append("0[1-6]").append(Constant.DIRECTORY_DELIMITER);
			}else{
				sb.append("{07,08,09,10,11,12}").append(Constant.DIRECTORY_DELIMITER);
			}
			sb.append("*").append(Constant.DIRECTORY_DELIMITER);
			return sb.toString();
		}
		//季
		if(period.equalsIgnoreCase("QUARTER")){
		   if(month >= 1 && month <= 3){
			   sb.append("0[1-3]").append(Constant.DIRECTORY_DELIMITER);
		   }else if(month >= 4 && month <= 6){
			   sb.append("0[4-6]").append(Constant.DIRECTORY_DELIMITER);
		   }else if(month >= 7 && month <= 9){
			   sb.append("0[7-9]").append(Constant.DIRECTORY_DELIMITER);
		   }else if(month >= 10 && month <= 12){
			   sb.append("{10,11,12}").append(Constant.DIRECTORY_DELIMITER);
		   }
		   sb.append("*").append(Constant.DIRECTORY_DELIMITER);
		   return sb.toString();
		}
		// 月
		if (String.valueOf(month).length() == 1) {
			sb.append("0" + month).append(Constant.DIRECTORY_DELIMITER);
		} else {
			sb.append(month).append(Constant.DIRECTORY_DELIMITER);
		}
		if(period.equalsIgnoreCase("MONTH")){
			sb.append("*").append(Constant.DIRECTORY_DELIMITER);
			return sb.toString();
		}
		//周
		if(period.equalsIgnoreCase("WEEK")){
			orzWeekPath(c,sb);
		    return sb.toString();
		}
		// 天
		if (String.valueOf(day).length() == 1) {
			sb.append("0" + day).append(Constant.DIRECTORY_DELIMITER);
		} else {
			sb.append(day).append(Constant.DIRECTORY_DELIMITER);
		}
		if(period.equalsIgnoreCase("DAY")){
		   return sb.toString();
		}
		// 文件名前缀
		sb.append(filePrefix).append(Constant.DELIMITER);
		// 小时
		if (String.valueOf(hour).length() == 1) {
			sb.append("0" + hour).append(Constant.DELIMITER);
		} else {
			sb.append(hour).append(Constant.DELIMITER);
		}
		return sb.toString();
	}
	
	public static String getFormedTime(String period, Calendar c) {
		StringBuffer sb = new StringBuffer();
		int year = c.get(Calendar.YEAR);
		int month = c.get(Calendar.MONTH) + 1;
		int day = c.get(Calendar.DAY_OF_MONTH);
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		// 年
		sb.append(year);
		if(period.equalsIgnoreCase("YEAR")){
			return sb.toString();
		}
		//半年
		if (period.equalsIgnoreCase("HALFYEAR")) {
			if(month >= 1 && month <= 6){
				sb.append("01");
			}else{
				sb.append("07");
			}
			return sb.toString();
		}
		//季
		if(period.equalsIgnoreCase("QUARTER")){
		   if(month >= 1 && month <= 3){
			   sb.append("01");
		   }else if(month >= 4 && month <= 6){
			   sb.append("04");
		   }else if(month >= 7 && month <= 9){
			   sb.append("07");
		   }else if(month >= 10 && month <= 12){
			   sb.append("10");
		   }
		   return sb.toString();
		}
		// 月
		if (String.valueOf(month).length() == 1) {
			sb.append("0" + month);
		} else {
			sb.append(month);
		}
		if(period.equalsIgnoreCase("MONTH")){
			return sb.toString();
		}
		//周
		if(period.equalsIgnoreCase("WEEK")){
			sb.append(c.get(Calendar.WEEK_OF_MONTH));
		    return sb.toString();
		}
		// 天
		if (String.valueOf(day).length() == 1) {
			sb.append("0" + day);
		} else {
			sb.append(day);
		}
		if(period.equalsIgnoreCase("DAY")){
		   return sb.toString();
		}
		// 小时
		if (String.valueOf(hour).length() == 1) {
			sb.append("0" + hour);
		} else {
			sb.append(hour);
		}
		return sb.toString();
	}
	
	/**
	 * 获取离线分析记录保存时间点
	 * @param period
	 * @return
	 */
	public static String getIndexTime(String period, Calendar c) {
		StringBuffer sb = new StringBuffer();
		int year = c.get(Calendar.YEAR);
		int month = c.get(Calendar.MONTH) + 1;
		int day = c.get(Calendar.DAY_OF_MONTH);
		int hour = c.get(Calendar.HOUR_OF_DAY);
		int minute = c.get(Calendar.MINUTE);
		// 年
		sb.append(year);
		if(period.equalsIgnoreCase("YEAR")){
			sb.append("00000000");
			return sb.toString();
		}
		//半年
		if (period.equalsIgnoreCase("HALFYEAR")) {
			if(month >= 1 && month <= 6){
				sb.append("01000000");
			}else{
				sb.append("07000000");
			}
			return sb.toString();
		}
		//季
		if(period.equalsIgnoreCase("QUARTER")){
		   if(month >= 1 && month <= 3){
			   sb.append("01000000");
		   }else if(month >= 4 && month <= 6){
			   sb.append("04000000");
		   }else if(month >= 7 && month <= 9){
			   sb.append("07000000");
		   }else if(month >= 10 && month <= 12){
			   sb.append("10000000");
		   }
		   return sb.toString();
		}
		// 月
		if (String.valueOf(month).length() == 1) {
			sb.append("0" + month);
		} else {
			sb.append(month);
		}
		if(period.equalsIgnoreCase("MONTH")){
			sb.append("000000");
			return sb.toString();
		}
		//周、 天
		if (String.valueOf(day).length() == 1) {
			sb.append("0" + day);
		} else {
			sb.append(day);
		}
		if(period.equalsIgnoreCase("WEEK") || period.equalsIgnoreCase("DAY") || period.equalsIgnoreCase("PEAK")){
			sb.append("0000");
			return sb.toString();
		}
		// 小时
		if (String.valueOf(hour).length() == 1) {
			sb.append("0" + hour);
		} else {
			sb.append(hour);
		}
		if(period.equalsIgnoreCase("HOUR")){
			sb.append("00");
		} else { // 都没有匹配上原样输出时间
			sb.append(minute > 9 ? minute : "0" + minute);
		}
		return sb.toString();
	}
	
	/**
	 * 周维度获取HDFS文件路径逻辑复杂，单独处理
	 * @param c
	 * @param sb
	 * @return
	 */
	private static StringBuffer orzWeekPath(Calendar c ,StringBuffer sb){
		sb.delete(0,sb.length());
		//周日至周六对应的DAY_OF_WEEK值为1至7
		//上周日
		c.set(Calendar.DAY_OF_WEEK,1); 
		int year7 = c.get(Calendar.YEAR);
		int month7 = c.get(Calendar.MONTH) + 1;
		String month7Str = String.valueOf(month7).length() == 1 ? "0"+month7 : ""+month7;
		int day7 = c.get(Calendar.DAY_OF_MONTH);
		String day7Str = getDayOfMonth(c);
		//上周一		
		c.add(Calendar.WEEK_OF_MONTH, -1);
		c.set(Calendar.DAY_OF_WEEK, 2); 
		int year1 = c.get(Calendar.YEAR);
		int month1 = c.get(Calendar.MONTH) + 1;
		String month1Str = String.valueOf(month1).length() == 1 ? "0"+month1 : ""+month1;
		int day1 = c.get(Calendar.DAY_OF_MONTH);
		String day1Str = getDayOfMonth(c);
		
		//上周二
    	c.add(Calendar.DAY_OF_MONTH, 1);	
		int day2 = c.get(Calendar.DAY_OF_MONTH);
		String day2Str = getDayOfMonth(c);
		
		//上周三
    	c.add(Calendar.DAY_OF_MONTH, 1);	
		int day3 = c.get(Calendar.DAY_OF_MONTH);
		String day3Str = getDayOfMonth(c);
		
		//上周四
    	c.add(Calendar.DAY_OF_MONTH, 1);	
		int day4 = c.get(Calendar.DAY_OF_MONTH);
		String day4Str = getDayOfMonth(c);
		
		//上周五
    	c.add(Calendar.DAY_OF_MONTH, 1);	
		int day5 = c.get(Calendar.DAY_OF_MONTH);
		String day5Str = getDayOfMonth(c);
				
		//上周六
    	c.add(Calendar.DAY_OF_MONTH, 1);	
		int day6 = c.get(Calendar.DAY_OF_MONTH);
		String day6Str = getDayOfMonth(c);
			
	    if(year1 != year7 || (year1 == year7 && month1 != month7)){
	    	//一周7天跨年或跨月
	    	//Map<日期,星期>
	    	Map<Integer,Integer> map = new HashMap<Integer,Integer>();
	    	map.put(day1, 1);
	    	map.put(day2, 2);
	    	map.put(day3, 3);
	    	map.put(day4, 4);
	    	map.put(day5, 5);
	    	map.put(day6, 6);
	    	map.put(day7, 7);
	    	
	    	ArrayList<Integer> dayList = new ArrayList<Integer>(map.keySet());
	    	Collections.sort(dayList);
	    	int maxDay = dayList.get(dayList.size()-1);
	    	int max = map.get(maxDay);
			if (max == 1) {
				sb.append(Constant.DIRECTORY_DELIMITER).append("{").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
						.append(Constant.DIRECTORY_DELIMITER).append(day1Str).append(",").append(year7)
						.append(Constant.DIRECTORY_DELIMITER).append(month7Str).append(Constant.DIRECTORY_DELIMITER)
						.append(day2Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
						.append(Constant.DIRECTORY_DELIMITER).append(day3Str).append(",").append(year7)
						.append(Constant.DIRECTORY_DELIMITER).append(month7Str).append(Constant.DIRECTORY_DELIMITER)
						.append(day4Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
						.append(Constant.DIRECTORY_DELIMITER).append(day5Str).append(",").append(year7)
						.append(Constant.DIRECTORY_DELIMITER).append(month7Str).append(Constant.DIRECTORY_DELIMITER)
						.append(day6Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
						.append(Constant.DIRECTORY_DELIMITER).append(day7Str).append("}").append(Constant.DIRECTORY_DELIMITER);
			}else if (max == 2) {
				sb.append(Constant.DIRECTORY_DELIMITER).append("{").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
						.append(Constant.DIRECTORY_DELIMITER).append(day1Str).append(",").append(year1)
						.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
						.append(day2Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
						.append(Constant.DIRECTORY_DELIMITER).append(day3Str).append(",").append(year7)
						.append(Constant.DIRECTORY_DELIMITER).append(month7Str).append(Constant.DIRECTORY_DELIMITER)
						.append(day4Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
						.append(Constant.DIRECTORY_DELIMITER).append(day5Str).append(",").append(year7)
						.append(Constant.DIRECTORY_DELIMITER).append(month7Str).append(Constant.DIRECTORY_DELIMITER)
						.append(day6Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
						.append(Constant.DIRECTORY_DELIMITER).append(day7Str).append("}").append(Constant.DIRECTORY_DELIMITER);
			}else if (max == 3) {
				sb.append(Constant.DIRECTORY_DELIMITER).append("{").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day1Str).append(",").append(year1)
				.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day2Str).append(",").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day3Str).append(",").append(year7)
				.append(Constant.DIRECTORY_DELIMITER).append(month7Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day4Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day5Str).append(",").append(year7)
				.append(Constant.DIRECTORY_DELIMITER).append(month7Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day6Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day7Str).append("}").append(Constant.DIRECTORY_DELIMITER);
	        }else if (max == 4) {
				sb.append(Constant.DIRECTORY_DELIMITER).append("{").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day1Str).append(",").append(year1)
				.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day2Str).append(",").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day3Str).append(",").append(year1)
				.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day4Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day5Str).append(",").append(year7)
				.append(Constant.DIRECTORY_DELIMITER).append(month7Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day6Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day7Str).append("}").append(Constant.DIRECTORY_DELIMITER);
	        }else if (max == 5) {
				sb.append(Constant.DIRECTORY_DELIMITER).append("{").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day1Str).append(",").append(year1)
				.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day2Str).append(",").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day3Str).append(",").append(year1)
				.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day4Str).append(",").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day5Str).append(",").append(year7)
				.append(Constant.DIRECTORY_DELIMITER).append(month7Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day6Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day7Str).append("}").append(Constant.DIRECTORY_DELIMITER);
	        }else if (max == 6) {
				sb.append(Constant.DIRECTORY_DELIMITER).append("{").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day1Str).append(",").append(year1)
				.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day2Str).append(",").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day3Str).append(",").append(year1)
				.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day4Str).append(",").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day5Str).append(",").append(year1)
				.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
				.append(day6Str).append(",").append(year7).append(Constant.DIRECTORY_DELIMITER).append(month7Str)
				.append(Constant.DIRECTORY_DELIMITER).append(day7Str).append("}").append(Constant.DIRECTORY_DELIMITER);
	        }	    	
	    }else{
    		//没有跨年和跨月
    		sb.append(Constant.DIRECTORY_DELIMITER).append("{").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
			.append(Constant.DIRECTORY_DELIMITER).append(day1Str).append(",").append(year1)
			.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
			.append(day2Str).append(",").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
			.append(Constant.DIRECTORY_DELIMITER).append(day3Str).append(",").append(year1)
			.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
			.append(day4Str).append(",").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
			.append(Constant.DIRECTORY_DELIMITER).append(day5Str).append(",").append(year1)
			.append(Constant.DIRECTORY_DELIMITER).append(month1Str).append(Constant.DIRECTORY_DELIMITER)
			.append(day6Str).append(",").append(year1).append(Constant.DIRECTORY_DELIMITER).append(month1Str)
			.append(Constant.DIRECTORY_DELIMITER).append(day7Str).append("}").append(Constant.DIRECTORY_DELIMITER);
	    }
		return sb;
	}
	
	/**
	 *  获取日历的天
	 * @param c
	 * @return
	 */
	private static String getDayOfMonth(Calendar c) {
		return String.valueOf(c.get(Calendar.DAY_OF_MONTH)).length() == 1 ? "0" + c.get(Calendar.DAY_OF_MONTH)
				: "" + c.get(Calendar.DAY_OF_MONTH);
	}
	
	
	
	/**
	 * 根据维度获取Redis KEY 失效时间
	 * @param period
	 * @return
	 */
	public static int getRedisExpireTime(String period) {
		int time = 0;
		if(period.equalsIgnoreCase("YEAR")){
			// 年
			time = 2*366*24*60*60;
		}else if (period.equalsIgnoreCase("HALFYEAR")) {
			// 半年
			time = 2*183*24*60*60;
		}else if (period.equalsIgnoreCase("QUARTER")) {
			//季
			time = 2*93*24*60*60;
		}else if (period.equalsIgnoreCase("MONTH")) {
			// 月
			time = 2*31*24*60*60;
		}else if (period.equalsIgnoreCase("WEEK")) {
			//周
			time = 2*7*24*60*60;
		}else if (period.equalsIgnoreCase("DAY")) {
			// 天
			time = 2*24*60*60;
		}else if (period.equalsIgnoreCase("PEAK")) {
			// 高峰时段
			time = 2*24*60*60;
		}else if (period.equalsIgnoreCase("HOUR")) {
			// 小时
			time = 2*60*60;
		}else if (period.equalsIgnoreCase("R")) {
			//实时
			time = 2*5*60;
		}
		return time;
	}
	
	/**
	 * 获取spark实时分析数据的时间点，往前推batchDuration的整的时间点
	 * @return
	 */
	public static String getRTPeriodTime() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
		long ts = System.currentTimeMillis();
		long tmpts = ts - ts % (5 * 60 * 1000);
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(tmpts);
		c.add(Calendar.MINUTE, -5);
		return sdf.format(c.getTime());
	}
	
	/**
	 * 获取spark实时分析数据的时间点，往前推batchDuration的整的时间点
	 * @return
	 */
	public static String getQuarterOfHour(String time) {
		String hour = time.substring(0,10);
		String fif_quarter = hour + Constant.FIF_QUARTER;
		String sec_quarter = hour + Constant.SEC_QUARTER;
		String thi_quarter = hour + Constant.THI_QUARTER;
		String fou_quarter = hour + Constant.FOU_QUARTER;
		String suffix = hour+Constant.SUFFIX;
		if(time.compareTo(fif_quarter)>=0 && time.compareTo(sec_quarter)<0){
			return fif_quarter;
		}else if(time.compareTo(sec_quarter)>=0 && time.compareTo(thi_quarter)<0){
			return sec_quarter;
		}else if(time.compareTo(thi_quarter)>=0 && time.compareTo(fou_quarter)<0){
			return thi_quarter;
		}else if(time.compareTo(fou_quarter)>=0 && time.compareTo(suffix)<=0){
			return fou_quarter;
		}
		return null;
	}
}