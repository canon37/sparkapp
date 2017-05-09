package com.zuipin.sparkapp.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @Title: DateUtil
 * @Package: com.zuipin.webapp.sparkapp
 * @author: zengxinchao
 * @date: 2016年11月22日 上午10:47:42
 * @Description: DateUtil
 */
public class DateUtil {
	public static SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static Calendar getCalendarWithoutTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar;
	}
	
	public static String getDateStr(Calendar c) {
		return FORMAT.format(c.getTime());
	}
	
	public static Date getDateWithoutTime() {
		return getCalendarWithoutTime().getTime();
	}
	
	public static String getDateStrWithoutTime() {
		return FORMAT.format(getDateWithoutTime());
	}
	
	public static String getDateStr(Date date) {
		return FORMAT.format(date);
	}
	
	public static String getCurrentDateStr() {
		return getDateStr(new Date());
	}
}
