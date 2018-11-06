package com.flink.demo.util;

import java.text.SimpleDateFormat;

/**
 * @author wudi E-mail:wudi891012@163.com
 * @version 创建时间：2018年11月2日 下午6:17:40 类说明
 */
public class TimeToStamp {

	public static String date2TimeStamp(String date_str, String format) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return String.valueOf(sdf.parse(date_str).getTime() / 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	public static void main(String[] args) {
		String date2TimeStamp = date2TimeStamp("12:00:00", "HH:mm:ss");
		System.out.println(date2TimeStamp);
	}
}
