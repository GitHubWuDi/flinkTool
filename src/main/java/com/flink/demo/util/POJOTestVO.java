package com.flink.demo.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;

import com.flink.demo.vo.WordCountVO;

/**
 * @author wudi E-mail:wudi891012@163.com
 * @version 创建时间：2018年10月26日 下午3:20:35 类说明
 */
public class POJOTestVO {

	public static List<WordCountVO> getPojoTest() {
		List<WordCountVO> list = new LinkedList<>();
		for (int i = 0; i < 100; i++) {
			WordCountVO wordCountVO = new WordCountVO();
			wordCountVO.setWord("wudi"+i);
			wordCountVO.setCount(i);
			list.add(wordCountVO);
		}
		return list;
	}
	
	/**
	 * 获得inputTest
	 * @return
	 */
	public static List<Tuple3<Integer, String, Integer>> inputTest(){
		final List<Tuple3<Integer, String, Integer>> input = new ArrayList<>();
		input.add(new Tuple3<>(1, "a", 1));
		input.add(new Tuple3<>(1, "a", 1));
		input.add(new Tuple3<>(2, "a", 1));
		input.add(new Tuple3<>(2, "b", 1));
		input.add(new Tuple3<>(3, "b", 1));
		input.add(new Tuple3<>(3, "c", 1));
		input.add(new Tuple3<>(1, "c", 1));
		input.add(new Tuple3<>(3, "c", 1));
		return input;
	}
	
	
	
}
