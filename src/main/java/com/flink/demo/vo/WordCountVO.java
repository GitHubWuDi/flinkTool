package com.flink.demo.vo;

/**
 * @author wudi E-mail:wudi891012@163.com
 * @version 创建时间：2018年10月26日 下午3:16:41 类说明
 */
public class WordCountVO {

	private String word;
	private Integer count;

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "word:"+word+",count:"+count;
		
	}
	
}
