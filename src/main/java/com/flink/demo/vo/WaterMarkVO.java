package com.flink.demo.vo;

/**
 * @author wudi E-mail:wudi891012@163.com
 * @version 创建时间：2018年10月31日 下午3:55:40 类说明
 */
public class WaterMarkVO {

	private long createTime; // 创建时间
	private String name;
	private Integer age;

	public WaterMarkVO() {}

	public WaterMarkVO(long createTime, String name, Integer age) {
		this.createTime = createTime;
		this.name = name;
		this.age = age;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

}
