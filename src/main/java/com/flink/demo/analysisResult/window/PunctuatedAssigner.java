package com.flink.demo.analysisResult.window;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.flink.demo.vo.WaterMarkVO;

/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2018年10月31日 下午4:08:11
* 类说明
*/
public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<WaterMarkVO> {

	@Override
	public long extractTimestamp(WaterMarkVO element, long previousElementTimestamp) {
		return element.getCreateTime();
	}

	@Override
	public Watermark checkAndGetNextWatermark(WaterMarkVO lastElement, long extractedTimestamp) {
		// TODO Auto-generated method stub
		return new Watermark(extractedTimestamp);
	}

}
