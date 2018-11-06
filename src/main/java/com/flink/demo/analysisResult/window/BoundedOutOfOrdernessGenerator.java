package com.flink.demo.analysisResult.window;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.flink.demo.vo.WaterMarkVO;
import com.flink.demo.vo.WordCountVO;

/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2018年10月31日 下午3:50:23
* 类说明
*/
public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<WaterMarkVO> {

	private final long maxOutOfOrderness = 3500; // 3.5 seconds
	private long currentMaxTimestamp;
	
	@Override
	public long extractTimestamp(WaterMarkVO element, long previousElementTimestamp) {
		long timestamp = element.getCreateTime();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
	}

	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
	}
}
