package com.flink.demo.analysisResult;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wudi 
 * E-mail:wudi891012@163.com
 * @version 
 * 创建时间：2018年10月31日 上午11:20:16 类说明
 */
public class FlinkLambdaTest {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//mapLambdaTest(env);
		flatMapLambdaTest(env);
		env.execute("flink-lambda-test");
	}

	public static void mapLambdaTest(StreamExecutionEnvironment env) {
		env.fromElements(1, 2, 3).map(i -> i * i).print();
	}
	
	public static void flatMapLambdaTest (StreamExecutionEnvironment env) {
	env.fromElements(1, 2, 3).flatMap((Integer number, Collector<String> out) -> {
	    StringBuilder builder = new StringBuilder();
	    for(int i = 0; i < number; i++) {
	        builder.append("a");
	        out.collect(builder.toString());
	    }
	}).returns(Types.STRING).print();
		
}
	
	
}
