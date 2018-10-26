package com.flink.demo.analysisResult;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import com.flink.demo.util.WordCountData;

/**
 * @author wudi E-mail:wudi891012@163.com
 * @version 创建时间：2018年10月25日 下午5:43:20 
 * 类说明 wordcount
 */
public class WordCount {

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);
		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
		environment.getConfig().setGlobalJobParameters(params);
		DataSet<String> text;
		if (params.has("input")) {
			text = environment.readTextFile(params.get("input"));
		} else {
			text = WordCountData.getDefaultTextLineDataSet(environment);
		}

		DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0).sum(1);
		// counts.print();
		if (params.has("output")) {
			// execute program
			counts.writeAsText(params.get("output"));
			environment.execute("WordCount1");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}
	}

	public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			String[] tokens = value.toLowerCase().split("\\W+");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}

	}

}
