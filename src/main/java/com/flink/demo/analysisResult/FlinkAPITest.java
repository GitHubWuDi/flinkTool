package com.flink.demo.analysisResult;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import com.flink.demo.util.POJOTestVO;
import com.flink.demo.vo.WordCountVO;

/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2018年10月26日 下午1:53:23
* 类说明
*/
public class FlinkAPITest {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		tupleTest(env);
		env.execute("flink-api");
	}
	
	
	private static void keySelector(StreamExecutionEnvironment env) {
		List<WordCountVO> list = POJOTestVO.getPojoTest();
		DataStreamSource<WordCountVO> dataStreamSource = env.fromCollection(list);
		
		KeyedStream<WordCountVO,String> keyBy = dataStreamSource.keyBy(new KeySelector<WordCountVO, String>() {
			@Override
			public String getKey(WordCountVO value) throws Exception {
				return value.getWord();
			}
		});
		keyBy.print();
	}
	
	private static void tupleTest(StreamExecutionEnvironment env) {
		List<Tuple3<Integer,String,Integer>> list = POJOTestVO.inputTest();
		DataStreamSource<Tuple3<Integer, String, Integer>> dataSource = env.fromCollection(list);
		DataStream<Tuple3<Integer, String, Integer>> sum = dataSource.keyBy(0,1).sum(0);
		sum.writeAsText("D:\\tmp\\file\\number");
	}
	
	/**
	 * pojo实体
	 * @param env
	 */
	private static void pojoTest(StreamExecutionEnvironment env){
		List<WordCountVO> list = POJOTestVO.getPojoTest();
		DataStreamSource<WordCountVO> dataStreamSource = env.fromCollection(list);
		DataStream<WordCountVO> keyBy = dataStreamSource.keyBy("word");
		keyBy.print();
	}
	
	
	/**
	 * map API
	 * @param text
	 */
	private static void mapApiTest(DataStream<String> text) {
		DataStream<Integer> map = text.map(new MapFunction<String, Integer>() {
			@Override
			public Integer map(String value) throws Exception {
				 return Integer.parseInt(value);
			}
		});
		map.writeAsText("D:\\tmp\\file\\number-out.txt");
		
	}
	
	private static void keyApiTest(DataStream<String> text) {
		KeyedStream<String,Tuple> keyBy = text.keyBy(0);
		keyBy.print();
	}
	
	
	
}
