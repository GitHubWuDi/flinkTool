package com.flink.demo.analysisResult;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author wudi 
 * E-mail:wudi891012@163.com
 * @version 
 * 创建时间：2018年10月31日 上午11:44:26 
 * 类说明 对以空格为边界的分词的统计工作
 */
public class WindowWordCount {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); //设置对应的时间类型
		StreamTableEnvironment sTableEnv = TableEnvironment.getTableEnvironment(env);
		DataStream<Tuple3<String, String, String>> mapStream = env.socketTextStream("192.168.118.81", 9999).map(new TableMapApiSplitter());
//		DataStream<Tuple2<Boolean,Tuple3<String,String,Long>>> dataStream = getStreamTableWithWindow(sTableEnv);
//		DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream = getStreamTableWithOutTime(env); //Table Convert Stream
//		DataStream<Tuple2<String, Integer>> dataStream = env.socketTextStream("192.168.118.81", 9999).flatMap(new Splitter())
//				.keyBy(0).timeWindow(Time.seconds(5)).allowedLateness(Time.seconds(5)).sum(1);
		DataStream<Tuple2<Boolean, Tuple3<String, Timestamp, Long>>> dataStream = getStreamTableWithOtherTime(sTableEnv, mapStream);
		//TODO 这里通过TABLE AND SQL 查询完成对应的数据流的处理工作
		dataStream.print();
		env.execute("Window Table WordCount");
	}

	
	private static DataStream<Tuple2<Boolean, Tuple3<String,Timestamp,Long>>> getStreamTableWithOtherTime(StreamTableEnvironment sTableEnv,DataStream<Tuple3<String, String, String>> mapStream) {
		sTableEnv.registerDataStream("table1", mapStream, "user,url,cTime.proctime");
		Table sqlQuery = sTableEnv.sqlQuery("select user,TUMBLE_END(cTime,INTERVAL '1' HOUR) AS endT,COUNT(url) as cnt from table1 group by user,TUMBLE(cTime,INTERVAL '1' HOUR)");
		TupleTypeInfo<Tuple3<String,Timestamp,Long>> tupleTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.SQL_TIMESTAMP(),Types.LONG());
		DataStream<Tuple2<Boolean,Tuple3<String,Timestamp,Long>>> dataStream = sTableEnv.toRetractStream(sqlQuery, tupleTypeInfo);
		return dataStream;
	}
	
	/*private static DataStream<Tuple2<Boolean, Tuple3<String,String,Long>>> getStreamTableWithWindow(StreamTableEnvironment sTableEnv){
		sTableEnv.registerTableSource("table1", new GeneratorTableSource("mary", new Timestamp(14400), "/www"));
		sTableEnv.registerTableSource("table2", new GeneratorTableSource("mary", new Timestamp(14400), "/www"));
		Table sqlQuery = sTableEnv.sqlQuery("select user,TUMBLE_END(cTime,INTERVAL '1' HOUR) AS endT,COUNT(url) as cnt from table1 group by user,TUMBLE(cTime,INTERVAL '1' HOUR)"); //进行query统计查询
		TupleTypeInfo<Tuple3<String,String,Long>> tupleTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.STRING(),Types.LONG());
		DataStream<Tuple2<Boolean,Tuple3<String,String,Long>>> dataStream = sTableEnv.toRetractStream(sqlQuery, tupleTypeInfo);
		return dataStream;
	} */
	
	
	/**
	 * Stream Table with out Time
	 * @param env
	 * @return
	 */
	private static DataStream<Tuple2<Boolean, Tuple2<String, Long>>> getStreamTableWithOutTime(
			StreamTableEnvironment sTableEnv,DataStream<Tuple3<String, String, String>> mapStream ) {
		sTableEnv.registerDataStream("table1",mapStream,"user,cTime,url");  //dataStream convert table (register Data Stream)
		Table sqlQuery = sTableEnv.sqlQuery("select user,COUNT(url) as cnt from table1 group by user"); //进行query统计查询
		TupleTypeInfo<Tuple2<String, Long>> tupleTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.LONG());
		DataStream<Tuple2<Boolean,Tuple2<String,Long>>> dataStream = sTableEnv.toRetractStream(sqlQuery, tupleTypeInfo);
		return dataStream;
	}
    
	/**
	 * table api 完成
	 * @author wd-pc
	 *
	 */
	public static class TableMapApiSplitter implements MapFunction<String, Tuple3<String,String,String>>{

		@Override
		public Tuple3<String, String, String> map(String value) throws Exception {
			String[] split = value.split(",");
			Tuple3<String, String, String> tuple3 = new Tuple3<String, String,String>(split[0],split[1],split[2]);
			return tuple3;
		}
		
		
	}
	
	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>>{
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (String word : sentence.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}
	
	
	public static class GeneratorTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute{

		private final String user;
		private final Timestamp cTime;
		private final String  url;
		
		
		public GeneratorTableSource(String user, Timestamp cTime, String  url) {
			this.user = user;
			this.cTime = cTime;
			this.url = url;
		}
		
		@Override
		public String explainSource() {
			return "GeneratorTableSource";
		}

		@Override
		public TypeInformation<Row> getReturnType() {
			String[] names = new String[] {"user", "cTime", "url"};
			TypeInformation[] types = 
				    new TypeInformation[] {Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING()};
			return Types.ROW(names,types);
		}

		@Override
		public TableSchema getTableSchema() {
			return new TableSchema(
					new String[] {"user", "cTime", "url"},
					new TypeInformation[] {Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING()});
		}

		@Override
		public DataStream<Row> getDataStream(StreamExecutionEnvironment arg0) {
			DataStreamSource<Row> addSource = arg0.addSource(new Generator(user, cTime, url));
			return addSource;
		}

		@Override
		public String getProctimeAttribute() {
			return "cTime";
		}

	
		
	}
	
	public static class Generator implements SourceFunction<Row>{

		private final String user;
		private Timestamp cTime;
		private String url;

		public Generator(String user, Timestamp cTime, String url) {
			this.user = user;
			this.cTime = cTime;
			this.url = url;
		}
		
		
		@Override
		public void run(SourceContext<Row> ctx) throws Exception {
			//ctx.collect(Row.of(cTime));
		}

		@Override
		public void cancel() {
			
		}

		
		
	}
	
	
	
	
}
