package com.flink.demo.analysisResult.UserAction;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.types.Row;

/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2018年11月6日 上午11:51:21
* 类说明 事件时间
*/
public class UserEventActionSource implements StreamTableSource<Row>,DefinedRowtimeAttributes {

	@Override
	public String explainSource() {
		return "UserEventActionSource";
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[] {"UserName", "URL", "UserActionTime"};
		TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()};
		return Types.ROW(names, types);
	}

	@Override
	public TableSchema getTableSchema() {
		return new TableSchema(
				new String[] {"UserName", "URL","UserActionTime"},
				new TypeInformation[] {Types.STRING(), Types.STRING(),Types.SQL_TIMESTAMP()});
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		return Collections.
			   singletonList(new RowtimeAttributeDescriptor("UserActionTime", 
					         new ExistingField("UserActionTime"), 
					         new BoundedOutOfOrderTimestamps(100))); 
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
		DataStream<Row> mapStream = env.fromElements(
			    Row.of("wudi","/wwww",new Timestamp(System.currentTimeMillis())),
			    Row.of("keke","/ssss",new Timestamp(new Date().getTime())),
			    Row.of("keke","/qqqq",new Timestamp(addHour(new Date(), 3).getTime())),
			    Row.of("keke","/qqqq",new Timestamp(addHour(new Date(), 3).getTime())));
		//mapStream.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
		//DataStream<Row> mapStream = env.socketTextStream("192.168.118.81", 9999).map(new RowMapApiSplitter());
		return mapStream;
	}
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //设置对应的时间类型
		StreamTableEnvironment sTableEnv = TableEnvironment.getTableEnvironment(env);
		sTableEnv.registerTableSource("UserActions", new UserEventActionSource());
		Table sqlQuery = sTableEnv.sqlQuery("select UserName,COUNT(URL) AS cnt,TUMBLE_END(UserActionTime,INTERVAL '1' HOUR) AS endT from UserActions group by UserName,TUMBLE(UserActionTime,INTERVAL '1' HOUR)");
		TupleTypeInfo<Tuple3<String,Long,Timestamp>> tupleTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.LONG(),Types.SQL_TIMESTAMP());
		DataStream<Tuple2<Boolean,Tuple3<String,Long,Timestamp>>> dataStream = sTableEnv.toRetractStream(sqlQuery, tupleTypeInfo);
		dataStream.writeAsText("D:\\tmp\\file\\word-nums");
		//dataStream.print();
		env.execute();
	}
   
	public static Date addDay(Date date,int n){
		Calendar cd = Calendar.getInstance();
		cd.setTime(date);
		cd.add(Calendar.DATE, n);// 增加小时
		return cd.getTime();
	}
	
	public static Date addHour(Date date,int n){
		Calendar cd = Calendar.getInstance();
		cd.setTime(date);
		cd.add(Calendar.HOUR, n);// 增加小时
		return cd.getTime();
	}
	
	
}
