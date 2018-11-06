package com.flink.demo.analysisResult.UserAction;

import java.sql.Timestamp;

import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.table.api.WindowedTable;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
* @author wudi E-mail:wudi891012@163.com
* @version 创建时间：2018年11月5日 下午4:11:53
* 类说明 处理时间
*/
public class UserProcessActionSource implements StreamTableSource<Row>,DefinedProctimeAttribute{

	@Override
	public String explainSource() {
		return "UserActionSource";
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		String[] names = new String[]{"UserName","URL","UserActionTime"};
		TypeInformation[] types = new TypeInformation[] {Types.STRING(), Types.STRING(),Types.SQL_TIMESTAMP()};
		return Types.ROW(names, types);
	}

	@Override
	public TableSchema getTableSchema() {
		return new TableSchema(
				new String[] {"UserName", "URL","UserActionTime"},
				new TypeInformation[] {Types.STRING(), Types.STRING(),Types.SQL_TIMESTAMP()});
	}

	@Override
	public String getProctimeAttribute() {
		return "UserActionTime";
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env){
		DataStream<Row> mapStream = env.fromElements(
			    Row.of("wudi","/wwww"),
			    Row.of("keke","/ssss"),
			    Row.of("keke","/qqqq"),
			    Row.of("keke","/pppp"));
		//DataStream<Row> mapStream = env.socketTextStream("192.168.118.81", 9999).map(new RowMapApiSplitter());
		return mapStream;
	}
     
	
	public static class RowMapApiSplitter implements MapFunction<String, Row>{
		@Override
		public Row map(String value) throws Exception {
			// TODO Auto-generated method stub
			String[] split = value.split(",");
			Row row = Row.of(split[0],split[1]);
			return row;
		}	
	}
	
	
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); //设置对应的时间类型
		StreamTableEnvironment sTableEnv = TableEnvironment.getTableEnvironment(env);
		//
		//TUMBLE_END(UserActionTime,INTERVAL '1' HOUR)
		// 
		sTableEnv.registerTableSource("UserActions", new UserProcessActionSource());
		Table sqlQuery = sTableEnv.sqlQuery("select UserName,COUNT(URL),TUMBLE_END(UserActionTime,INTERVAL '1' HOUR) from UserActions group by UserName,TUMBLE(UserActionTime,INTERVAL '1' HOUR)");
		TupleTypeInfo<Tuple3<String,Long,Timestamp>> tupleTypeInfo = new TupleTypeInfo<>(Types.STRING(),Types.LONG(),Types.SQL_TIMESTAMP());
		DataStream<Tuple2<Boolean,Tuple3<String,Long,Timestamp>>> dataStream = sTableEnv.toRetractStream(sqlQuery, tupleTypeInfo);
		dataStream.writeAsText("D:\\tmp\\file\\word-nums");
		//dataStream.print();
		env.execute();
	}
	
	
}
