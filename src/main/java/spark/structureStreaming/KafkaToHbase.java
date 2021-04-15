package spark.structureStreaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * @author 安安小疼
 */
public class KafkaToHbase {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaToHbase" + args[0])
                .getOrCreate();

        final Dataset<Row> line = spark.readStream()
                .format("socket")
                .option("host", "node01")
                .option("port", 9999)
                .load();

        // Encoders 隐式转换
        Dataset<Put> output = line.as(Encoders.STRING())
                .mapPartitions(new MapPartitionsFunction<String, Put>() {
                    @Override
                    public Iterator<Put> call(Iterator<String> input) throws Exception {
                        List<Put> list = new ArrayList<Put>();
                        while (input.hasNext()) {
                            String line = input.next();
                            JSONObject jsonObject = JSON.parseObject(line);
                            String ob_test = jsonObject.getString("ob_test");
                            if ("Y".equals(ob_test)) {
                                JSONObject detail = jsonObject.getJSONObject("detail");
                                String rowKey = detail.getString("rowKey");
                                Put put = new Put(Bytes.toBytes(rowKey));
                                Set<String> dSet = detail.keySet();
                                for (String d : dSet) {
                                    String value = detail.getString(d);
                                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(d), Bytes.toBytes(value));
                                }
                                list.add(put);
                            }
                        }
                        return list.iterator();
                    }
                }, Encoders.kryo(Put.class));

        StreamingQuery query = output.writeStream()
                .foreach(new HbaseSink())
                .outputMode(OutputMode.Append())
                // memory, console, foreach, foreachBatch, noop
                .format("console")
                .start();

        query.awaitTermination();
        spark.stop();
    }
}

class HbaseSink extends ForeachWriter<Put> {
    private Connection connection = null;
    private static Logger logger = LoggerFactory.getLogger(HbaseSink.class);

    @Override
    public boolean open(long partitionId, long epochId) {
        try{
            Configuration conf = HBaseConfiguration.create();
            conf.set(HConstants.ZOOKEEPER_QUORUM, "node1, node2, node3");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void process(Put put) {
        try{
            TableName table_name = TableName.valueOf("table_name");
            Table table = connection.getTable(table_name);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(Throwable errorOrNull) {
        try{
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
