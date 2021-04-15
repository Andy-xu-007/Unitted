package spark.structureStreaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class KafkaToHive {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        Configuration hive = new Configuration();
        hive.addResource("");

        SparkSession spark = SparkSession.builder()
                .appName("KafkaToHive-" + args[0])
                .enableHiveSupport()
                .getOrCreate();

        final Dataset<Row> line = spark.readStream()
                .format("socket")
                .option("host", "node01")
                .option("port", 9999)
                .load();

        Dataset<String> row = line.as(Encoders.STRING())
                .mapPartitions(new MapPartitionsFunction<String, String>() {
                    @Override
                    public Iterator<String> call(Iterator<String> input) throws Exception {
                        List<String> list = new ArrayList<String>();
                        while (input.hasNext()) {
                            String line = input.next();
                            JSONObject jsonObject = JSON.parseObject(line);
                            String ob_test = jsonObject.getString("ob_test");
                            if ("Y".equals(ob_test)) {
                                JSONObject detail = jsonObject.getJSONObject("detail");
                                Set<String> kSet = detail.keySet();
                                List<String> valueList = new ArrayList<String>();
                                for (String s : kSet) {
                                    valueList.add(detail.getString(s).trim());
                                }
                                String join = StringUtils.join(valueList, ",");
                                list.add(join);
                            }
                        }
                        return list.iterator();
                    }
                }, Encoders.STRING());
        row.createOrReplaceTempView("tmp1");

        Dataset<Row> line1 = spark.sql("select split(value, ',') as a from tmp1");
        line1.createOrReplaceTempView("tmp2");
        Dataset<Row> result = spark.sql("select a[0] as name, a[1] as age, a[2] as uuid from tmp2");

        StreamingQuery query = result.writeStream()
                .outputMode(OutputMode.Append())
//                .foreach()
                .start();

        query.awaitTermination();
        spark.close();
    }
}
