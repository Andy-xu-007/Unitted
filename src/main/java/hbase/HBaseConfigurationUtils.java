package hbase;

import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HBaseConfigurationUtils {

    /**
     *
     * @param tableName
     * @param colFamily
     * @param cols
     * @param beforeDate
     * @return
     */
    public static Configuration generationHBaseConnectionScanTable(String tableName, String colFamily, String cols, LocalDate beforeDate) {
        Configuration conf = HBaseConfiguration.create();
        Configuration hbase = HBaseConfiguration.create();

        hbase.addResource(new Path("/opt/hbase-2.2.2/conf/hbase-site.xml"));
        conf.set("hbase.zookeeper.quorum", hbase.get("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort",hbase.get("hbase.zookeeper.property.clientPort"));
        // 两个集群使用相同的zookeeper集群的时候，则必须使用不同的ZK.znode.parent，因为他们不能够写在相同的文件夹内
        conf.set("zookeeper.znod.parent", hbase.get("zookeeper.znod.parent"));
        conf.setInt("hbase.rpc.timeout", 7200000);
        conf.setInt("hbase.client.operation.timeout", 7200000);
        conf.setInt("hbase.client.scanner.timeout.pariod", 7200000);
        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        Scan scan = new Scan();
        String[] selectCols = cols.split(",");
        for (String col : selectCols) {
            scan.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }

        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");

        RowFilter filter = new RowFilter(CompareOperator.EQUAL,
                new RegexStringComparator(dateFormat.format(beforeDate) + "[A-Za-z0-9]*"));
        scan.setFilter(filter);
        try {
            // 添加Scan
            ClientProtos.Scan proto = null;
            proto = ProtobufUtil.toScan(scan);
            conf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return conf;
    }

    public static Configuration generationHBaseConnectionScanTable(String tableName, String colFamily, String cols) {
        Configuration conf = HBaseConfiguration.create();
        Configuration hbase = HBaseConfiguration.create();

        hbase.addResource(new Path("/opt/hbase-2.2.2/conf/hbase-site.xml"));
        conf.set("hbase.zookeeper.quorum", hbase.get("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort",hbase.get("hbase.zookeeper.property.clientPort"));
        // 两个集群使用相同的zookeeper集群的时候，则必须使用不同的ZK.znode.parent，因为他们不能够写在相同的文件夹内
        conf.set("zookeeper.znod.parent", hbase.get("zookeeper.znod.parent"));
        conf.setInt("hbase.rpc.timeout", 7200000);
        conf.setInt("hbase.client.operation.timeout", 7200000);
        conf.setInt("hbase.client.scanner.timeout.pariod", 7200000);
        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        Scan scan = new Scan();
        String[] selectCols = cols.split(",");
        for (String col : selectCols) {
            scan.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }

        try {
            ClientProtos.Scan proto = null;
            proto = ProtobufUtil.toScan(scan);
            conf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return conf;
    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "mr");

        SparkConf conf = new SparkConf().setAppName("tmp_test" + args[0]);
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        // SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        final LocalDate nowDate = LocalDate.parse(args[0], dateTimeFormatter);

        final String dtalOptionCols = "NUM_ACC_CPT,MONTH_TR,NUM_CUST";

        Configuration dtalConfig = generationHBaseConnectionScanTable("CCRMP:GW_ONE2M_DTAL", "cf1", dtalOptionCols);
        int repartition = Integer.parseInt(args[1]);

        // 读HBase数据转化成RDD
        JavaPairRDD<ImmutableBytesWritable, Result> dtal_source = javaSparkContext
                .newAPIHadoopRDD(dtalConfig, TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .repartition(repartition);

        dtal_source.cache();  // RDD缓存
        JavaRDD<Result> dtal_sourceResult = dtal_source.values().persist(StorageLevel.MEMORY_AND_DISK());

        // 按照要求筛选数据
        JavaRDD<Result> dtal_midResult = dtal_sourceResult.filter(new Function<Result, Boolean>() {
            @Override
            public Boolean call(Result result) {
                for (String col : dtalOptionCols.split(",")) {
                    if (!result.containsColumn(Bytes.toBytes("cf1"), Bytes.toBytes(col))) {
                        return false;
                    } else if (Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes(col))) == null) {
                        return false;
                    }
                }

                int isValid = 0;
                String amtCrlm = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("AMT_CRLM"))).trim();

                if (Double.parseDouble(amtCrlm) > 0) {
                    isValid++;
                } else {
                    return false;
                }

                String balDb = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("BAL_DB"))).trim();

                if (Double.parseDouble(balDb) / Double.parseDouble(amtCrlm) > 0.5) {
                    isValid++;
                } else {
                    return false;
                }

                return (isValid > 0);
            }
        });

        dtal_midResult
                .mapToPair(new PairFunction<Result, String, Map<String, String>>() {
                    @Override
                    public Tuple2<String, Map<String, String>> call(Result result) {
                        Map<String, String> data = new HashMap<>();

                        for (String col : dtalOptionCols.split(",")) {
                            String dtal_data = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes(col))).trim();
                            data.put(col, dtal_data);
                        }

                        String numAccCpt = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("NUM_ACC_CPT"))).trim();
                        String monthTr = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("MONTH_TR"))).trim();

                        return new Tuple2<String, Map<String, String>>(numAccCpt + "|" + monthTr, data);
                    }
                }).groupByKey().distinct().mapToPair(new PairFunction<Tuple2<String, Iterable<Map<String, String>>>, String, Map<String, String>>() {
            @Override
            public Tuple2<String, Map<String, String>> call(Tuple2<String, Iterable<Map<String, String>>> arg0) throws java.lang.Exception {
                Map<String, String> result = new HashMap<>();
                result.put("NUM_ACC_CPT", arg0._1().split("\\|")[0]);
                result.put("MONTH_TR", arg0._1().split("\\|")[1]);

                Set<String> numCust = new HashSet<>();
                Iterable<Map<String, String>> dataList = arg0._2();
                for(Map<String, String> data : dataList) {
                    numCust.add(data.get("NUM_CUST"));
                }

                result.put("CUST_CNT_MONTH", Integer.toString(numCust.size()));
                return new Tuple2<String, Map<String, String>>(Integer.toString(numCust.size()), result);
            }
        }).filter(new Function<Tuple2<String, Map<String, String>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Map<String, String>> arg0) throws java.lang.Exception {
                String custCntMonth = "";
                custCntMonth = arg0._1();
                return (!("").equals(custCntMonth) && Integer.valueOf(custCntMonth) >= 5);
            }
        }).mapToPair(new PairFunction<Tuple2<String, Map<String, String>>, String, Map<String, String>>() {
            @Override
            public Tuple2<String, Map<String, String>> call(Tuple2<String, Map<String, String>> arg0) throws java.lang.Exception {
                return new Tuple2<>(arg0._2().get("NUM_ACC_CPT"), arg0._2());
            }
        });

        final String optionColsOne2MCPT = "NUM_ACC_CPT,MONTH_TR,CUST_CNT_MONTH,CUST_CNT";

        Configuration configOne2MCPT = generationHBaseConnectionScanTable("CCRMP:GW_ONE2M_CPT", "cf1", optionColsOne2MCPT);
        JobConf jobConf = new JobConf(configOne2MCPT);
        Job job = null;

        try {
           job =  Job.getInstance(jobConf);
           job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "CCRMP:GW_ONE2M_CPT");
           job.setOutputFormatClass(TableOutputFormat.class);
           // output.saveAsNewAPIHadoopDataset(job.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }

        javaSparkContext.close();

    }
}
