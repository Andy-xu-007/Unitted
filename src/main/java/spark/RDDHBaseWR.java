package spark;

import HbaseDemo.HBaseTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.io.IOException;

public class RDDHBaseWR {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("spark.RDDHBaseWR").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        String testCols = "andy,cammie";
        Configuration hBaseConnection = HBaseTools.HBaseConnection("NXP:test", "cf1", testCols);
        JavaRDD<Result> values = jsc.newAPIHadoopRDD(hBaseConnection, TableInputFormat.class, ImmutableBytesWritable.class, Result.class).values();
        JavaPairRDD<ImmutableBytesWritable, Put> mapToPair = values.filter(new Function<Result, Boolean>() {
            @Override
            public Boolean call(Result v1) throws Exception {
                if (v1.containsColumn(Bytes.toBytes("cf1"), Bytes.toBytes("andy"))) {
                    return false;
                }
                return true;
            }
        })
                .mapToPair(new PairFunction<Result, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(Result result) throws Exception {
                        String andy = Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("andy")));
                        String rowKey = "1514851485";
                        Put put = new Put(Bytes.toBytes(rowKey));
                        put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("andy"), Bytes.toBytes(andy));
                        return new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
                    }
                });

        JobConf jobConf = new JobConf(hBaseConnection);
        Job job = null;
        try {
            job = Job.getInstance(jobConf);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "NXP:test");
            job.setOutputFormatClass(TableOutputFormat.class);
            mapToPair.saveAsNewAPIHadoopDataset(job.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }

        SparkSession session = SparkSession.builder().getOrCreate();
        JavaRDD<Andy_caseClass> andy_session = values.map(new Function<Result, Andy_caseClass>() {
            @Override
            public Andy_caseClass call(Result v1) throws Exception {
                Andy_caseClass andy_caseClass = new Andy_caseClass();
                String andy = Bytes.toString(v1.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("andy")));
                andy_caseClass.setAndy_base(andy);
                return andy_caseClass;
            }
        });

        Dataset<Row> dataFrame = session.createDataFrame(andy_session, Andy_caseClass.class);
        dataFrame.createOrReplaceTempView("andy_table");

        String[] columns = dataFrame.columns();
        Dataset<Row> sql = session.sqlContext().sql("");
        JavaRDD<Row> rowJavaRDD = sql.toJavaRDD();

        session.close();
        jsc.close();

    }

    public static class Andy_caseClass implements Serializable {
        public String getAndy_base() {
            return andy_base;
        }

        public void setAndy_base(String andy_base) {
            this.andy_base = andy_base;
        }

        String andy_base;
    }
}
