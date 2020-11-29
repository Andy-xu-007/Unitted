package HbaseDemo;

import com.twitter.chill.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTools {

    public static Configuration HBaseConnection(String tableName, String cosFamily, String cols) {
        Configuration conf = HBaseConfiguration.create();
        Configuration hbase = HBaseConfiguration.create();
        hbase.addResource(new Path("/opt/hbase-2.2.2/conf/hbase-site.xml"));
        conf.set("hbase.zookeeper.quorum",hbase.get("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort", hbase.get("hbase.zookeeper.property.clientPort"));
        conf.set("zookeeper.znode.parent", hbase.get("zookeeper.znode.parent"));
        conf.setInt("hbase.rpc.timeout", 72000000);
        conf.setInt("hbase.client.operation.timeout", 72000000);
        conf.setInt("hbase.client.scanner.timeout.period", 72000000);
        conf.setInt("hbase.client.scanner.caching", 1000);

        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        Scan scan = new Scan();
//        scan.withStartRow();
        for (String str : cols.split(",")) {
            scan.addColumn(Bytes.toBytes(cosFamily), Bytes.toBytes(str));
        }
        try {
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            conf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return conf;
    }
}
