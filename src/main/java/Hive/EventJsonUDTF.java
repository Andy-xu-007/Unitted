package Hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.List;


public class EventJsonUDTF extends GenericUDTF {

    public StructObjectInspector initialize(StructObjectInspector argOIs)
            throws UDFArgumentException {

        // 定义UDTF返回值名称和类型
        List<String> fiedlfName = new ArrayList<>();
        List<ObjectInspector> fieldType = new ArrayList<>();

        return ObjectInspectorFactory.getStandardStructObjectInspector(fiedlfName, fieldType);
    }

    @Override
    public void process(Object[] args) throws HiveException {

    }

    @Override
    public void close() throws HiveException {

    }
}
