package HbaseDemo

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, NamespaceDescriptor, TableName}

import scala.util.{Failure, Success, Try}

class HbaseUtil {
  // 封装hbase参数
  val conf = HBaseConfiguration.create()
  // 以下K值在hbase-site.xml文件中
  conf.set("hbase.zookeeper.quorum", "node1, node2, node3")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  // conf.set ("zookeeper.znode.parent", "/hbase-unsecure") //看情况有时候要加有时候不加
  // 获取hbase连接操作，创建连接对象
  private val connection: Connection = ConnectionFactory.createConnection(conf)
  // 获取hbase的客户端操作，管理员
  private val admin: Admin = connection.getAdmin

  // 列出所有表
  def ListTables = {
    val names = admin.listTableNames()
    names.foreach(println(_))
  }

  // 判断表是否存在
  def isTableExists(tableName: String) = {
    val exist = admin.tableExists(TableName.valueOf(tableName))
    println(s"is the table $tableName exists?: ", exist)
  }

  // 创建命名空间
  def createNameSpace(ns: String) = {

    // val descriptor = new NamespaceDescriptor(ns)
    val namespaceDescriptor = NamespaceDescriptor.create(ns).build()  // 旧方法，老版本NamespaceDescriptor是private的
    Try {
      admin.createNamespace(namespaceDescriptor)
    } match {
      case Success(_) =>
      case Failure(e) => e.printStackTrace()
    }
  }

  /**
   * 扫描数据
   *
   * @param tableName  表名
   * @param cf         列族
   * @param qualifier  列限定符
   */
  def scan(tableName: String, cf: String, qualifier: String) ={
    val table  = connection.getTable(TableName.valueOf(tableName))
    val s = new Scan()  // 可以加过滤器，起始、终止行

    s.addFamily(cf.getBytes())  // 可选
    s.addColumn(cf.getBytes, qualifier.getBytes)  // 可选
    val scanner = table.getScanner(s)
    Try {
      val iterator = scanner.iterator()
      while (iterator.hasNext) {
        val result = iterator.next()
        println("Found row: " + result)
        println("Found value: " + Bytes.toString(
          result.getValue(cf.getBytes, qualifier.getBytes)
        ))

//        // 常规scan，自动获取rowKey, CF, qualifier, value
//        for (cell <- result.rawCells()) {
//          println("RowKey: " + Bytes.toString(CellUtil.cloneRow(cell)) +
//            ", cf: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
//            ", Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
//            ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)))
//        }
      }
      scanner.close()
      table.close()
    } match {
      case Success(_) =>
      case Failure(e) => e.printStackTrace()
    }
  }

  /**
   * 获取表，get
   *
   * @param tableName 表名
   * @param rowKey 行
   * @param cf 列族
   * @param qualifier 列名
   *
   * @return HBase 表
   */
  def getTable(tableName: String, rowKey: String, cf: String, qualifier: String) = {
    val table = Try(connection.getTable(TableName.valueOf(tableName)))

     table.get.close()
    table match {
      case Success(v) => v
      case Failure(e) => e.printStackTrace()
        null
    }

    // 以下是常规方法
    val table1 = connection.getTable(TableName.valueOf(tableName))
    // 1，获取get对象
    val get = new Get(Bytes.toBytes(rowKey))
    // 指定获取的列簇，列  -- 可选项
    get.addFamily(Bytes.toBytes(cf))
    get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier))
    get.readAllVersions()  // 获取旧版本放法是setMaxVersion
    get.readVersions(4)  // 旧版本方法是setVersion
    // 获取数据
    val result = table1.get(get)
    // 解析result并打印
    for (cell <- result.rawCells()) {
      println("cf: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
        ", Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
        ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)))
    }

    table1.close()
  }

  /**
   * 创建表
   *
   * @param tableName 表名
   * @param cfs  列簇
   */
  def createTable(tableName:String, cfs: String*): Any = {
    val tb = TableName.valueOf(tableName)
    // 是否有表
    if (admin.tableExists(tb)){
      println(s"the table $tableName is already exists")
      return
    }
    // 是否有列族，没有就终止
    if (cfs.length <= 0) {
      println("please enter columnFamily info")
      return
    }

    Try {
//      if (admin.tableExists(tb)) {
//        admin.disableTable(tb)
//        admin.deleteTable(tb)
//      }

      // 创建表描述器
      val tableDesc = TableDescriptorBuilder.newBuilder(tb)
      for (cf <- cfs) {
        tableDesc.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes).build())
        println(s"create columnFamily $cf")
      }
      admin.createTable(tableDesc.build())
      println("Done !")
    } match {
      case Success(_) =>
      case Failure(e) => e.printStackTrace()
    }
  }

  /**
   * 删除表 drop
   *
   * @param tableName 表名
   */
  def drop(tableName: String): Any = {
    val tb = TableName.valueOf(tableName)
    if (admin.tableExists(tb)) {
      println(s"the table $tableName is not exists")
      return
    }
    Try {
      admin.disableTable(tb)
      admin.deleteTable(tb)
    } match {
      case Success(_) =>
      case Failure(e) => e.printStackTrace()
    }
  }

  /**
   * 删除表中数据 delete
   *
   * @param tableName 表名
   * @param rowKey    行健
   * @param cf        列族
   * @param qualifier 列限定符
   *
   */
  def delete(tableName: String, rowKey: String, cf: String, qualifier: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    // getBytes: 使用平台的默认字符集将此 String 编码为 byte 序列，并将结果存储到一个新的 byte 数组中
    val info = new Delete(rowKey.getBytes)
    // 删除最近版本的，addColumns是删除所有版本的数据，可选
    // 如果加上时间戳的话，删除小于等于这个时间戳的版本
    // addColumn：要注意flush对数据的影响，因为不flush得话，删除之后，旧得数据会出现，所以慎用
    info.addColumn(cf.getBytes, qualifier.getBytes)
    table.delete(info)
    table.close()
  }

  /**
   * 往表中存放数据
   *
   * @param tableName 表名
   * @param rowKey    行健
   * @param cf        列族
   * @param qualifier 列限定符
   * @param value     具体的值
   */

  def putData(tableName: String, rowKey: String, cf: String, qualifier: String, value: String) = {
    val table = connection.getTable(TableName.valueOf(tableName))
    Try {
      val g = new Get(rowKey.getBytes)
      val result = table.get(g)
      table.close()
      // GetValue()是用来获取指定对象的属性值
      Bytes.toString(result.getValue(cf.getBytes, qualifier.getBytes))
    } match {
      case Success(_) =>
      case Failure(e) => e.printStackTrace()
        null
    }

//    // 以下是常规方法
//    // 创建put对象
//    val put = new Put(Bytes.toBytes(rowKey))
//    // 给put对象赋值
//    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(value))
//    // 插入数据
//    table.put(put)
//    table.close()

  }

  // 关闭
  def close() = {
    if (admin != null) {
      Try {
        admin.close()
      } match {
        case Success(_) =>
        case Failure(e) => e.printStackTrace()
      }
    }
    if (connection != null) {
      Try {
        connection.close()
      } match {
        case Success(_) =>
        case Failure(e) => e.printStackTrace()
      }
    }
  }
}
