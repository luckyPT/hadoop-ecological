package com.pt.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class HbaseUtil {
    private static final String TABLE_NAME = "test";
    private static final String FAMILY = "cf1";
    private static final String COLUMN = "a";

    private static Configuration conf;
    private static Connection connection;
    private static ExecutorService executorService;
    private static Admin admin;
    private static Table table;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.38.161.138");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.unsafe.stream.capability.enforce", "false");
        conf.set("hbase.cluster.distributed", "true");
        try {
            executorService = Executors.newFixedThreadPool(3);
            connection = ConnectionFactory.createConnection(conf, executorService);
            admin = connection.getAdmin();
            table = connection.getTable(TableName.valueOf(TABLE_NAME));
        } catch (Exception e) {
            System.out.println("Error:hbase init error");
        }
    }

    public static void main(String[] args) throws Exception {
        //scanData("a", "z").forEach(System.out::println);
        //addData("row2", "degh");
        getSomeRows(new ArrayList<String>() {{
            add("row1");
            add("row2");
        }}).forEach(System.out::println);
        close();
    }

    public static void addData(String rowKey, String data) throws Exception {
        Put put = new Put(Bytes.toBytes(rowKey));
        // 参数出分别：列族、列、值
        put.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUMN),
                Bytes.toBytes(data));

        table.put(put);
        System.out.println("插入一条数据成功!");
    }

    public static List<String> getSomeRows(List<String> rowkeys) throws Exception {
        List<Get> gets = rowkeys.stream().map(rowkey -> new Get(Bytes.toBytes(rowkey))).collect(Collectors.toList());
        Result[] results = table.get(gets);
        return Arrays.stream(results).map(result -> Bytes.toString(result.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUMN)))).collect(Collectors.toList());
    }

    public static String getOneDataByRowkey(String rowKey) throws Exception {
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        return Bytes.toString(result.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUMN)));
    }

    public static List<String> scanData(String startRow, String endRow) throws Exception {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(endRow));// not contain
        ResultScanner scanner = table.getScanner(scan);
        List<String> ret = new ArrayList<>();
        for (Result result : scanner) {
            String value = Bytes.toStringBinary(result.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COLUMN)));
            ret.add(value);
        }
        return ret;
    }

    public static void close() throws Exception {
        table.close();
        connection.close();
        executorService.shutdown();
    }
}
