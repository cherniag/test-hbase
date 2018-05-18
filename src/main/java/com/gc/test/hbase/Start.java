package com.gc.test.hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.LongMsg;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class Start {

    private static final String TABLE_SCORES = "Scores";
    private static final byte[] COLUMN_NAME = "name".getBytes();
    private static final byte[] COLUMN_SCORE = "score".getBytes();
    private static final TableName TABLE_NAME_SCORES = TableName.valueOf(TABLE_SCORES);
    private static final byte[] QUALIFIER_1 = "q1".getBytes();
    private static final byte[] QUALIFIER_2 = "q2".getBytes();

    public static void main(String[] args) throws Throwable {
        Configuration config = getConfig();
        Connection connection = ConnectionFactory.createConnection(config);

        //createTable(connection);

        Table scoresTable = connection.getTable(TABLE_NAME_SCORES);

        //put(scoresTable);

        aggregate(config, scoresTable);
    }


    private static void put(Table scoresTable) throws IOException {
        long timestamp = System.currentTimeMillis();
        for (int i = 30; i < 40; i++) {
            byte[] row = Bytes.toBytes("row" + i);
            Put p = new Put(row);
            p.addImmutable(COLUMN_NAME, QUALIFIER_1, timestamp + i, Bytes.toBytes("row" + i));
            // value should be Long -> for LongColumnInterpreter or Double for DoubleColumnInterpreter!!!
            p.addImmutable(COLUMN_SCORE, QUALIFIER_2, timestamp + i, Bytes.toBytes((long) i));
            scoresTable.put(p);
        }
    }

    private static void createTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME_SCORES);
        tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_NAME));
        tableDescriptor.addFamily(new HColumnDescriptor(COLUMN_SCORE));
        admin.createTable(tableDescriptor);
    }

    private static void aggregate(Configuration config, Table scoresTable) throws Throwable {
        // enable org.apache.hadoop.hbase.coprocessor.AggregateImplementation!!!
        AggregationClient aggregationClient = new AggregationClient(config);
        Scan scan = new Scan();
        //scan.addFamily(COLUMN_SCORE); // specify column family
        scan.addColumn(COLUMN_SCORE, QUALIFIER_2); // specify column family with qualifier
        //scan.setTimeRange(1526645985026L, 1526645985029L); // filter by timestamp

        // Create filter, while specifying the comparison operator and comparator.
        Filter rowIdFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("row5")));
        scan.setFilter(rowIdFilter);

        // should have the same type of column values
        ColumnInterpreter<Long, Long, EmptyMsg, LongMsg, LongMsg> ci = new LongColumnInterpreter();

        Long rowCount = aggregationClient.rowCount(scoresTable, ci, scan);
        System.out.println("Row count: " + rowCount);

        Long sum = aggregationClient.sum(scoresTable, ci, scan);
        System.out.println("Sum all: " + sum);

        double average = aggregationClient.avg(scoresTable, ci, scan);
        System.out.println("Avg all: " + average);

        Long min = aggregationClient.min(scoresTable, ci, scan);
        System.out.println("Min: " + min);

        Long max = aggregationClient.max(scoresTable, ci, scan);
        System.out.println("Max: " + max);

//        Long median = aggregationClient.median(scoresTable, ci, scan);
//        System.out.println("Median: " + median);

        double std = aggregationClient.std(scoresTable, ci, scan);
        System.out.println("Std: " + std);
    }

    private static Configuration getConfig() throws IOException, ServiceException {
        // create localhost config (standalone mode)
        Configuration config = HBaseConfiguration.create();
        String path = Start.class
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

        // check hbase is run
        HBaseAdmin.checkHBaseAvailable(config);

        return config;
    }


}
