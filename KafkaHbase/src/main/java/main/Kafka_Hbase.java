package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import main.ConnectioHbase;

public class Kafka_Hbase {

	private static Kafka_Hbase go_kafka;

	public static void main(String[] args) throws Exception {

		go_kafka = new Kafka_Hbase();

		//go_kafka.Consome();
		
		go_kafka.ReadBroLog();
		
	}

	public static void Consome() throws Exception {

		// Configure Spark to connect to Kafka running on local machine
		Map<String, Object> kafkaParams = new HashMap<String, Object>();

		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

		// Configure Spark to listen messages in topic test
		Collection<String> topics = Arrays.asList("BroLog");

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkKafka10WordCount");

		// Read messages in batch of 30 seconds
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10)); //Durations.milliseconds(10)); é bem rápido

		// Start reading messages from Kafka and get DStream
		final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		// Read value of each message from Kafka and return it
		JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String, String>, String>() {
			public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
				return kafkaRecord.value();
			}
		});

		lines.foreachRDD(rdd -> {

			System.out.println("Dados:Do RDDe" + rdd.count());

			rdd.foreachPartition(partitionOfRecords -> {

				String value;

				ConnectioHbase lo_connection = new ConnectioHbase();

				// Get Gson object
				Gson gson = new GsonBuilder().setPrettyPrinting().create();

				// parse json string to object
				Cl_BroLog lo_log;

				while (partitionOfRecords.hasNext()) {

					value = partitionOfRecords.next();

					lo_log = gson.fromJson(value, Cl_BroLog.class);

					lo_connection.M_LogPutTable(lo_log);

				}

				lo_connection.Close();

			});

		});

		jssc.start();
		jssc.awaitTermination();

	}
	
	public void ReadBroLog() throws IOException, ServiceException {
		
		ConnectioHbase lo_connection = new ConnectioHbase();
		
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ReadHbase");

		// Read messages in batch of 30 seconds
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10)); //Durations.milliseconds(10)); é bem rápido

		
		
		lo_connection.M_LogGetTable();
		
	}

	public void ReadTable(String nome) throws IOException, ServiceException {

		TableName test = TableName.valueOf("test");
		byte[] row_1 = Bytes.toBytes("row1");
		String fam = "dns";
		byte[] column1 = Bytes.toBytes("a");

		Configuration config = HBaseConfiguration.create();

		String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();

		config.addResource(new Path(path));

		Connection connection = ConnectionFactory.createConnection(config);

		HBaseAdmin.checkHBaseAvailable(config);

		Admin admin = connection.getAdmin();

		Table table = connection.getTable(test);

		// Pega dados da tabela
		Get g = new Get(row_1);
		Result r = table.get(g);
		byte[] value = r.getValue(fam.getBytes(), column1);

		System.out.println("Fetched value: " + Bytes.toString(value));
		assert Arrays.equals(Bytes.toBytes("cell_data"), value);
		System.out.println("Done. ");

	}
}
