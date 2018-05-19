package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.google.protobuf.ServiceException;

import scala.Tuple2;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import main.ConnectioHbase;

public class Kafka_Hbase {

	private static Kafka_Hbase go_kafka;
		
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// new Kafka_Hbase().Consome();

		go_kafka = new Kafka_Hbase();

		go_kafka.Consome();
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
		Collection<String> topics = Arrays.asList("test");

		SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("SparkKafka10WordCount");

		// Read messages in batch of 30 seconds
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(15));

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

		// Break every message into words and return list of words
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}
		});

		// Take every word and return Tuple with (word,1)
		JavaPairDStream<String, Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String word) throws Exception {
				// go_kafka.ReadTable(word);
				//go_kafka.PutTable(word);
				// System.out.println("Linha Kafka:" + word);
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// Count occurance of each word
		JavaPairDStream<String, Integer> wordCount = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer first, Integer second) throws Exception {
				return first + second;
			}
		});

		words.foreachRDD(rdd -> {

			rdd.foreachPartition(partitionOfRecords -> {							
				
				String value;
				int num = 0; 
				
				ConnectioHbase connection = new ConnectioHbase();
				
				System.out.println("Dados:fora do while");
				
				while (partitionOfRecords.hasNext()) {
					
					num ++;
					
					value = partitionOfRecords.next();
					
					System.out.println("Dados:" + num + "-" + value );
					
					connection.PutTable(value, num);
				}						
				
				System.out.println("Dados:Apos o while");
				
				connection.Close();
				
			});

		});

		// Print the word count
		wordCount.print();

		jssc.start();
		jssc.awaitTermination();

	}
	
	
	public void PutTable(String nome) throws IOException, ServiceException {

		TableName test = TableName.valueOf("test");
 
		String fam = "dns";
		
		byte[] column1 = Bytes.toBytes("a");

		Configuration config = HBaseConfiguration.create();

		String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();

		config.addResource(new Path(path));

		Connection connection = ConnectionFactory.createConnection(config);
		
		HBaseAdmin.checkHBaseAvailable(config);

		Admin admin = connection.getAdmin();

		Table table = connection.getTable(test);

		// Inserir linhas na tabela
		// Row1 => Family1:Qualifier1, Family1:Qualifier2

		String rw;

		rw = "row" + getDateTime();

		System.out.println("Linha PUT:" + rw);

		byte[] row_1 = Bytes.toBytes(rw);

		Put p = new Put(row_1);

		p.addImmutable(fam.getBytes(), column1, Bytes.toBytes(nome));

		table.put(p);

		// admin.disableTable(test);
		try {
			HColumnDescriptor desc1 = new HColumnDescriptor(row_1);
			admin.addColumn(test, desc1);
			System.out.println("Success.");
		} catch (Exception e) {
			System.out.println("Failed.");
			System.out.println(e.getMessage());
		} finally {
			// admin.enableTable(test);
		}
		// System.out.println("Done. ");
		// FImInserir linhas na tabela

	}

	private String getDateTime() {
		DateFormat dateFormat = new SimpleDateFormat("dd_MM_yyyyHHmmss");
		Date date = new Date();
		return dateFormat.format(date);
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
