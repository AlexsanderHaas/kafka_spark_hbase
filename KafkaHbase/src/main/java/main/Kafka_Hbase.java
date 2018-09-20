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
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;


import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import main.ConnectioHbase;
import scala.Tuple2;


//Add 21/08
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

//Add 16/09
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
//import javax.json.stream.JsonParserFactory;
//import org.json.simple.parser.JSONParser;

public class Kafka_Hbase {

	private static Kafka_Hbase go_kafka;

	public static void main(String[] args) throws Exception {

		go_kafka = new Kafka_Hbase();

		go_kafka.Consome();
		
		//go_kafka.ReadBroLog();
		
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
		Collection<String> topics = Arrays.asList("BroLogConn");

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("BroLogConn");
		
		// Read messages in batch of 30 seconds
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3)); //Durations.milliseconds(10)); é bem rápido
		
		//Add 21/08/18
		//Disable INFO messages-> https://stackoverflow.com/questions/48607642/disable-info-messages-in-spark-for-an-specific-application
		Logger.getRootLogger().setLevel(Level.ERROR);
		
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
		
		//comentado para testar o processamento mappreduce me tempo de execução
		lines.foreachRDD(rdd -> {

			System.out.println("Dados:Do RDDe" + rdd.count());

			rdd.foreachPartition(partitionOfRecords -> {

				String value;

				//ConnectioHbase lo_connection = new ConnectioHbase();

				// Get Gson object
				Gson gson = new GsonBuilder().setPrettyPrinting().create();

				// parse json string to object
				Cl_BroLog lo_log;
				
				
				
				while (partitionOfRecords.hasNext()) {

					value = partitionOfRecords.next();

					lo_log = gson.fromJson(value, Cl_BroLog.class);
					
					//System.out.println("VALUE:"+value);
					
					JSONParser pars = new JSONParser();
					
					JSONObject lv_obj = (JSONObject) pars.parse(value);
				
										
					Iterator keys =  lv_obj.keySet().iterator();
					
					
					while(keys.hasNext()) {
						
						String n = (String) keys.next().toString();
						
						System.out.println("\n Fieldname:"+ n + "\t \t Value:" + lv_obj.get(n));
						
					}
					
					//lo_connection.M_LogPutTable(lo_log);

				}

				//lo_connection.Close();

			});

		});
		
		
		//Teste tratar as linhas para fazer o map VERIFICAR NO SPARK SE ESSE REALMENTE SERIA O JEITO CORRETO
		// Break every message into words and return list of words
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String line) throws Exception {
				
				// Get Gson object
				//Gson gson = new GsonBuilder().setPrettyPrinting().create();

				// parse json string to object
				//Cl_BroLog lo_log;	
				
				//lo_log = gson.fromJson(line, Cl_BroLog.class);							
				
//				String lv_key = "UID: "+lo_log.getUid() +";IP_Orig: "+ lo_log.getOrig_h() +";IP_Resp: "+ lo_log.getResp_h();
				//String lv_key = "TIPO: "+lo_log.getLog()+";IP_Orig: "+ lo_log.getOrig_h() +";IP_Resp: "+ lo_log.getResp_h();
				
				//String lv_key = lo_log.getDns();
				
				//System.out.println("\n Line WORDS - " + lv_key);
								
				return Arrays.asList(line.split(" ")).iterator();
				//return Arrays.asList(lv_key.split(";")).iterator();
			}
		});
		
		// Take every word and return Tuple with (word,1)
		JavaPairDStream<String, Integer> wordMap = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String word) throws Exception {
				// go_kafka.ReadTable(word);
				//go_kafka.PutTable(word);
				//System.out.println("Linha Kafka:" + word);
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// Count occurance of each word
		JavaPairDStream<String, Integer> wordCount = wordMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer first, Integer second) throws Exception {
				//System.out.println("COUNT:" + first +" Second:" +second);
				return first + second;
			}
		});


		// Print the word count
		wordCount.print(100);// sem informar o número so os 10 primeiros são exibidos
		
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
