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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.*;

import com.google.protobuf.ServiceException;
import com.sun.tools.internal.jxc.gen.config.Schema;

import java.io.IOException;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import main.ConnectioHbase;
import scala.Tuple2;
import scala.collection.immutable.Seq;
import scala.reflect.api.TypeTags.TypeTag;

//Add 21/08
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import main.Cl_JavaRecord;

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
		Collection<String> topics = Arrays.asList("BroLog");

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("BroLogConn");
		
		//SparkConf conf = new SparkConf().setAppName("BroLogConn");//se for executar no SUBMIT não precisa setar o master
		
		// Read messages in batch of 30 seconds
		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(3));//Durations.milliseconds(10)); é bem rápido
		
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
		
		//ORIGINAL 13/11/18
		//comentado para testar o processamento mappreduce me tempo de execução
		/*lines.foreachRDD(rdd -> {

			System.out.println("Dados:Do RDDe" + rdd.count());
			
			rdd.foreachPartition(partitionOfRecords -> {				
				
				String value;

				ConnectioHbase lo_connection = new ConnectioHbase();

				// Get Gson object
				Gson gson = new GsonBuilder().setPrettyPrinting().create();

				// parse json string to object
				Cl_BroLog lo_log;
				
				Dataset<Row> lv_st; 
				
				while (partitionOfRecords.hasNext()) {

					value = partitionOfRecords.next();
					
					lo_log = gson.fromJson(value, Cl_BroLog.class);
															
					lo_connection.M_PutConn(value);								
					
					//System.out.println("VALUE:"+value);									
					
					//lo_connection.M_LogPutTable(lo_log);

				}

				//lo_connection.Close();

			});

		});
		*/
		// comentado para testar o processamento mappreduce me tempo de execução
		lines.foreachRDD((rdd,time )-> {
			
			String lv_table = "BRO_LOG";
			String lv_zkurl = "localhost:2181";
			
			Map<String, String> map = new HashMap<String, String>();
			map.put("zkUrl", "localhost:2181");
			map.put("hbase.zookeeper.quorum", "master");
			map.put("table", lv_table);
			
			System.out.println("Dados:Do RDDe" + rdd.count());
			
			SparkSession lv_sess = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			
			//Dataset<Row> lv_json = lv_sess.read().json(); criar um Dataframe e fazer retornar um JSON, e tentar salva direto no phoenix para ver se é 
			//mais rapido descobrir como alterar o key do json
			
	
			// parse json string to object
			
			
		/*	// Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
		      JavaRDD<String> rowRDD = rdd.map(word -> {
		        Cl_JavaRecord record = new Cl_JavaRecord();		        
		       		        
		        record.setWord(word);
		        
		        return word.toString();
		        
		      });*/
			
		      //Dataset<Row> lv_st = lv_sess.createDataFrame(rdd, Cl_JavaRecord.class);
			  
		      //Dataset<Row> lv_js = lv_sess.read().json(lv_st.as(Encoders.STRING()).collectAsList());
		      
		      //lv_st.toJSON().printSchema();
		      
			  //JavaRDD<Row> rowRDD = lv_sess.read().json(rdd).toJavaRDD();
		      
		      //Dataset<Row> wordsDataFrame = lv_sess.createDataFrame(rowRDD, Cl_JavaRecord.class);
		      			
			  //JavaRDD<Cl_JavaRecord> lv_rec = rowRDD;
			
		      //Dataset<Row> lv_json = lv_sess.createDataFrame(rowRDD, Cl_JavaRecord.class);
		      //Dataset<Row> lv_json = rdd.to
		    
			//FUNCIONOU
			  /*Dataset<Row> lv_json = lv_sess.read().json(rdd);
			  
			  lv_json.show();
			  
			  lv_json.printSchema();
			  
			  lv_json.write()
				.format("org.apache.phoenix.spark")
				.mode("overwrite") 
				.option("table", "RESULT1") 
				.option("zkUrl", lv_zkurl) 
				.save();*/
			  
			  //Dataset<Row> lv_json = lv_sess.read().json(rowRDD);
		      
		     /* List<String> list = lv_json.as(Encoders.STRING()).collectAsList();
		      
		      System.out.println(list);
		      
		      Dataset<String> df1 = lv_sess.createDataset(list, Encoders.STRING()); 
		      
		      df1.show();
		      
		      Dataset<Row> lv_jn = lv_sess.read().json(df1);*/
		      
			  /*JavaRDD<Row> lrowRDD = rdd.map(new Function<String, Row>() {
                  @Override
                  public Row call(String msg) {
                    Row row = RowFactory.create(msg);
                    return row;
                  }
                });
			  
			//Create Schema       
		        StructType schema = DataTypes.createStructType(new StructField[] {DataTypes.createStructField("Message", DataTypes.StringType, true)});
		        //Get Spark 2.0 session       
		        
		        Dataset<Row> lv_l = lv_sess.createDataFrame(lrowRDD, schema);
			  
			  
		        lv_l.printSchema();
			      lv_l.show();*/
		        
		    /*  lv_json.printSchema();
		      lv_json.show();*/
		      
		      //SQLContext lv_sql = new SQLContext();
		      
		      //Dataset<Row> wordsDataFrame = lv_sql.createDataset

		     /* // Creates a temporary view using the DataFrame
		      wordsDataFrame.createOrReplaceTempView("words");

		      // Do word count on table using SQL and print it
		      Dataset<Row> wordCountsDataFrame =
		          lv_sess.sql("select word, count(*) as total from words group by word");
		      
		      System.out.println("========= " + time + "=========");
		      
		      wordCountsDataFrame.show();*/
			
			/*List<String> jsonData = Arrays.asList(
			        "{\"user_id\":1111,\"account_num\":12345}");
			Dataset<String> anotherPeopleDataset = spark.createDataset(jsonData, Encoders.STRING());
			Dataset<Row> anotherPeople = spark.read().json(anotherPeopleDataset);*/
			
			List<String> lv_rdd = rdd.collect();
			
			/*List<String> jsonData = Arrays.asList(
			        "{\"user_id\":1111,\"account_num\":12345}");*/
			
			//Dataset<String> anotherPeopleDataset = lv_sess.createDataset(jsonData, Encoders.STRING());
			Dataset<String> anotherPeopleDataset = lv_sess.createDataset(lv_rdd, Encoders.STRING());
			
			Dataset<Row> lv_data = lv_sess.read().json(anotherPeopleDataset);
			//lv_data.printSchema();
			//lv_data.show();
			
			try {			
			
			Dataset<Row> lv_conn = lv_data.select("conn.*")
										  .filter(col("conn.uid").isNotNull())
					                      .withColumnRenamed("id.orig_h", "id_orig_h") //id.orig_p id.resp_h id.resp_p
					                      .withColumnRenamed("id.orig_p", "id_orig_p")
					                      .withColumnRenamed("id.resp_h", "id_resp_h")
					                      .withColumnRenamed("id.resp_p", "id_resp_p");
			/*
			System.out.println("IFFF:"+lv_conn.head().isNullAt(1));
			
			if(lv_conn.head().isNullAt(1)) {*/
				
			
			//lv_conn.printSchema();
			long lv_num = lv_conn.count();
			System.out.println("LOG de CONN:"+lv_num);
			//lv_conn.show();
			
			lv_conn.write()
			       .format("org.apache.phoenix.spark")
			       .mode("overwrite") 
			       .option("table", "JSON1") 
			       .option("zkUrl", lv_zkurl) 
			       .save();
			//}
			} catch (Exception e) {
				System.out.println(e);
			}
			//MUDA O NOME DE COLUNA
			/*Dataset<Row> lv_aux = lv_data.withColumnRenamed("id.orig_h", "id_orig_h");
								         //.selectExpr("CAST(ts AS STRING)");//consigo renomear as colunas
			
			//lv_aux.selectExpr("CAST(ts AS STRING)").printSchema();;
			
			lv_aux.printSchema();
			lv_aux.show();*/
			
			//lv_aux.select(lv_aux.col("ts").cast("int")).printSchema();
			
		/*	 lv_data.write()
				.format("org.apache.phoenix.spark")
				.mode("overwrite") 
				.option("table", "RESULT2") 
				.option("zkUrl", lv_zkurl) 
				.save();*/
			
			  /*rdd.foreachPartition(partitionOfRecords -> {

				String value;
				
				

				//ConnectioHbase lo_connection = new ConnectioHbase();

				// Get Gson object
				Gson gson = new GsonBuilder().setPrettyPrinting().create();

				// parse json string to object
				Cl_BroLog lo_log;
				Dataset<Row> lv_r;
				while (partitionOfRecords.hasNext()) {

					value = partitionOfRecords.next();

					lv_r = lv_sess.read().json(value);
					
					lv_r.show();
					
					//lo_log = gson.fromJson(value, Cl_BroLog.class);

					//lo_connection.M_PutConn(value);

					// System.out.println("VALUE:"+value);

					// lo_connection.M_LogPutTable(lo_log);

				}

				// lo_connection.Close();

			});*/

		});
		
		/*//Teste tratar as linhas para fazer o map VERIFICAR NO SPARK SE ESSE REALMENTE SERIA O JEITO CORRETO
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
		wordCount.print();// sem informar o número so os 10 primeiros são exibidos
		*/
		//lines.print();
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
