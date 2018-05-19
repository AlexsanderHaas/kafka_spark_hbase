package main;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.protobuf.ServiceException;

import scala.Tuple2;

public class ConnectioHbase {

	// TableName test = TableName.valueOf("test");

	TableName test = TableName.valueOf("kafka");

	String fam = "cf";

	byte[] column1 = Bytes.toBytes("test1");

	Configuration config = HBaseConfiguration.create();

	String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();

	Connection connection;

	Admin admin;

	Table table;

	public ConnectioHbase() throws IOException, ServiceException {

		Conecta();

	}

	public void Conecta() throws IOException, ServiceException {

		config.addResource(new Path(path));

		connection = ConnectionFactory.createConnection(config);

		HBaseAdmin.checkHBaseAvailable(config);

		admin = connection.getAdmin();

		table = connection.getTable(test);

	}

	public void PutTable(String nome, int n_row) throws IOException, ServiceException {

		// Inserir linhas na tabela
		// Row1 => Family1:Qualifier1, Family1:Qualifier2

		String rw;

		rw = "row" + getDateTime() + "_" + n_row;

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

	public void PutTuple(Tuple2<String, Integer> nome, int n_row) throws IOException, ServiceException {

		// Inserir linhas na tabela
		// Row1 => Family1:Qualifier1, Family1:Qualifier2

		String rw;
		byte[] word = Bytes.toBytes("word");
		byte[] count = Bytes.toBytes("count");

		String val = Integer.toString(nome._2());

		byte[] value1 = Bytes.toBytes(val);

		rw = "row" + getDateTime() + "_" + n_row;

		System.out.println("Count PUT:" + nome._2());

		byte[] row_1 = Bytes.toBytes(rw);

		Put p = new Put(row_1);

		p.addImmutable(fam.getBytes(), word, Bytes.toBytes(nome._1));
		p.addImmutable(fam.getBytes(), count, value1);

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

	public void Close() throws IOException {
		connection.close();
	}

	// feito 11-05

	public void M_PutTable(String im_line) throws IOException, ServiceException {

		// Inserir linhas na tabela
		// Row1 => Family1:Qualifier1, Family1:Qualifier2

		String lv_val;
		String lv_key;

		byte[] lv_row;
		byte[] lv_coluna;
		byte[] lv_value;

		JSONObject lv_obj = new JSONObject(im_line);
		
		System.out.println("Line: " + im_line);
		System.out.println("Objeto: " + lv_obj);
		Iterator<?> keys = lv_obj.keys();

		Object lv_dados;
		
		Class cls;

		lv_key = "ts";

		//lv_val = (String) lv_obj.get(lv_key);// Da erro pois vem como booleano e não conveeerte pra string. O consumo
												// pego do meio de outrrrrrra
		
		//lv_row = Bytes.toBytes("23232.455"); inseriu com ponto
		//lv_row = Bytes.toBytes((Double) lv_obj.get(lv_key)); AJUSTAR PARA SALVAR A LINHA CORRETA
		
		lv_dados = lv_obj.get(lv_key);
		lv_row = lv_dados.toString().getBytes(); // salvou mas ver para salvar o mesmo valor que recebe pois esta indo com EA
		
		Put ls_table = new Put(lv_row);
		
		System.out.println("Row: " + lv_obj.get(lv_key) + "   byte" + lv_row);
		//while (keys.hasNext()) {

			//lv_key = (String) keys.next();
			
			System.out.println("Key: " + lv_key);
			System.out.println("Value: " + lv_obj.get(lv_key));
			
			
			
			lv_coluna = Bytes.toBytes(lv_key);

			lv_dados = lv_obj.get(lv_key);
			
			cls = lv_dados.getClass();
						
			System.out.println("col: " + lv_key + "byte" + lv_coluna);

			/*if (lv_dados instanceof Boolean) {

				lv_value = Bytes.toBytes((Boolean)lv_dados);

			}else if(lv_dados instanceof JSONArray)  {
				//lv_value = Bytes.toBytes((JSONArray) lv_dados);
			}*/

			//lv_value = Bytes.toBytes((String) lv_obj.get(lv_key)); // Da erro pois vem como booleano e não converte
																	// pra string
			//if (lv_dados instanceof String) {

				//lv_value = Bytes.toBytes("TESTERR");//((String)lv_dados);
				
				lv_value = lv_dados.toString().getBytes();
				
				System.out.println("value: " + lv_dados + "byte" + lv_value);
				
				ls_table.addImmutable(fam.getBytes(), lv_coluna, lv_value);
				System.out.println("LS_TABLE: " + ls_table);
				
			//}					

		//}

		table.put(ls_table);

		/*
		 * String rw; byte[] word = Bytes.toBytes("word"); byte[] count =
		 * Bytes.toBytes("count");
		 * 
		 * String val = Integer.toString(nome._2());
		 * 
		 * byte[] value1 = Bytes.toBytes(val);
		 * 
		 * rw = "row" + getDateTime() + "_" + n_row;
		 * 
		 * System.out.println("Count PUT:" + nome._2());
		 * 
		 * byte[] row_1 = Bytes.toBytes(rw);
		 * 
		 * Put p = new Put(row_1);
		 * 
		 * p.addImmutable(fam.getBytes(), word, Bytes.toBytes(nome._1));
		 * p.addImmutable(fam.getBytes(), count, value1);
		 * 
		 * 
		 * table.put(p);
		 */

		// admin.disableTable(test);
		try {
			HColumnDescriptor desc1 = new HColumnDescriptor(lv_row);
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

}

