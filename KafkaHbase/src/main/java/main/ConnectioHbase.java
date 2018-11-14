package main;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


//import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
//import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
//import org.apache.hadoop.hbase.filter.ByteArrayComparable;
//import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ServiceException;


//Add 16/09
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
import java.util.Date;


public class ConnectioHbase {

	//Nome das Colunas no Hbase    
    public String[]  gs_key = {
	    "ts"		,
	    "uid"		,
	    "id.orig_h" ,
	    "id.orig_p" ,
	    "id.resp_h" ,
	    "id.resp_p" ,
	    "proto"		,
    };
	
	TableName gv_brolog = TableName.valueOf("BRO_LOG");

	String gv_fam_key = "key";

	String gv_fam_conn = "conn";

	String gv_fam_dns  = "dns";

	String gv_fam_http = "http";

	Configuration gv_config = HBaseConfiguration.create();

	//String gv_path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
	//String gv_path = this.getClass().getClassLoader().getResource("/home/user/Documentos/spark_bro/hbase-site.xml").getPath();
	//String gv_path = this.getClass().getClassLoader().getResource("/usr/hdp/current/hbase-client/conf/hbase-site.xml").getPath();
	
	File lv_f = new File("/usr/hdp/current/hbase-client/conf/hbase-site.xml");
	String gv_path = lv_f.getPath();
	
	Connection gv_connection;

	Admin gv_admin;

	Table gv_table;

	public ConnectioHbase() throws IOException, ServiceException {

		Conecta();

	}

	public void Conecta() throws IOException, ServiceException {

		gv_config.addResource(new Path(gv_path));

		gv_connection = ConnectionFactory.createConnection(gv_config);

		//HBaseAdmin.checkHBaseAvailable(gv_config);

		gv_admin = gv_connection.getAdmin();

		gv_table = gv_connection.getTable(gv_brolog);

	}

	public void Close() throws IOException {
		gv_connection.close();
	}
	
	public void M_PutConn(String im_json) throws ParseException, IOException{							
		
		JSONParser lv_parser = new JSONParser();
		
		JSONObject lv_obj_log = (JSONObject) lv_parser.parse(im_json);			//ao converter o TS esta ficando ferrado									
		
		//Trata para pegar o JSON do tipo de LOG
		String lv_log_key = lv_obj_log.keySet().iterator().next().toString(); //Camptura o identificador do tipo do LOG
		
		//String lv_log_value = lv_obj_log.get(lv_log_key).toString();
		Object lv_log_value = lv_obj_log.get(lv_log_key);
					
		
		//JSONObject lv_obj_val = (JSONObject) lv_parser.parse(lv_log_value);
		JSONObject lv_obj_val = (JSONObject) lv_log_value;
		
		//System.out.println("\nJSON-Log:"+lv_log_key + "\t VALOR:"+lv_log_value);
		
		//Trata os valores do JSON para salvar no banco
		
		String lv_field,
			   lv_value,
		       lv_replace;
		
		String lv_famyli = "";			

		byte[] lv_row;

		String lv_id = null;
		
		switch (lv_log_key) {
		
		case "conn":
			
			lv_famyli = gv_fam_conn;
			break;
			
		case "dns":

			lv_famyli = gv_fam_dns;
			break;
		
		case "http":

			lv_famyli = gv_fam_dns;
			break;
			
		default:
			break;
		}
		
		if(lv_famyli.equals("")) {
			return;
		}		
		
		//System.out.println("\nFAM-Log:"+lv_famyli);
		
		BigDecimal number = new BigDecimal(lv_obj_val.get("ts").toString()); //Precisa pois no LOG do BRO as chaves são o UID e TS pois no DNS os UID se repetem
																			 //Fica dando erro que linha já existe se não inserir
		Date lv_time = new Date();
		
		long lv_stamp = lv_time.getTime();
				
		lv_id = (String)lv_obj_val.get("id.orig_h") + 
				'-' +
				(String)lv_obj_val.get("uid");
		
		if(lv_famyli.equals(gv_fam_dns)) {
			//System.out.println("\n TIMESTAMP:"+lv_stamp);
			lv_id = lv_id + "-" + lv_stamp; //Pegar STAMP do programa e não do BRO
		}

		lv_id = lv_id.replace("::", "");//Se o nome da coluna fe80::e5cd:4a03:57de:e00a
		lv_id = lv_id.replace(":", "");//Se o nome da coluna tiver fe80::e5cd:4a03:57de:e00a
		
		//System.out.println("\n Row:"+lv_id);

		lv_row = Bytes.toBytes(lv_id);

		Put ls_table = new Put(lv_row);

		M_PutLs(gv_fam_key, "log", lv_famyli, ls_table); //Para se ter um identificador de que log é essa informação
		
		Iterator<?> lv_keys = lv_obj_val.keySet().iterator();
		
		while(lv_keys.hasNext()) {
			
			lv_field = lv_keys.next().toString();															
			
			lv_value = lv_obj_val.get(lv_field).toString();	
			
			if(lv_field.equals(gs_key[0])) {				
				
				number = new BigDecimal(lv_obj_val.get(lv_field).toString());

				//System.out.println("\nID: " +lv_id +  "\tBIG->TIMSTT: "+ number);
				
				lv_value = number.toString();
				
			}
			

			//System.out.println("Fieldname:"+ lv_field + "\t \t Value:" + lv_value);
			
			if(lv_field.contains("::")) {
				System.out.println("\nLinha com :: _>"+lv_id);
			}
			
			lv_replace = lv_field.replace(".", "_");//Se o nome da coluna tiver ponto remove pro "UNDERLINE"
			
			if(lv_field.equals(gs_key[0]) ||
		       lv_field.equals(gs_key[1]) ||
		       lv_field.equals(gs_key[2]) ||
		       lv_field.equals(gs_key[3]) ||
		       lv_field.equals(gs_key[4]) ||
		       lv_field.equals(gs_key[5]) ||
		       lv_field.equals(gs_key[6])
			   ){																	
				
				M_PutLs(gv_fam_key, lv_replace, lv_value, ls_table);
				//System.out.println("\n Fieldname:"+ lv_field + "\t \t Value:" + lv_value);
			}else{
				//System.out.println("\n FAM:"+lv_famyli+"\tFieldname:"+ lv_field + "\t \t Value:" + lv_value);
				M_PutLs(lv_famyli, lv_replace, lv_value, ls_table);			
			}
															
		}
		
		gv_table.put(ls_table);

		try {
			//HColumnDescriptor desc1 = new HColumnDescriptor(lv_row);
			ColumnFamilyDescriptor lv_desc = ColumnFamilyDescriptorBuilder.newBuilder(lv_row).build();

			//gv_admin.addColumn(gv_brolog, desc1);
			gv_admin.addColumnFamily(gv_brolog, lv_desc);
			//System.out.println("Success.");
		} catch (Exception e) {
			
			System.out.println("Failed.");
			System.out.println("\n TIPO:" +lv_log_key+ "\tRow:" +lv_id+ "\tLOG-JSON:" + lv_log_value);
			System.out.println(e.getMessage());
		} finally {
			// admin.enableTable(test);
		}
		
	}
	
	public void M_LogPutTable(Cl_BroLog lo_log) throws IOException, ServiceException {

		byte[] lv_row;

		Timestamp lv_timestamp = new Timestamp(System.currentTimeMillis());

		String lv_stamp;

		lv_stamp = Long.toString(lv_timestamp.getTime());

		// System.out.println("TimeHUmm: " + lv_stamp);

		lv_row = Bytes.toBytes(lv_stamp);

		Put ls_table = new Put(lv_row);

		M_PutColumns(lo_log, ls_table);			

		// ----------- PutTable
		/*if(lo_log.getLog().equals("DNS")){
			System.out.println("LS_TABLE: " + ls_table);
		}*/


		gv_table.put(ls_table);

		/*try {
			HColumnDescriptor desc1 = new HColumnDescriptor(lv_row);
			gv_admin.addColumn(gv_brolog, desc1);
			//System.out.println("Success.");
		} catch (Exception e) {
			System.out.println("Failed.");
			System.out.println(e.getMessage());
		} finally {
			// admin.enableTable(test);
		}*/

	}

	private Put M_PutColumns(Cl_BroLog lo_log, Put ls_table) {

		//***********************************
		//KEY=====>>>
		//***********************************

		// ----------- Ts
		M_PutLs(gv_fam_key, lo_log.gs_key[0], lo_log.getTs(), ls_table);

		// ----------- Uid
		M_PutLs(gv_fam_key, lo_log.gs_key[1], lo_log.getUid(), ls_table);

		// ----------- Id.Orig_H
		M_PutLs(gv_fam_key, lo_log.gs_key[2], lo_log.getOrig_h(), ls_table);

		// ----------- Id.Orig_P
		M_PutLs(gv_fam_key, lo_log.gs_key[3], lo_log.getOrig_p(), ls_table);

		// ----------- Id.Resp_H
		M_PutLs(gv_fam_key, lo_log.gs_key[4], lo_log.getResp_h(), ls_table);

		// ----------- Id.Orig_P
		M_PutLs(gv_fam_key, lo_log.gs_key[5], lo_log.getResp_p(), ls_table);

		//***********************************
		//DNS=====>>>
		//***********************************

		// ----------- Query
		M_PutLs(gv_fam_dns, lo_log.gs_dns[0], lo_log.getQuery(), ls_table);

		if(lo_log.getLog().equals("DNS")) {

			// ----------- Proto
			M_PutLs(gv_fam_dns, lo_log.gs_dns[1], lo_log.getProto(), ls_table);

		}		

		// ----------- Trans_Id
		M_PutLs(gv_fam_dns, lo_log.gs_dns[2], lo_log.getTrans_id(), ls_table);

		//***********************************
		//CONN=====>>>
		//***********************************

		if(lo_log.getLog().equals("CONN")) {

			// ----------- Proto
			M_PutLs(gv_fam_conn, lo_log.gs_conn[0], lo_log.getProto(), ls_table);

		}

		// ----------- Service
		M_PutLs(gv_fam_conn, lo_log.gs_conn[1],  lo_log.getService(), ls_table);

		// ----------- Duration
		M_PutLs(gv_fam_conn, lo_log.gs_conn[2], lo_log.getDuration(), ls_table);

		// ----------- Orig_Bytes
		M_PutLs(gv_fam_conn, lo_log.gs_conn[3], lo_log.getOrig_bytes(), ls_table);

		// ----------- Resp_Bytes
		M_PutLs(gv_fam_conn, lo_log.gs_conn[4], lo_log.getResp_bytes(), ls_table);

		// ----------- Conn_State
		M_PutLs(gv_fam_conn, lo_log.gs_conn[5], lo_log.getConn_state(), ls_table);

		// ----------- Orig_Pkts
		M_PutLs(gv_fam_conn, lo_log.gs_conn[6], lo_log.getOrig_pkts(), ls_table);

		// ----------- Orig_Ip_Bytes
		M_PutLs(gv_fam_conn, lo_log.gs_conn[7], lo_log.getOrig_ip_bytes(), ls_table);

		// ----------- Resp_Pkts
		M_PutLs(gv_fam_conn, lo_log.gs_conn[8], lo_log.getResp_pkts(), ls_table);

		// ----------- Resp_ip_bytes
		M_PutLs(gv_fam_conn, lo_log.gs_conn[9], lo_log.getResp_ip_bytes(), ls_table);

		//***********************************	
		//HTTP=====>>>
		//***********************************

		// ----------- Request_body_len
		M_PutLs(gv_fam_http, lo_log.gs_http[0], lo_log.getRequest_body_len(), ls_table);

		// ----------- Response_body_len
		M_PutLs(gv_fam_http, lo_log.gs_http[1], lo_log.getResponse_body_len(), ls_table);

		// ----------- Status_code
		M_PutLs(gv_fam_http, lo_log.gs_http[2], lo_log.getStatus_code(), ls_table);

		// ----------- Status_msg
		M_PutLs(gv_fam_http, lo_log.gs_http[3], lo_log.getStatus_msg(), ls_table);

		// ----------- Resp_Fuids
		List<String> lv_fuids = lo_log.getResp_fuids();

		if(lv_fuids != null) {

			for( String temp : lv_fuids){

				M_PutLs(gv_fam_http, lo_log.gs_http[4], temp, ls_table);							
			}
		}

		return ls_table;
	}

	private Put M_PutLs(String lv_fam, String col, String value, Put ls_table) {

		byte[] lv_coluna;
		byte[] lv_value;

		if (value != null) {

			//System.out.println("\n" + lv_fam + ":" + col + ":" + value);

			lv_coluna = Bytes.toBytes(col);

			lv_value = Bytes.toBytes(value);

			ls_table.addImmutable(lv_fam.getBytes(), lv_coluna, lv_value);
		}

		return ls_table;

	}
	
	public void M_LogGetTable() throws IOException {
		
		byte[] lv_row1 = Bytes.toBytes("1528667602825");
		
		List<Filter> lst_filters = new ArrayList<Filter>();			
		
		FilterList lv_list;
		
		//Filter lv_filter1 = new FamilyFilter(CompareOp.EQUAL,new BinaryComparator(gv_fam_key.getBytes()) );
		
		//Filter lv_filter2 = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(lv_row1));
		
		//lst_filters.add(lv_filter1);
		
		lv_list = new FilterList(Operator.MUST_PASS_ALL, lst_filters);
		
		Scan lv_scan = new Scan();
		
		lv_scan.setFilter(lv_list);	
				
		try{
			
			ResultScanner lv_scanner = gv_table.getScanner(lv_scan);
			
            int i = 0;
            
         // Get Gson object
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			
            Cl_BroLog lo_log;
                                    
            for (Result result : lv_scanner) {
            	
            	System.out.println("\n CellRow:" + Bytes.toString(result.getRow()) );
            	
            	for(Cell cell : result.listCells()) {
            		
            		System.out.println(i+"-- \n Familia: " + Bytes.toString(cell.getFamilyArray()) 
            				+ "\n Columns:" + Bytes.toString(cell.getQualifierArray())
            				+ "\n Value:" + Bytes.toString(cell.getValueArray()) );
            	}
            	
            	/*for(KeyValue key : result.list()){
            		
            		//System.out.println(i+"--Qualifier: " + key.getKeyString() + "Value:" + Bytes.toString(key.getValue()) );
            	}*/
                
                
                i++;
                
            }
           
            assert i == 2 : "This filtering sample should return 1 row but was " + i + ".";
            
        }catch (Exception e) {
			System.out.println("Erro"+ e);
		}
		
        System.out.println("Done. ");
			

	}
	
}








