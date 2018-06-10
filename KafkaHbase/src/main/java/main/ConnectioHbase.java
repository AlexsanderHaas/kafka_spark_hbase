package main;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

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

import com.google.protobuf.ServiceException;

public class ConnectioHbase {

	TableName gv_brolog = TableName.valueOf("BroLog");
	
	String gv_famy_key = "key";
	
	String gv_fam_conn = "conn";
	
	String gv_fam_dns  = "dns";
	
	String gv_fam_http = "http";

	Configuration gv_config = HBaseConfiguration.create();

	String gv_path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();

	Connection gv_connection;

	Admin gv_admin;

	Table gv_table;

	public ConnectioHbase() throws IOException, ServiceException {

		Conecta();

	}

	public void Conecta() throws IOException, ServiceException {

		gv_config.addResource(new Path(gv_path));

		gv_connection = ConnectionFactory.createConnection(gv_config);

		HBaseAdmin.checkHBaseAvailable(gv_config);

		gv_admin = gv_connection.getAdmin();

		gv_table = gv_connection.getTable(gv_brolog);

	}

	public void Close() throws IOException {
		gv_connection.close();
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

		try {
			HColumnDescriptor desc1 = new HColumnDescriptor(lv_row);
			gv_admin.addColumn(gv_brolog, desc1);
			//System.out.println("Success.");
		} catch (Exception e) {
			System.out.println("Failed.");
			System.out.println(e.getMessage());
		} finally {
			// admin.enableTable(test);
		}

	}
	
private Put M_PutColumns(Cl_BroLog lo_log, Put ls_table) {
	
		//***********************************
		//KEY=====>>>
		//***********************************
	
		// ----------- Ts
		M_PutLs(gv_famy_key, lo_log.gs_key[0], lo_log.getTs(), ls_table);

		// ----------- Uid
		M_PutLs(gv_famy_key, lo_log.gs_key[1], lo_log.getUid(), ls_table);

		// ----------- Id.Orig_H
		M_PutLs(gv_famy_key, lo_log.gs_key[2], lo_log.getOrig_h(), ls_table);

		// ----------- Id.Orig_P
		M_PutLs(gv_famy_key, lo_log.gs_key[3], lo_log.getOrig_p(), ls_table);

		// ----------- Id.Resp_H
		M_PutLs(gv_famy_key, lo_log.gs_key[4], lo_log.getResp_h(), ls_table);

		// ----------- Id.Orig_P
		M_PutLs(gv_famy_key, lo_log.gs_key[5], lo_log.getResp_p(), ls_table);
		
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

}
