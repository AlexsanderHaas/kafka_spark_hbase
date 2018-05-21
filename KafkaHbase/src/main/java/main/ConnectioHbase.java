package main;

import java.io.IOException;

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

	String gv_family_dns = "dns";

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

	public void M_DnsPutTable(Cl_BroDns lo_dns) throws IOException, ServiceException {

		byte[] lv_row;		

		lv_row = Bytes.toBytes(lo_dns.getTs());

		Put ls_table = new Put(lv_row);

		// ----------- Uid
		M_PutLs(lo_dns.gs_colunas[1], lo_dns.getUid(), ls_table);

		// ----------- Id.Orig_H
		M_PutLs(lo_dns.gs_colunas[2], lo_dns.getOrig_h(), ls_table);


		// ----------- Id.Orig_P
		M_PutLs(lo_dns.gs_colunas[3], lo_dns.getOrig_p(), ls_table);


		// ----------- Id.Resp_H
		M_PutLs(lo_dns.gs_colunas[4], lo_dns.getResp_h(), ls_table);
	

		// ----------- Id.Orig_P
		M_PutLs(lo_dns.gs_colunas[5], lo_dns.getResp_p(), ls_table);
		

		// ----------- Proto
		M_PutLs(lo_dns.gs_colunas[6], lo_dns.getProto(), ls_table);
		

		// ----------- Query
		M_PutLs(lo_dns.gs_colunas[7], lo_dns.getQuery(), ls_table);

		// ----------- PutTable

		System.out.println("LS_TABLE: " + ls_table);

		gv_table.put(ls_table);

		try {
			HColumnDescriptor desc1 = new HColumnDescriptor(lv_row);
			gv_admin.addColumn(gv_brolog, desc1);
			System.out.println("Success.");
		} catch (Exception e) {
			System.out.println("Failed.");
			System.out.println(e.getMessage());
		} finally {
			// admin.enableTable(test);
		}

	}

	private Put M_PutLs(String col, String value, Put ls_table) {

		byte[] lv_coluna;
		byte[] lv_value;

		if (value != null) {

			lv_coluna = Bytes.toBytes(col);

			lv_value = Bytes.toBytes(value);

			ls_table.addImmutable(gv_family_dns.getBytes(), lv_coluna, lv_value);
		}

		return ls_table;

	}

}
