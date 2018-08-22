package main;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class Cl_BroLog {
	
	private String log;
	
	private String ts;
	
    private String uid;
    
    @SerializedName("id.orig_h")
    private String orig_h;
    
    @SerializedName("id.orig_p")
    private String orig_p;
    
    @SerializedName("id.resp_h")
    private String resp_h;
    
    @SerializedName("id.resp_p")
    private String resp_p;
    
    //DNS     
    private String query;	 //DNS    
    private String proto;	 //DNS,CONN					    
    private String trans_id; //DNS
	
    //CONN      
    private String service;    
    private String duration;          
    private String orig_bytes;        
    private String resp_bytes;        
    private String conn_state;        
    private String orig_pkts;         
    private String orig_ip_bytes;     
    private String resp_pkts;         
    private String resp_ip_bytes; 
    
    //HTTP
    private String request_body_len;  
    private String response_body_len; 
    private String status_code;       
    private String status_msg;        
    private List<String> resp_fuids;    
    

//Nome das Colunas no Hbase    
    public String[]  gs_key = {
    		"ts"			     ,
    		"uid"		         ,
    		"idOrigH"            ,
    		"idOrigP"            ,
    		"idRespH"            ,
    		"idRespP"            ,
    	};
    
    public String[]  gs_dns = {
    		"query"		         , 
    		"proto"              , 
    		"trans_id"	         ,     		
    };
    
    public String[]  gs_conn = {   
    		"proto"				 ,
    		"service"            ,
    		"duration"           ,
    		"orig_bytes"         ,
    		"resp_bytes"         ,
    		"conn_state"         ,
    		"orig_pkts"          ,
    		"orig_ip_bytes"      ,
    		"resp_pkts"          ,
    		"resp_ip_bytes"      ,    		
    };
    
    public String[]  gs_http = {
    		"request_body_len"   ,
    		"response_body_len"  ,
    		"status_code"        ,
    		"status_msg"         ,
    		"resp_fuids"         ,
    };
    
   
    public String getTuple() {	
    	
    	String lv_tuple = "  Tipo	  : " +this.log			+ 
    					  "; Orig_IP  : " +this.orig_h		+ 
    					  "; Orig_PRT : " +this.orig_p		+
    					  "; Resp_IP  : " +this.resp_h		+ 
    					  "; Resp_PRT : " +this.resp_p;
    	
    	return lv_tuple;
    	
    }
    
    public String getConn() {
    	
    	String lv_conn = "";        
    	
    	if(this.log.equals("CONN")) {

    		lv_conn = getTuple() +
    				  "; service      : " +this.service      + 
    				  "; duration     : " +this.duration     + 
    				  "; orig_bytes   : " +this.orig_bytes   + 
    				  "; resp_bytes   : " +this.resp_bytes   + 
    				  "; conn_state   : " +this.conn_state   + 
    				  "; orig_pkts    : " +this.orig_pkts    + 
    				  "; orig_ip_bytes: " +this.orig_ip_bytes+ 
    				  "; resp_pkts    : " +this.resp_pkts    + 
    				  "; resp_ip_bytes: " +this.resp_ip_bytes;     	
    	}         
    	
    	return lv_conn;
    }
    
    public String getDns() {
    	
    	String lv_dns = "";
    	
    	//System.out.println("TIPO:"+ log + "TAM:" + log.length());
    	
    	if(this.log.equals("DNS")) {
    		
    		lv_dns = //getTuple() +
    				"; query	: " +query;	  //+
    				//"; proto    : " +proto    +
    				//"; trans_id : " +trans_id;    		
    	}
    	    	
    	return lv_dns;
    }
    
    public String getTs() {
		return ts;
	}

	public void setTs(String ts) {
		this.ts = ts;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getOrig_h() {
		return orig_h;
	}

	public void setOrig_h(String orig_h) {
		this.orig_h = orig_h;
	}

	public String getOrig_p() {
		return orig_p;
	}

	public void setOrig_p(String orig_p) {
		this.orig_p = orig_p;
	}

	public String getResp_h() {
		return resp_h;
	}

	public void setResp_h(String resp_h) {
		this.resp_h = resp_h;
	}

	public String getResp_p() {
		return resp_p;
	}

	public void setResp_p(String resp_p) {
		this.resp_p = resp_p;
	}

	public String getQuery() {

		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getProto() {
		return proto;
	}

	public void setProto(String proto) {
		this.proto = proto;
	}

	public String getTrans_id() {
		return trans_id;
	}

	public void setTrans_id(String trans_id) {
		this.trans_id = trans_id;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public String getOrig_bytes() {
		return orig_bytes;
	}

	public void setOrig_bytes(String orig_bytes) {
		this.orig_bytes = orig_bytes;
	}

	public String getResp_bytes() {
		return resp_bytes;
	}

	public void setResp_bytes(String resp_bytes) {
		this.resp_bytes = resp_bytes;
	}

	public String getConn_state() {
		return conn_state;
	}

	public void setConn_state(String conn_state) {
		this.conn_state = conn_state;
	}

	public String getOrig_pkts() {
		return orig_pkts;
	}

	public void setOrig_pkts(String orig_pkts) {
		this.orig_pkts = orig_pkts;
	}

	public String getOrig_ip_bytes() {
		return orig_ip_bytes;
	}

	public void setOrig_ip_bytes(String orig_ip_bytes) {
		this.orig_ip_bytes = orig_ip_bytes;
	}

	public String getResp_pkts() {
		return resp_pkts;
	}

	public void setResp_pkts(String resp_pkts) {
		this.resp_pkts = resp_pkts;
	}

	public String getResp_ip_bytes() {
		return resp_ip_bytes;
	}

	public void setResp_ip_bytes(String resp_ip_bytes) {
		this.resp_ip_bytes = resp_ip_bytes;
	}

	public String getRequest_body_len() {
		return request_body_len;
	}

	public void setRequest_body_len(String request_body_len) {
		this.request_body_len = request_body_len;
	}

	public String getResponse_body_len() {
		return response_body_len;
	}

	public void setResponse_body_len(String response_body_len) {
		this.response_body_len = response_body_len;
	}

	public String getStatus_code() {
		return status_code;
	}

	public void setStatus_code(String status_code) {
		this.status_code = status_code;
	}

	public String getStatus_msg() {
		return status_msg;
	}

	public void setStatus_msg(String status_msg) {
		this.status_msg = status_msg;
	}

	public List<String> getResp_fuids() {
		return resp_fuids;
	}

	public void setResp_fuids(List<String> resp_fuids) {
		resp_fuids.addAll(this.resp_fuids);		
	}

	public String getLog() {
		return log;
	}

	public void setLog(String log) {
		this.log = log;
	}
    
	
}
