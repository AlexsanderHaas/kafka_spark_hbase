package main;

import com.google.gson.annotations.SerializedName;

public class Cl_BroDns {
	
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
    
    private String proto;
    
    private String query;

//Nome das Colunas no Hbase    
    public String[]  gs_colunas = {
    		"ts",
    		"uid",
    		"idOrigH",
    		"idOrigP",
    		"idRespH",
    		"idRespP",
    		"proto",
    		"query",	
    	};

    
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
    
	
}
