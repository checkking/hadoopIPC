package cn.edu.xmu.software.hadoopIPC;

import java.io.Serializable;

public class ConnectionHeader implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String protocol;
	
	public ConnectionHeader(){}
	
	public ConnectionHeader(String protocol){
		this.setProtocol(protocol);
	}
	
	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
	
	public String toString(){
		return "ConnectionHeader:"+protocol;
	}

}
