package cn.edu.xmu.software.hadoopIPC;

enum Status {
	SUCCESS (0),
	ERROR (1),
	FATAL (-1);
	
	int state;
	
	private Status(int state){
		this.state = state;
	}
}
