package cn.edu.xmu.software.hadoopIPC;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Hashtable;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Client {

	private int counter;
	private AtomicBoolean running = new AtomicBoolean(true); // if client runs
	private Class<? extends Serializable> valueClass;
	private Properties conf;
	private Hashtable<ConnectionId, Connection> connections =
			    new Hashtable<ConnectionId, Connection>();
	
	public Client(Class<? extends Serializable> valueClass, Properties conf) throws IOException{
		this.valueClass = valueClass;
		this.conf = conf;
	}
	
	public Connection getConnection(ConnectionId remoteId, Call call) throws IOException {
		if(!running.get())
			throw new IOException("The client is stoped.");
		Connection connection = null;
		do{
			synchronized (connections) {
				connection = connections.get(remoteId);
				if(connection == null) {
					connection = new Connection(remoteId);
					connections.put(remoteId, connection);
				}
			}
		}while(!connection.addCall(call));
		
		return connection;
	}
	
	public void stop() {
		if(!running.compareAndSet(true, false)) {
			return;
		}
		synchronized (connections) {
			for(Connection conn : connections.values()) {
				conn.interrupt();
			}
		}
		
		while(!connections.isEmpty()) {
			try{
				Thread.sleep(100);
			}catch(InterruptedException e) {
				
			}
		}
	}
	
	public Serializable call(Serializable param, InetSocketAddress addr, 
			Class<?> protocol, Properties conf) throws IOException {
		Call call = new Call(param);
		ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol, conf);
		Connection connection = getConnection(remoteId, call);
		connection.sendParam(call);
		boolean interrupted = false;
		synchronized (call) {
			while(!call.done) {
				try {
					call.wait();
				} catch (InterruptedException e) {
					interrupted = true;
				}
			}
			if(interrupted) {
				Thread.currentThread().interrupt();
			}
			if(call.error != null) {
				throw call.error;
			}
			else {
				return call.value;
			}
		}
	}
	
	static class ConnectionId{
		InetSocketAddress address;
		Class<?> protocol;
	   private static final int PRIME = 16777619;
	   private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
	   
	   public InetSocketAddress getAddress() {
		   return address;
	   }
	   
	   public Class<?> getProtocol() {
		   return protocol;
	   }
	   
	   public boolean getTcpNoDelay() {
		   return tcpNoDelay;
	   }
	   
	   ConnectionId(InetSocketAddress address, Class<?> protocol,boolean tcpNoDelay) {
		   this.address = address;
		   this.protocol = protocol;
		   this.tcpNoDelay = tcpNoDelay;
	   }
	   
	   static ConnectionId getConnectionId(InetSocketAddress address, Class<?> protocol, Properties conf) {
		   String tcpNoDelayStr = conf.getProperty("client.connection.tcpnodelay", "false");
		   return new ConnectionId(address, protocol, tcpNoDelayStr.equals("true") ? true:false);
	   }
	   
     static boolean isEqual(Object a, Object b) {
         return a == null ? b == null : a.equals(b);
       }

       @Override
       public boolean equals(Object obj) {
         if (obj == this) {
           return true;
         }
         if (obj instanceof ConnectionId) {
           ConnectionId that = (ConnectionId) obj;
           return isEqual(this.address, that.address)
               && isEqual(this.protocol, that.protocol)
               && this.tcpNoDelay == that.tcpNoDelay;
         }
         return false;
       }
       
       @Override
       public int hashCode() {
         int result = 1;
         result = PRIME * result + ((address == null) ? 0 : address.hashCode());
         result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
         result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
         return result;
       }
	}
	
	private class Call{
		int id;
		Serializable param;
		Serializable value;
		IOException error;
		boolean done;
		
		protected Call(Serializable param) {
			this.param = param;
			synchronized (Client.this) {
				this.id = counter++;
			}
			this.done = false;
		}
		
		protected synchronized void callComplete(){
			this.done = true;
			notify();
		}
		
		protected synchronized void setValue(Serializable value) {
			this.value = value;
			callComplete();
		}
		
		protected synchronized void setException(IOException ioe) {
			this.error = ioe;
			callComplete();
		}
	}
	
	private class Connection extends Thread{
		private InetSocketAddress server;
		private ConnectionHeader header;
		private final ConnectionId remoteId;
		ByteBuffer byteBuffer;
		private Socket socket;
		private SocketChannel channel;
		private boolean tcpNoDelay;
		
		private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
	   private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
	   private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
	   private IOException closeException; // close reason
	   private final static short maxRetries = 5;  // 5 times to retry connection
	   private final static short timeOutRetries = 45; // 20s*45 = 15min , max retry timeout is 15min.
	   private final static int maxIdleTime = 5*60*1000; // 5min idle time

	   
		public Connection(ConnectionId remoteId) throws UnknownHostException {
			this.remoteId = remoteId;
			this.server = remoteId.address;
			if(this.server.isUnresolved()) {
				throw new UnknownHostException("unknown host: " + 
                        remoteId.getAddress().getHostName());
			}
			this.tcpNoDelay = remoteId.getTcpNoDelay();
			this.header = new ConnectionHeader(remoteId.getProtocol().getName());
			this.setName("IPC Client connection to " + remoteId.getAddress().toString());
			this.setDaemon(true);
		}
		
		@Override
		public void run() {
			while(waitForWork()) {
				try{
					receiveResponse();
				}catch(IOException ioe) {
					markClosed(ioe);
				}
			}
			close();
		}

		private synchronized void close(){
			if(shouldCloseConnection.get()) {
				return;
			}
			
			synchronized (connections) {
				if(connections.get(remoteId) == this){
					connections.remove(remoteId);
				}
			}
		}
		
		private synchronized boolean addCall(Call call) {
			if(shouldCloseConnection.get()) {
				return false;
			}
			calls.put(call.id, call);
			notify();
			return true;
		}
		
		private void receiveResponse() throws IOException{
			 channel.read(byteBuffer);
			 if(byteBuffer.remaining() > 0) {
				 return;
			 }
			 int id = byteBuffer.getInt();
			 Call call = calls.get(id);
			 if(call == null)
				 return;
			 byteBuffer.clear();
			 channel.read(byteBuffer);
			 if(byteBuffer.remaining() > 0) 
				 return;
			 int state = byteBuffer.getInt();
			 byteBuffer.clear();

			 if(state == Status.SUCCESS.state) {
				  channel.read(byteBuffer);
				  byte[] buff = byteBuffer.array();
				  if(buff.length > 0){
					  ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buff));
					  Serializable value = null;
					try {
						value = valueClass.cast(ois.readObject());
					} catch (ClassNotFoundException e) {
						markClosed(new IOException(e.getCause()));
					}
					  call.setValue(value);
				  }
				  byteBuffer.clear();
				  calls.remove(id);
			 }else if(state == Status.ERROR.state) {
				 call.setException(new IOException("error"));
				 calls.remove(id);
			 }else if(state == Status.FATAL.state) {
				 markClosed(new IOException("fatal"));
			 }
		}
		
		private synchronized void markClosed(IOException ioe) {
			if(shouldCloseConnection.compareAndSet(false, true)) {
				closeException = ioe;
				notifyAll();
			}
		}
		
		private boolean waitForWork(){
			if(calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
				long timeout = maxIdleTime - (System.currentTimeMillis() - lastActivity.get());
				try {
					wait(timeout);
				} catch (InterruptedException e) {
				}
			}
			if(!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
				return true;
			}else if(shouldCloseConnection.get()) {
				return false;
			} else if(calls.isEmpty()) {
				markClosed(null);
				return false;
			} else {
		      markClosed((IOException)new IOException().initCause(
		                new InterruptedException()));
		      return false;
			}
		}
		
		private void writeHeader() throws IOException {
			ByteArrayOutputStream headerBuff = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(headerBuff);
			oos.writeObject(header);
			ByteArrayOutputStream buff = new ByteArrayOutputStream();
			int headerLen = headerBuff.size();
			DataOutputStream  dos = new DataOutputStream(buff);
			dos.writeInt(headerLen);
			oos.close();
			oos = null;
			oos = new ObjectOutputStream(buff);
			oos.writeObject(header);
			ByteBuffer buffer = ByteBuffer.allocate(buff.size());
			buffer.wrap(buff.toByteArray());
			channel.write(buffer);
		}
		
		private void sendParam(Call call) throws IOException {
	      if (shouldCloseConnection.get()) {
	          return;
	        }
	      byteBuffer.putInt(call.id);
	      ByteArrayOutputStream buffOut = new ByteArrayOutputStream();
	      ObjectOutputStream oos = new ObjectOutputStream(buffOut);
	      oos.writeObject(call.param);
	      byteBuffer.put(buffOut.toByteArray());
	      synchronized (channel) {
	    	    channel.write(byteBuffer);
		    }
		}
		
		private synchronized void setupConnection() throws IOException {
			short ioFailures = 0;
			short timeOutFailures = 0;
			while(true) {
				try{
					channel.configureBlocking(false);
					this.socket = this.channel.socket();
					this.socket.setTcpNoDelay(tcpNoDelay);
					this.socket.connect(server, 20000);
					writeHeader();
					byteBuffer = ByteBuffer.allocate(10*1024);
					return;
				}catch(SocketTimeoutException toe) {
					closeConnection();
					timeOutFailures++;
					if(timeOutFailures > timeOutRetries) {
						throw toe;
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				}catch(IOException ioe) {
					closeConnection();
					ioFailures++;
					if(ioFailures > maxRetries) {
						throw ioe;
					}
					try{
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				}
			}
		}
		
		private void closeConnection(){
			try {
				socket.close();
			} catch (IOException e) {
			}
			socket = null;
		}
	}
}
