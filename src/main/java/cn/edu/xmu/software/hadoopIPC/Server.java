package cn.edu.xmu.software.hadoopIPC;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;




public abstract class Server {

	private String addressString;
	private int port;
	private int readThreadCnt;
	private Properties conf;
	volatile private boolean running = true;
	private int backlog;
	private static ThreadLocal<Server> SERVER = new ThreadLocal<Server>();
	private static ThreadLocal<Call> curCall = new ThreadLocal<Call>();
	private Class<? extends Serializable> paramClass;
	private BlockingQueue<Call> callQueue; // queued calls
	
	private List<Connection> connectionList =
			Collections.synchronizedList(new LinkedList<Connection>());
	private int numConnections = 0;
	
	public Server(String addressString, int port,Class<? extends Serializable> paramClass, Properties conf){
		this.addressString = addressString;
		this.port = port;
		this.conf = conf;
		backlog = Integer.parseInt(conf.getProperty("server.listener.backlog","128"));
		readThreadCnt = Integer.parseInt(conf.getProperty("server.listener.readTrheadCnt", "10"));
		this.paramClass = paramClass;
	}
	
	public static void bind(ServerSocket socket, InetSocketAddress address, int backlog) throws IOException{
		try {
			socket.bind(address, backlog);
		} catch (BindException e) {
			BindException bindException = new BindException("Problem binding to "+address +" :"+e.getMessage());
			bindException.initCause(e);
			throw bindException;
		} catch (SocketException e) {
	      if ("Unresolved address".equals(e.getMessage())) {
	          throw new UnknownHostException("Invalid hostname for server: " + 
	                                         address.getHostName());
	        } else {
	          throw e;
	        }
		} 
	}
	
	private void setupResponse(ByteArrayOutputStream response, Call call, 
			Status status, Serializable rv, String error) throws IOException {
		response.reset();
		DataOutputStream out = new DataOutputStream(response);
		ObjectOutputStream oos = new ObjectOutputStream(out);
		oos.writeInt(call.id);
		oos.writeInt(status.state);
		if(status == Status.SUCCESS) {
			oos.writeObject(rv);
		}
		else {
			oos.writeObject(error);
		}
		call.setResponse(ByteBuffer.wrap(response.toByteArray()));
	}
	
   /** Called for each call. */
   public abstract Serializable call(Class<?> protocol,
		   Serializable param, long receiveTime)
   throws IOException;
	
	public void closeConnection(Connection c) {
		synchronized (connectionList) {
			if(connectionList.remove(c)) {
				numConnections--;
			}
			c.close();
		}
	}
	
	private static class Call {
		private int id;
		private Serializable param;
		private Connection connection;
		private long timestamp;
		private ByteBuffer response;
		
		public Call(int id, Serializable param, Connection connection){
			this.id = id;
			this.param = param;
			this.connection = connection;
			this.timestamp = System.currentTimeMillis();
			response = null;
		}
		
	   @Override
	   public String toString() {
	      return param.toString() + " from " + connection.toString();
	    }

	   public void setResponse(ByteBuffer response) {
	      this.response = response;
	    }
	}
	
	private class Connection {
		private boolean rpcHeaderRead = false;
		private boolean headerRead = false;
		
		private SocketChannel channel = null;
		private ByteBuffer data = null;
		private ByteBuffer dataLengthBuffer = null;
		private LinkedList<Call> responseQueue = null;
		private volatile int rpcCount = 0; // number of outstanding rpcs
		private long lastContact;
		private int dataLength;
		private Socket socket = null;
		// Cache the remote host & port info so that even if the socket is 
		// disconnected, we can say where it used to connect to.
		private String hostAddress = null;
		private int remotePort;
		private InetAddress addr = null;
		
		private ConnectionHeader header = null;
		Class<?> protocol;
		private ByteBuffer rpcHeaderBuffer;
	    
	   public Connection(SocketChannel channel, 
			   long lastContact){
		   this.channel = channel;
		   this.dataLengthBuffer = ByteBuffer.allocate(4);
		   this.setLastContact(lastContact);
		   this.socket = channel.socket();
		   this.addr = socket.getInetAddress();
		   this.remotePort = socket.getPort();
		   this.hostAddress = this.addr.getHostAddress();
		   this.responseQueue = new LinkedList<Call>();
	   }
	   
		private void setLastContact(long lastContact) {
			this.lastContact = lastContact;
		}
	   
	   private void incRpcCount(){
		   rpcCount++;
	   }
	   
	   private void decRpcCount() {
		   rpcCount--;
	   }
	   
	   private void close(){
		   data = null;
		   dataLengthBuffer = null;
		   if(!channel.isOpen())
			   return;
		   try{
			   socket.shutdownOutput();
		   } catch(Exception e) {}
		   if(channel.isOpen()){
			   try {
				channel.close();
			} catch (IOException e) {
			}
		   }
		   try {
			socket.close();
		} catch (IOException e) {
		}
		   
	   }
	   
	   
	   
	   private int readAndProcess() throws IOException, ClassNotFoundException, InterruptedException{
		   while(true){
			  int count = 0;
			  if(dataLengthBuffer.remaining() > 0) {
				  count  = channel.read(dataLengthBuffer);
				  if( count < 0 || dataLengthBuffer.remaining() > 0) 
					  return count;
			  }
			  
			  dataLengthBuffer.flip();
			  dataLength = dataLengthBuffer.getInt();
			  data = ByteBuffer.allocate(dataLength);
			  count  = channel.read(data);
			  if(data.remaining() == 0) {
				  dataLengthBuffer.clear();
				  data.flip();
			  }
			  boolean isHeaderRead = headerRead;
			  processOneRpc(data.array());
			  if(!isHeaderRead){
				  data  = null;
				  continue;
			  }
			  
			  return count;
		   }
	   }
	   
	  private void processOneRpc(byte[] buf) throws IOException, ClassNotFoundException, InterruptedException {
		   if(headerRead){
			   processData(buf);
		    }
		   else {
			   processHeader(buf);
			   headerRead = true;
		   }
	   }
	   
	  private void processData(byte[] buf) throws IOException, ClassNotFoundException, InterruptedException {
		  DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf));
		  int id = dis.readInt();
		  ObjectInputStream ois = new ObjectInputStream(dis);
		  Serializable param = (Serializable) ois.readObject();
		  Call call = new Call(id,param,this);
		  callQueue.put(call);
		  incRpcCount();
	  }
	  
	  private void processHeader(byte[] buf) throws IOException, ClassNotFoundException {
		  ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(buf));
		  header = (ConnectionHeader)ois.readObject();
		  String protocolClassName = header.getProtocol();
		  if(protocolClassName != null) {
			  protocol = Class.forName(protocolClassName);
		  }
	  }


	}
	
	private class Listener extends Thread{
		private ServerSocketChannel acceptChannel = null;
		private InetSocketAddress address = null;
		private Selector selector = null;
		private Reader[] readers = null;
		private ExecutorService readPool = null;
		private int currentReader = 0;
		
		public Listener() throws IOException{
			address = new InetSocketAddress(addressString,port);
			acceptChannel = ServerSocketChannel.open();
			acceptChannel.configureBlocking(false);
			bind(acceptChannel.socket(),address,backlog);
			port = acceptChannel.socket().getLocalPort();
			
			readers = new Reader[readThreadCnt];
			for(int i = 0; i < readThreadCnt; i++) {
				Selector readSelector = Selector.open();
				readers[i] = new Reader(readSelector);
				readPool.execute(readers[i]);
			}
			
			selector = Selector.open();
			acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
			this.setName("IPC Server listening on port "+port);
			this.setDaemon(true);
		}

		public Reader getReader() {
			currentReader = (currentReader+1)%readers.length;
			return readers[currentReader];
		}
		
		public void doAccept(SelectionKey key) throws SocketException, IOException {
			Connection c = null;
			ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
			SocketChannel channel;
			while((channel = serverChannel.accept()) != null) {
				channel.configureBlocking(false);
				channel.socket().setTcpNoDelay(false);
				Reader reader = getReader();
					reader.startAdd();
					SelectionKey readKey = channel.register(reader.readSelector, SelectionKey.OP_READ);
					c = new Connection(channel,System.currentTimeMillis());
					readKey.attach(c);
					synchronized(connectionList){
						connectionList.add(c);
						numConnections++;
					}
					reader.finishAdd();

			}
		}
		
		public void doRead(SelectionKey key) throws InterruptedException {
			int count = 0;
			Connection c = (Connection) key.attachment();
			if(c == null)
				return;
			try{
				count = c.readAndProcess(); 
			}catch (InterruptedException e) {
				throw e;
			}
			catch(Exception ex){
				count = -1;
			}
			if(count < 0) {
				closeConnection(c);
				c = null;
			}
			else {
				c.setLastContact(System.currentTimeMillis());
			}
		}
		
		@Override
		public void run() {
			SERVER.set(Server.this);
			while(running){
				SelectionKey key = null;
				try {
					selector.select();
					Iterator<SelectionKey> itr = selector.selectedKeys().iterator();
					while(itr.hasNext()){
						key = itr.next();
						itr.remove();
						if(key.isValid()){
							if(key.isAcceptable()){
								doAccept(key);
							}
						}
					}
				} catch (OutOfMemoryError err) {
					// TODO Auto-generated catch block
					err.printStackTrace();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}
		
		private class Reader implements Runnable {
			   private volatile boolean adding = false;
			   private Selector readSelector = null;
				
			   public Reader(Selector readSelector) {
					this.readSelector = readSelector;
				}
			   
			   public void startAdd(){
				   adding = true;
				   readSelector.wakeup();
			   }
			   
			   public void finishAdd(){
				   adding = false;
				   this.notify();
			   }

				public void run() {
					synchronized (this) {
						while(running){
							SelectionKey key = null;
							try{
								readSelector.select();
								while(adding){
									this.wait(1000);
								}
								Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
								while(iter.hasNext()){
									key = iter.next();
									iter.remove();
									if(key.isValid()){
										if(key.isReadable()){
											doRead(key);
										}
									}
								}
							}catch (InterruptedException e) {
								if(running) {
									//记录info日志
								}
							}catch (IOException ex){
								//记录error日志
							}

						}
					}
					
				}
			}
	}
	
	private class Handler extends Thread{
		public Handler(int instanceNumber){
			this.setDaemon(true);
			this.setName("IPC Server handler "+instanceNumber+" on "+port);
		}

		@Override
		public void run() {
			SERVER.set(Server.this);
			while(running) {
				try {
					final Call call = callQueue.take();
					Serializable value = null;
					String error = null;
					curCall.set(call);
					try {
						value = call(call.connection.protocol,call.param,call.timestamp);
					} catch (IOException ioe) {
						// TODO Auto-generated catch block
						error = ioe.getMessage();
					}
					curCall.set(null);
					synchronized (call.connection.responseQueue) {
						 
					}
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					
				}
				
			}
		}
		
	}

}
