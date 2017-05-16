package iiis.systems.os.blockdb;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;

public class BlockDatabaseServer {
    private Server server;

    private void start(String address, int port) throws IOException {
        server = NettyServerBuilder.forAddress(new InetSocketAddress(address, port))
                .addService(new BlockDatabaseImpl())
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BlockDatabaseServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
    
    static public void testDatabaseOperation(){
    	DatabaseEngine.setup("aaa");
    	final DatabaseEngine dbEngine = DatabaseEngine.getInstance();

		Thread t1 = new Thread(new Runnable(){
		    public void run(){
		    	for (int i = 0; i < 50; i++){
		    		System.out.println(Integer.toString(i) + " deposit " + Boolean.toString(dbEngine.deposit(Integer.toString(i), 100)));
		    		dbEngine.get(Integer.toString(i));
		    		try {
		    		Thread.sleep(1000);
		    		} catch(InterruptedException e){
		    			e.printStackTrace();
		    		}
		    	}
		    }
		});
		t1.start();
		
		Thread t2 = new Thread(new Runnable(){
		    public void run(){
		    	for (int i = 0; i < 50; i++){
		    		
		    		System.out.println(Integer.toString(i) + " withdraw " + Boolean.toString(dbEngine.withdraw(Integer.toString(i), 50)));
		    		dbEngine.get(Integer.toString(i));
		    		try {
		    		Thread.sleep(1000);
		    		} catch(InterruptedException e){
		    			e.printStackTrace();
		    		}
		    	}
		    }
		});
		t2.start();
		
		
		Thread t3 = new Thread(new Runnable(){
		    public void run(){
		    	try{
		    		Thread.sleep(100);
		    		}catch (Exception e){
		    			e.printStackTrace();
		    		}
		    	for (int i = 0; i < 50; i++){
		    		System.out.println(Integer.toString(i) + " transfer " + Boolean.toString(dbEngine.transfer(Integer.toString(i),Integer.toString(i+1), 50)));
		    		dbEngine.get(Integer.toString(i));
		    		/*try {
		    		Thread.sleep(1000);
		    		} catch(InterruptedException e){
		    			e.printStackTrace();
		    		}*/
		    	}
		    }
		});
		t3.start();
    }
    
    public static void main(String[] args) throws IOException, JSONException, InterruptedException {
    	
    	testDatabaseOperation();
    	
    	/*JSONObject config = Util.readJsonFile("config.json");
        config = (JSONObject)config.get("1");
        String address = config.getString("ip");
        int port = Integer.parseInt(config.getString("port"));
        String dataDir = config.getString("dataDir");

        DatabaseEngine.setup(dataDir);

        final BlockDatabaseServer server = new BlockDatabaseServer();
        server.start(address, port);
        server.blockUntilShutdown();*/
    }

    static class BlockDatabaseImpl extends BlockDatabaseGrpc.BlockDatabaseImplBase {
        private final DatabaseEngine dbEngine = DatabaseEngine.getInstance();

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            int value = dbEngine.get(request.getUserID());
            GetResponse response = GetResponse.newBuilder().setValue(value).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void put(Request request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.put(request.getUserID(), request.getValue());
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void withdraw(Request request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.withdraw(request.getUserID(), request.getValue());
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void deposit(Request request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.deposit(request.getUserID(), request.getValue());
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void transfer(TransferRequest request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.transfer(request.getFromID(), request.getToID(), request.getValue());
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void logLength(Null request, StreamObserver<GetResponse> responseObserver) {
            int value = dbEngine.getLogLength();
            GetResponse response = GetResponse.newBuilder().setValue(value).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
