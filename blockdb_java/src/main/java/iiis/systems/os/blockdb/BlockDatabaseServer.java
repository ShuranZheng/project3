package iiis.systems.os.blockdb;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import iiis.systems.os.blockchaindb.Null;
import iiis.systems.os.blockchaindb.BooleanResponse;
import iiis.systems.os.blockchaindb.GetBlockRequest;
import iiis.systems.os.blockchaindb.GetRequest;
import iiis.systems.os.blockchaindb.GetResponse;
import iiis.systems.os.blockchaindb.JsonBlockString;
import iiis.systems.os.blockchaindb.Transaction;
import iiis.systems.os.blockchaindb.BlockChainMinerGrpc.BlockChainMinerBlockingStub;
import iiis.systems.os.blockchaindb.BlockChainMinerGrpc.BlockChainMinerImplBase;
import iiis.systems.os.blockchaindb.GetHeightResponse;
import iiis.systems.os.blockchaindb.VerifyResponse;
import iiis.systems.os.blockchaindb.VerifyResponse.Results;

import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;


public class BlockDatabaseServer {
    private Server server;

    private void start(String address, int port) throws IOException {
        server = NettyServerBuilder.forAddress(new InetSocketAddress(address, port))
                .addService(new BlockChainMinerImpl())
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
    
    static String me;
    static JSONObject conf;
    
    
    public static void main(String[] args) throws IOException, JSONException, InterruptedException {
    	
    	//testDatabaseOperation(0, 1, 0);
    	me = args[0].substring(args[0].indexOf('=') + 1);
    	//System.out.println(me);
    	JSONObject config = Util.readJsonFile("config.json");
    	conf = config;
        config = (JSONObject)config.get(me);
        String address = config.getString("ip");
        int port = Integer.parseInt(config.getString("port"));
        String dataDir = config.getString("dataDir");

        DatabaseEngine.setup(dataDir);
        DatabaseEngine dbEngine = DatabaseEngine.getInstance();
        if (!dbEngine.recover()) {
        	System.out.println("Fail to start the database.");
        }else{
        	final BlockDatabaseServer server = new BlockDatabaseServer();
        	server.start(address, port);
        	server.blockUntilShutdown();
        }
    }
    
    
    
    static class BlockChainMinerImpl extends BlockChainMinerImplBase {
        private final DatabaseEngine dbEngine = DatabaseEngine.getInstance();
        
        /**
         * <pre>
         * Return UserID's Balance on the Chain, after considering the latest valid block. Pending transactions have no effect on Get()
         * </pre>
         */
        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            int value = dbEngine.get(request.getUserID());
            GetResponse response = GetResponse.newBuilder().setValue(value).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

      /*  @Override
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
        }*/
        
        /**
         * <pre>
         * Receive and Broadcast Transaction: balance[FromID]-=Value, balance[ToID]+=(Value-MiningFee), balance[MinerID]+=MiningFee
         * Return Success=false if FromID is same as ToID or latest balance of FromID is insufficient
         * </pre>
         */
        @Override
        public void transfer(Transaction trans, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.transfer(trans);
            if (!success) {
	            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
	            responseObserver.onNext(response);
	            responseObserver.onCompleted();
	            return;
            }
            
            int nservers = conf.getInt("nservers");
            for (int i = 1; i<= nservers; i++)
            if (!Integer.toString(i).equals(me)){
            	JSONObject config = (JSONObject)conf.get(Integer.toString(i));
                String address = config.getString("ip");
                int port = Integer.parseInt(config.getString("port"));
               Client c = new Client(address, port);
               c.pushTrans(trans);
            }
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        
        /**
         * <pre>
         * Check if a transaction has been written into a block, or is still waiting, or is invalid on the longest branch.
         * </pre>
         */
        @Override
        public void verify(Transaction trans, StreamObserver<VerifyResponse> responseObserver){
        	VerifyResult v = dbEngine.verify(trans);
            VerifyResponse response = VerifyResponse.newBuilder().setResult(v.result).setBlockHash(v.blockHash).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        
        /**
         * <pre>
         * Get the current blockchain length; use the longest branch if multiple branch exist.
         * </pre>
         */
        @Override
        public void getHeight(Null request, StreamObserver<GetHeightResponse> responseObserver){
        	GetHeightResult g = dbEngine.getHeight();
        	GetHeightResponse response = GetHeightResponse.newBuilder().setHeight(g.height).setLeafHash(g.leafHash).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
        
        
        /**
         * <pre>
         * Get the Json representation of the block with BlockHash hash value
         * </pre>
         */
        @Override
        public void getBlock(GetBlockRequest request,
                StreamObserver<JsonBlockString> responseObserver) {
            }

            /**
             * <pre>
             * Send a block to another server
             * </pre>
             */
        @Override
            public void pushBlock(JsonBlockString request,
                StreamObserver<Null> responseObserver) {
              
            }

            /**
             * <pre>
             * Send a transaction to another server
             * </pre>
             */
        @Override
            public void pushTransaction(Transaction trans,
                StreamObserver<Null> responseObserver) {
              dbEngine.transfer(trans);
            }

       /* @Override
        public void logLength(Null request, StreamObserver<GetResponse> responseObserver) {
            int value = dbEngine.getLogLength();
            GetResponse response = GetResponse.newBuilder().setValue(value).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }*/
    }
}
