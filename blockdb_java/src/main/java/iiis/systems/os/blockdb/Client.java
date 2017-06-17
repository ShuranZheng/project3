package iiis.systems.os.blockdb;


import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import iiis.systems.os.blockchaindb.BlockChainMinerGrpc;
import iiis.systems.os.blockchaindb.BlockChainMinerGrpc.BlockChainMinerBlockingStub;
import iiis.systems.os.blockchaindb.JsonBlockString;
import iiis.systems.os.blockchaindb.Transaction;

	/**
	 * A simple client that requests a greeting from the {@link HelloWorldServer}.
	 */
	public class Client {
	  private final ManagedChannel channel;
	  private final BlockChainMinerBlockingStub blockingStub;

	  /** Construct client connecting to HelloWorld server at {@code host:port}. */
	  public Client(String host, int port) {
	    this(ManagedChannelBuilder.forAddress(host, port)
	        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
	        // needing certificates.
	        .usePlaintext(true)
	        .build());
	  }

	  /** Construct client for accessing RouteGuide server using the existing channel. */
	  Client(ManagedChannel channel) {
	    this.channel = channel;
	    blockingStub = BlockChainMinerGrpc.newBlockingStub(channel);
	  }

	  public void shutdown() throws InterruptedException {
	    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	  }

	  
	  public void pushTrans(Transaction trans) {
	    try {
	      blockingStub.pushTransaction(trans);
	    } catch (StatusRuntimeException e) {
	      return;
	    }
	   }
	  
	  public void pushBlock(JsonBlockString block) {
		    try {
		      blockingStub.pushBlock(block);
		    } catch (StatusRuntimeException e) {
		      return;
		    }
		   }
}
