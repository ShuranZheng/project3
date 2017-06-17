package iiis.systems.os.blockdb;

import iiis.systems.os.blockchaindb.VerifyResponse.Results;

	public class VerifyResult{
    	public Results result;
    	public String blockHash;
    	
    	public VerifyResult(Results result, String blockHash){
    		this.result = result;
    		this.blockHash = blockHash;
    	}
    };

