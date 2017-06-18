package iiis.systems.os.blockdb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.json.JSONArray;
import org.json.JSONObject;

import iiis.systems.os.blockchaindb.Null;
import iiis.systems.os.blockchaindb.BooleanResponse;
import iiis.systems.os.blockchaindb.GetRequest;
import iiis.systems.os.blockchaindb.GetResponse;
import iiis.systems.os.blockchaindb.JsonBlockString;
import iiis.systems.os.blockchaindb.Transaction;
import iiis.systems.os.blockchaindb.BlockChainMinerGrpc.BlockChainMinerImplBase;
import iiis.systems.os.blockchaindb.GetHeightResponse;
import iiis.systems.os.blockchaindb.VerifyResponse;
import iiis.systems.os.blockchaindb.VerifyResponse.Results;
import iiis.systems.os.blockdb.hash.Hash;

public class DatabaseEngine {
    private static DatabaseEngine instance = null;

    public static DatabaseEngine getInstance() {
        return instance;
    }

    public static void setup(String dataDir) {
        instance = new DatabaseEngine(dataDir);
    }
    
    public boolean recoverTrans(JSONObject transaction){
    	String type = transaction.getString("Type");
    	if (transaction.getInt("Value")<0) return false;
    	if (type.equals("TRANSFER")){
    		String fromId = transaction.getString("FromID");
    		String toId = transaction.getString("ToID");
    		int value = transaction.getInt("Value");
    		int fromBalance = getOrZero(fromId);
            int toBalance = getOrZero(toId);
            if (fromBalance - value < 0) return false;
            balances.put(fromId, fromBalance - value);
            balances.put(toId, toBalance + value);
    	}
    	if (type.equals("DEPOSIT")){
    		String userId = transaction.getString("UserID");
    		int value = transaction.getInt("Value");
    		int balance = getOrZero(userId);
            balances.put(userId, balance + value);
    	}
    	if (type.equals("PUT")){
    		String userId = transaction.getString("UserID");
    		int value = transaction.getInt("Value");
            balances.put(userId, value);
    	}
    	if (type.equals("WITHDRAW")){
    		String userId = transaction.getString("UserID");
    		int value = transaction.getInt("Value");
    		int balance = getOrZero(userId);
    		if (balance - value < 0) return false;
            balances.put(userId, balance - value);
    	}
    	return true;
    }
    
    public boolean recover() throws IOException{
    	logLength = 0;
    	blockNum = 0;
    	//System.out.println("recover");
    	File logFile = new File(dataDir + "log.json");
    	File firstBlock = new File(dataDir + "1.json");
    	if (firstBlock.exists() && (!logFile.exists())) {
    		System.out.println("Missing log file!");
    		return false;
    	}
    	JSONObject log = null;
    	if (logFile.exists()) {
			log = Util.readJsonFile(dataDir + "log.json");
			blockNum = log.getInt("BlockNumber");
    	}
    	//while ((new File(dataDir + "/" + Integer.toString(blockNum+1) + ".json")).exists()) blockNum ++;
    	for (int i = 1; i <= blockNum; i++){
    		File blockI = new File(dataDir + Integer.toString(i) + ".json");
    		if (!blockI.exists()) {
    			System.out.println("Missing "+i+".json!");
    			return false;
    		}
    		JSONObject block = Util.readJsonFile(dataDir + Integer.toString(i) + ".json");
            JSONArray trans = block.getJSONArray("Transactions");
            for (int j = 0; j < trans.length(); j++)
            	if (!recoverTrans(trans.getJSONObject(j))){
            		System.out.println("Inconsistent block files or log files!");
            		return false;
            	}
    	}
    	
		if (logFile.exists()) {
			JSONArray trans = log.getJSONArray("Transactions");
			logLength = trans.length();
			for (int j = 0; j < trans.length(); j++)
				if (!recoverTrans(trans.getJSONObject(j))){
            		System.out.println("Inconsistent block files or log files!");
            		return false;
            	}
		}
		return true;
    	
    }

    public static final boolean FAIR = true;
    private HashMap<String, Integer> balances = new HashMap<>();
    private HashMap<String, Integer> approved = new HashMap<>();
    public ArrayList<String> blockStrings = new ArrayList<>();
    private LinkedList<Transaction> pendingTrans = new LinkedList<>();
    public HashMap<String, Integer> uuid = new HashMap<>();
 //   private HashMap<String, ReadWriteLock> locks = new HashMap<>();
    private int logLength = 0, blockNum = 0;
    private final int N = 50;
    private ReadWriteLock RWLock = new ReentrantReadWriteLock(FAIR);
    private ReadWriteLock appLock = new ReentrantReadWriteLock(FAIR);
    private String dataDir;
    private int height = 0;

    DatabaseEngine(String dataDir) {
        this.dataDir = dataDir;
    }
    
 /*   private ReadWriteLock getLock(String userId){
    	lockLock.lock();
    	try{
    		if (!locks.containsKey(userId)) locks.put(userId, new ReentrantReadWriteLock(FAIR));
    		return locks.get(userId);
    	}finally{
    		lockLock.unlock();
    	}
    }*/

    private int getOrZero(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
        	balances.put(userId, 1000);
            return 1000;
        }
    }

    public int get(String userId) {
       // ReadWriteLock lock = getLock(userId);
        appLock.readLock().lock();
        try{
        	if (!approved.containsKey(userId)) return 1000;
        	else return approved.get(userId);
        }
        finally{
        	appLock.readLock().unlock();
        }   
    }
    
    /*private void writeLog(String type, String fromId, String toId, int value){
    
    	try{
    		File dataFolder = new File(dataDir);
    		if (!dataFolder.exists()) dataFolder.mkdir();
    		File logFile = new File(dataDir + "log.json");
    		
    		if (logLength ==  N) {
    			blockNum ++;
    			JSONObject log = Util.readJsonFile(dataDir + "log.json");
    			BufferedWriter blockWriter = new BufferedWriter(new FileWriter(dataDir  + "T" + Integer.toString(blockNum) + ".json"));
    			log.put("BlockID", blockNum);
    			log.put("Nonce", "00000000");
    			log.put("PrevHash", "00000000");
    			log.remove("BlockNumber");
    			log.write(blockWriter);
    			blockWriter.close();
    			logLength = 0;
        		logFile.delete();
    		}
    		
    		
    		logLength++;
    		//System.out.println(logLength);
    		JSONObject log = null;
    		JSONArray trans = null;
    		if (logFile.exists()) {
    			log = Util.readJsonFile(dataDir + "log.json");
    			trans = log.getJSONArray("Transactions");
    			
    		}
    		else {
    			log = new JSONObject();
    			trans = new JSONArray();
    			log.put("Transactions", trans);
    		}
            
            JSONObject transaction = new JSONObject();
            transaction.put("Type", type);
            if (type.equals("TRANSFER")){
            	transaction.put("FromID", fromId);
            	transaction.put("ToID", toId);
            	transaction.put("Value", value);
            } else
            {
            	transaction.put("UserID", fromId);
            	transaction.put("Value", value);
            }
            trans.put(transaction);
            log.put("BlockNumber", blockNum);
	    	BufferedWriter logWriter = new BufferedWriter(new FileWriter(dataDir + "log.json"));
	    	log.write(logWriter);
	 //   	logWriter.flush();
	    	logWriter.close();
	    }catch (IOException e){
	    	e.printStackTrace();
    	}
    }

    public boolean put(String userId, int value) {
    	if (value < 0) return false;
   //     ReadWriteLock lock = getLock(userId);
        RWLock.writeLock().lock();
        try{
        	balances.put(userId, value);
            return true;
            
        }
        finally{
        	//*********************************************
            //Write the log
        	writeLog("PUT", userId, "", value);
        	//**********************************************
            
        	RWLock.writeLock().unlock();
        }
      
    }

    

    public boolean withdraw(String userId, int value) {
    	if (value < 0) return false;
       // ReadWriteLock lock = getLock(userId);
        RWLock.writeLock().lock();
        int balance = getOrZero(userId);
    	if (balance - value < 0) {
    		RWLock.writeLock().unlock();
    		return false;
    	}
        try{
        	balances.put(userId, balance - value);
          	return true;
      
        }
        finally{
        	//***********************************************
            //Write the log
            writeLog("WITHDRAW", userId, "", value);
        //    System.out.println(balances.get(userId));
        //************************************************
        	RWLock.writeLock().unlock();
        }
    }*/
    
    public boolean deposit(String userId, int value) {
    	if (value < 0) return false;
       // ReadWriteLock lock = getLock(userId);
        RWLock.writeLock().lock();
        try{
        	int balance = getOrZero(userId);
            balances.put(userId, balance + value);
            return true;
        }
        finally{
        	  
        	RWLock.writeLock().unlock();
        }
        
    }

    public boolean transfer(Transaction trans) {
    	if (uuid.containsKey(trans.getUUID())) return false;
    	int value = trans.getValue();
    	int fee = trans.getMiningFee();
    	String fromId = trans.getFromID();
    	String toId = trans.getToID();
    	
        if ((value - fee < 0) || (fee < 0) || (fromId.equals(toId))) return false;
       // ReadWriteLock fromLock = getLock(fromId);
        RWLock.writeLock().lock();
        try{
        	int fromBalance = getOrZero(fromId);
        	if (fromBalance - value < 0) return false;
        	balances.put(fromId, fromBalance - value);
        }
        finally{
        	RWLock.writeLock().unlock();
        }
      //  ReadWriteLock toLock = getLock(toId);
        RWLock.writeLock().lock();
        try{
        	int toBalance = getOrZero(toId);
        	balances.put(toId, toBalance + value - fee);
        	pendingTrans.add(trans);
        	uuid.put(trans.getUUID(), -1);
        	 //***********************************
            //Write the log
            //writeLog("TRANSFER", fromId, toId, value);
            //*************************************
        	
        	//System.out.println(balances.get(fromId));

            return true;            
        }
        finally{
        	RWLock.writeLock().unlock();
        }
        
    }
    
    public boolean approvedTransfer(String fromId, String toId, int value, int fee, int mul){
    	if (!approved.containsKey(fromId)) approved.put(fromId, 1000);
		if (!approved.containsKey(toId)) approved.put(toId, 1000);
		int from = approved.get(fromId);
		approved.put(fromId, from - value*mul);
		int to = approved.get(toId);
		approved.put(toId, to + (value -fee)*mul);
		if (mul == 1 && from - value*mul < 0) return false;
		return true;
    }

    public void pushBlock(JsonBlockString request){
    	appLock.writeLock().lock();
    	try{
    	String json = request.getJson();
    	JSONObject req = new JSONObject(request.getJson());
    	String prev = req.getString("PrevHash");
    	String miner = req.getString("MinerID");
    	if (!miner.matches("Server\\d{2}")) return;
    	
    	if (height > 0 && !Hash.getHashString(blockStrings.get(height - 1)).equals(prev)) {
    		//System.out.println("Prev Hash not correct!");
    		return;
    	}
    	if (!Hash.checkHash(Hash.getHashString(json))) {
    		//System.out.println("Invalid block");
    		return;
    	}
    	
    	height++;
    	blockStrings.add(json);
    //	System.out.println(json);
    /*	try{
    		File dataFolder = new File(dataDir);
    		if (!dataFolder.exists()) dataFolder.mkdir();
    	BufferedWriter blockWriter = new BufferedWriter(new FileWriter(dataDir  + Integer.toString(height) + ".json"));
    	blockWriter.write(json);
		blockWriter.close();
    	}catch (IOException e){
    		e.printStackTrace();
    	}*/
    	
    	int totalFee = 0;
    	JSONArray records = req.getJSONArray("Transactions");
    	for (int i = 0; i < records.length(); i++){
    		JSONObject trans = records.getJSONObject(i);
    		String fromId = trans.getString("FromID");
    		String toId = trans.getString("ToID");
    		int value = trans.getInt("Value");
    		int fee = trans.getInt("MiningFee");
    		totalFee += fee;
    		if (!approvedTransfer(fromId, toId, value, fee, 1)){
    			for (int j = 0; j <= i; j++){
    				JSONObject transJ = records.getJSONObject(j);
    	    		String fromIdJ = transJ.getString("FromID");
    	    		String toIdJ = transJ.getString("ToID");
    	    		int valueJ = transJ.getInt("Value");
    	    		int feeJ = transJ.getInt("MiningFee");
    	    		approvedTransfer(fromIdJ, toIdJ, valueJ, feeJ, -1);
    			}
    			return;
    		}
    	}
    	
    	deposit(miner, totalFee);
    	if (!approved.containsKey(miner)) approved.put(miner, 1000);
		int minerBalance = approved.get(miner);
		approved.put(miner, minerBalance + totalFee);
		
    	for (int i=0; i<records.length(); i++){
    		JSONObject trans = records.getJSONObject(i);
    		String u = trans.getString("UUID");
    		uuid.put(u, height);
    	}
    	}
    	finally{
    		appLock.writeLock().unlock();
    	}
    }
    
    public int getLogLength() {
        return logLength;
    }
    
    public VerifyResult verify(Transaction trans){
    	String u = trans.getUUID();
    	if (!uuid.containsKey(u)) return new VerifyResult(Results.FAILED, null);
    	int bn = uuid.get(u);
    	if (bn>=0 && bn<= height - 6) 
    		return new VerifyResult(Results.SUCCEEDED, Hash.getHashString(blockStrings.get(bn-1)));
    	return new VerifyResult(Results.PENDING, null);
    }
    
    public String getBlock(String hash){
    	for (int i = 0; i < height; i++)
    		if (Hash.getHashString(blockStrings.get(i)).equals(hash)) return blockStrings.get(i);
    	return null;
    }
    
    public GetHeightResult getHeight(){
    	GetHeightResult g = new GetHeightResult(height, Hash.getHashString(blockStrings.get(height-1)));
    	return g;
    }
}
