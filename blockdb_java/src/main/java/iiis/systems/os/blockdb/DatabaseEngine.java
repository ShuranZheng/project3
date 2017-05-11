package iiis.systems.os.blockdb;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DatabaseEngine {
    private static DatabaseEngine instance = null;

    public static DatabaseEngine getInstance() {
        return instance;
    }

    public static void setup(String dataDir) {
        instance = new DatabaseEngine(dataDir);
    }

    public static final boolean FAIR = true;
    private HashMap<String, Integer> balances = new HashMap<>();
    private HashMap<String, ReadWriteLock> locks = new HashMap<>();
    private int logLength = 0;
    private Lock lockLock = new ReentrantLock(FAIR);
    private String dataDir;

    DatabaseEngine(String dataDir) {
        this.dataDir = dataDir;
    }
    
    private ReadWriteLock getLock(String userId){
    	lockLock.lock();
    	try{
    		if (!locks.containsKey(userId)) locks.put(userId, new ReentrantReadWriteLock(FAIR));
    		return locks.get(userId);
    	}finally{
    		lockLock.unlock();
    	}
    }

    private int getOrZero(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            return 0;
        }
    }

    public int get(String userId) {
        ReadWriteLock lock = getLock(userId);
        lock.readLock().lock();
        try{
            return getOrZero(userId);
        }
        finally{
        	lock.readLock().unlock();
        }   
    }
    
    private void writeLog(String type, String fromId, String toId, int value){
    	 logLength++;
    	try{
	    	FileWriter output = new FileWriter("log.txt", true);
	    	if (type == "TRANSFER")
	    		output.write("{\"Type\":\""+type+"\",\"Value\":"+value+",\"FromID\":\""+fromId+"\",\"ToID\":\""+toId+"\"}"+System.lineSeparator());
	    	else output.write("{\"Type\":\""+type+"\",\"UserID\":\""+fromId+"\",\"Value\":"+value+"}"+System.lineSeparator());
	    	output.flush();
	    	output.close();
	    }catch (IOException e){
	    	e.printStackTrace();
    	}
    }

    public boolean put(String userId, int value) {
        ReadWriteLock lock = getLock(userId);
        lock.writeLock().lock();
        try{
        	balances.put(userId, value);
        	
        	//*********************************************
            //Write the log
            writeLog("PUT", userId, "", value);
            
          //**********************************************
            return true;
            
        }
        finally{
        	lock.writeLock().unlock();
        }
      
    }

    public boolean deposit(String userId, int value) {
        ReadWriteLock lock = getLock(userId);
        lock.writeLock().lock();
        try{
        	int balance = getOrZero(userId);
            balances.put(userId, balance + value);
            
          //*************************************************
        	//Write the log
            writeLog("DEPOSIT", userId, "", value);
         //   System.out.println(balances.get(userId));
        //*************************************************
            return true;
        }
        finally{
        	lock.writeLock().unlock();
        }
        
    }

    public boolean withdraw(String userId, int value) {
        ReadWriteLock lock = getLock(userId);
        lock.writeLock().lock();
        try{
        	int balance = getOrZero(userId);
        	if (balance - value < 0) return false;
        	balances.put(userId, balance - value);
        	
        	
        //***********************************************
            //Write the log
            writeLog("WITHDRAW", userId, "", value);
        //    System.out.println(balances.get(userId));
        //*************************************************
        	return true;
      
        }
        finally{
        	lock.writeLock().unlock();
        }
    }

    public boolean transfer(String fromId, String toId, int value) {
        
        ReadWriteLock fromLock = getLock(fromId);
        fromLock.writeLock().lock();
        try{
        	int fromBalance = getOrZero(fromId);
        	if (fromBalance - value < 0) return false;
        	balances.put(fromId, fromBalance - value);
        }
        finally{
        	fromLock.writeLock().unlock();
        }
        ReadWriteLock toLock = getLock(toId);
        toLock.writeLock().lock();
        try{
        	int toBalance = getOrZero(toId);
        	balances.put(toId, toBalance + value);
        	
        	 //***********************************
            //Write the log
            writeLog("TRANSFER", fromId, toId, value);
            //*************************************
            //System.out.println(balances.get(fromId));
            return true;
            
        }
        finally{
        	toLock.writeLock().unlock();
        }

       
    }

    public int getLogLength() {
        return logLength;
    }
}
