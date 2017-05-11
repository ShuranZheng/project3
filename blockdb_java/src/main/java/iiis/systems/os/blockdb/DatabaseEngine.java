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

    private HashMap<String, Integer> balances = new HashMap<>();
    private HashMap<String, ReadWriteLock> locks = new HashMap<>();
    private int logLength = 0;
    private String dataDir;

    DatabaseEngine(String dataDir) {
        this.dataDir = dataDir;
    }
    
    private ReadWriteLock getLock(String userId){
    	if (locks.containsKey(userId)) return locks.get(userId);
    	locks.put(userId, new ReentrantReadWriteLock());
    	return locks.get(userId);
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
    	try{
	    	FileWriter output = new FileWriter("log.txt", true);
	    	if (type == "TRANSFER")
	    		output.write("{\"Type\":\""+type+"\",\"Value\":"+value+",\"FromID\":\""+fromId+"\",\"ToID\":\""+toId+"\"}\n");
	    	else output.write("{\"Type\":\""+type+"\",\"UserID\":\""+fromId+"\",\"Value\":"+value+"}");
	    }catch (IOException e){
	    	e.printStackTrace();
    	}
    }

    public boolean put(String userId, int value) {
        ReadWriteLock lock = getLock(userId);
        lock.writeLock().lock();
        try{
        	balances.put(userId, value);
        }
        finally{
        	lock.writeLock().unlock();
        }
      //*********************************************
        logLength++;
        //Write the log
        writeLog("PUT", userId, "", value);
        
      //**********************************************
        return true;
    }

    public boolean deposit(String userId, int value) {
        ReadWriteLock lock = getLock(userId);
        lock.writeLock().lock();
        try{
        	int balance = getOrZero(userId);
            balances.put(userId, balance + value);
        }
        finally{
        	lock.writeLock().unlock();
        }
        
    //*************************************************
        logLength++;
    	//Write the log
        writeLog("DEPOSIT", userId, "", value);
    //*************************************************
        return true;
    }

    public boolean withdraw(String userId, int value) {
        ReadWriteLock lock = getLock(userId);
        lock.writeLock().lock();
        try{
        	int balance = getOrZero(userId);
        	if (balance - value < 0) return false;
        	balances.put(userId, balance - value);
        }
        finally{
        	lock.writeLock().unlock();
        }
      //***********************************************
    	logLength++;
        //Write the log
        writeLog("WITHDRAW", userId, "", value);
    //*************************************************
    	return true;
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
        }
        finally{
        	toLock.writeLock().unlock();
        }

        //***********************************
        logLength++;
        //Write the log
        writeLog("TRANSFER", fromId, toId, value);
        //*************************************
        return true;
    }

    public int getLogLength() {
        return logLength;
    }
}
