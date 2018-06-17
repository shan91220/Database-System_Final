package org.vanilladb.core.storage.buffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.util.CoreProperties;

public class LRUHistory {
	private BlockId blk;
	private long lastK;
	private List<Long> pinTime;
	private int access;
//	private static final long LRU_K;
	
//	static {
//		LRU_K = CoreProperties.getLoader().getPropertyAsLong(BufferPoolMgr.class.getName()
//				+ ".LRU_K", 2);
//	}
	
	LRUHistory(BlockId blk, long time){
		this.blk = blk;
		if(k!=1)
			this.lastK = -1;  // inf
		else
			this.lastK = time;
		pinTime = new ArrayList<Long>();
		pinTime.add(time);
		this.access = 1;
	}
//	public boolean isK() {
//		return hits >= LRU_K;
//	}
	public int getAccessTime() {
		return this.access;
	}
	public void accessAgain(long time, int k) {
		this.access++;
		pinTime.add(time);
		if (access >= k) {
			if(pinTime.size()-k < 0)  System.out.println("Error!!!!!!!!!!");
			this.lastK = pinTime.get(pinTime.size()-k);
		}
	}
	public long getLastKpinTime(int K) {
		return this.lastK;
	}
}

