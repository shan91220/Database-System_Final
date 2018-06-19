package org.vanilladb.core.storage.buffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.util.CoreProperties;

public class LRUHistory {
	private BlockId blk;
	private int index;
	private long lastK;
	private List<Long> pinTime;
	private List<Long> unpinTime;
//	private int access;
	
	LRUHistory(BlockId blk, long time, int k, int index){
		this.index = index;
		this.blk = blk;
		if(k!=1)
			this.lastK = -1;  // inf
		else
			this.lastK = time;
		pinTime = new ArrayList<Long>();
		unpinTime = new ArrayList<Long>();
		pinTime.add(time);
//		this.access = 1;
	}
	public BlockId getBlockId() {
		return blk;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	public int getIndex() {
		return this.index;
	}
//	public int getAccessTime() {
//		return this.access;
//	}
	public void accessAgain(long time, int k) {
//		this.access++;
		pinTime.add(time);
		if (pinTime.size() >= k) {
			if(pinTime.size()-k < 0)  System.out.println("Error!!!!!!!!!!");
			this.lastK = pinTime.get(pinTime.size()-k);
		}
	}
	public long getLastKpinTime() {
		return this.lastK;
	}
	public void setTime(long time, int k) {  // for unpin
		unpinTime.add(time);
		if (unpinTime.size() >= k) {
			if(unpinTime.size()-k < 0)  System.out.println("Error!!!!!!!!!!");
			this.lastK = unpinTime.get(unpinTime.size()-k);
		}
	}
}

