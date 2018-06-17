package org.vanilladb.core.storage.buffer;

import org.vanilladb.core.storage.file.BlockId;

public class BufferQueue {
	private BlockId blk;
	private int buffPoolIndex;
//	private int lastPinTime;
//	private int last_1pinTime;
//	private int access;
	
	BufferQueue(BlockId blk, int buffPoolIndex){
		this.blk = blk;
		this.buffPoolIndex = buffPoolIndex;
//		this.last_1pinTime = -1; // inf
//		this.lastPinTime = time;
//		this.access = 1;
	}
	public BlockId getBlockId() {
		return this.blk;
	}
	public void setBlockId(BlockId blk) {
		this.blk = blk;
	}
	public int getIndex() {
		return this.buffPoolIndex;
	}
//	public int getAccessTime() {
//		return this.access;
//	}
//	public void accessAgain(int time, int k) {
//		this.access++;
//		lastK_1pinTime = time;
//		
//	}
//	public int getLastKpinTime() {
//		return this.lastKpinTime;
//	}
//	public int getLastK_1pinTime() {
//		return this.lastK_1pinTime;
//	}
}
