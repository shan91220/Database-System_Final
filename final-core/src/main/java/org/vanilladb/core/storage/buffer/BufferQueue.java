package org.vanilladb.core.storage.buffer;

import org.vanilladb.core.storage.file.BlockId;

public class BufferQueue {
	private BlockId blk;
	private int buffPoolIndex;
	
	BufferQueue(BlockId blk, int buffPoolIndex){
		this.blk = blk;
		this.buffPoolIndex = buffPoolIndex;
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
}
