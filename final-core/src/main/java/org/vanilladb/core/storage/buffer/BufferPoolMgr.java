/*******************************************************************************
 * Copyright 2017 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core.storage.buffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.util.CoreProperties;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 */
class BufferPoolMgr {
	private Buffer[] bufferPool;
	private Map<BlockId, Buffer> blockMap;
	private HashMap<BlockId, LRUHistory> historyMap;
	private List<BufferQueue> buffQueue;
	private long timer;
	private int maxHistory;
	private volatile int lastReplacedBuff;
	private AtomicInteger numAvailable;
	// Optimization: Lock striping
	private Object[] anchors = new Object[1009];
	private static final int LRU_K;
	
	static {
		LRU_K = CoreProperties.getLoader().getPropertyAsInteger(BufferPoolMgr.class.getName()
				+ ".LRU_K", 2);
	}
	

	/**
	 * Creates a buffer manager having the specified number of buffer slots.
	 * This constructor depends on both the {@link FileMgr} and
	 * {@link org.vanilladb.core.storage.log.LogMgr LogMgr} objects that it gets
	 * from the class {@link VanillaDb}. Those objects are created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link VanillaDb#initFileAndLogMgr(String)} or is called first.
	 * 
	 * @param numBuffs
	 *            the number of buffer slots to allocate
	 */
	BufferPoolMgr(int numBuffs) {
		bufferPool = new Buffer[numBuffs];
		blockMap = new ConcurrentHashMap<BlockId, Buffer>();
		historyMap = new HashMap<BlockId, LRUHistory>();
		numAvailable = new AtomicInteger(numBuffs);
		lastReplacedBuff = 0;
		timer = 0;
		maxHistory = numBuffs*2;
		buffQueue = new ArrayList<BufferQueue>();
		for (int i = 0; i < numBuffs; i++)
			bufferPool[i] = new Buffer();

		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	// Optimization: Lock striping
	private Object prepareAnchor(Object o) {
		int code = o.hashCode() % anchors.length;
		if (code < 0)
			code += anchors.length;
		return anchors[code];
	}

	/**
	 * Flushes all dirty buffers.
	 */
	void flushAll() {
		for (Buffer buff : bufferPool) {
			try {
				buff.getExternalLock().lock();
				buff.flush();
			} finally {
				buff.getExternalLock().unlock();
			}
		}
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * 
	 * @param txNum
	 *            the transaction's id number
	 */
	void flushAll(long txNum) {
		for (Buffer buff : bufferPool) {
			try {
				buff.getExternalLock().lock();
				if (buff.isModifiedBy(txNum)) {
					buff.flush();
				}
			} finally {
				buff.getExternalLock().unlock();
			}
		}
	}

	/**
	 * Pins a buffer to the specified block. If there is already a buffer
	 * assigned to that block then that buffer is used; otherwise, an unpinned
	 * buffer from the pool is chosen. Returns a null value if there are no
	 * available buffers.
	 * 
	 * @param blk
	 *            a block ID
	 * @return the pinned buffer
	 */
	Buffer pin(BlockId blk) {
		// Only the txs acquiring the same block will be blocked
		synchronized (prepareAnchor(blk)) {
			timer++;
//			if(timer % 1000 == 0) {
//				if (historyMap.size() != 0)	clearHistory();
//			}
			// Find existing buffer
			Buffer buff = findExistingBuffer(blk);

			// If there is no such buffer // 1. buffer還沒滿（剛開機） 2. buffer滿了要swap
			if (buff == null) {
				
				int lastReplacedBuff = this.lastReplacedBuff;
				int currBlk = (lastReplacedBuff + 1) % bufferPool.length;
				// Clock if bufferPool is not full
				if(lastReplacedBuff <= bufferPool.length) {
					while (currBlk != lastReplacedBuff) {
						buff = bufferPool[currBlk];
						
						// Get the lock of buffer if it is free
						if (buff.getExternalLock().tryLock()) {
							try {
								// Check if there is no one use it
								if (!buff.isPinned()) {
									this.lastReplacedBuff++;
									
									// Swap
									BlockId oldBlk = buff.block();
									if (oldBlk != null)
										blockMap.remove(oldBlk);
									buff.assignToBlock(blk);
									blockMap.put(blk, buff);
									if (!buff.isPinned())
										numAvailable.decrementAndGet();
									
									// Pin this buffer
									buff.pin();
									BufferQueue bfq = new BufferQueue(blk, currBlk);
									buffQueue.add(bfq);
									LRUHistory his = new LRUHistory(blk, timer);
									historyMap.put(blk, his);
									return buff;
								}
							} finally {
								// Release the lock of buffer
								buff.getExternalLock().unlock();
							}
						}
						currBlk = (currBlk + 1) % bufferPool.length;
					}
					return null;
				} else { // swap using LRU-K
					return findSwapBuffer(timer, blk, null, null); // LRU-K
				}

				// Choose Unpinned Buffer
				// clock strategy
//				int lastReplacedBuff = this.lastReplacedBuff;
//				int currBlk = (lastReplacedBuff + 1) % bufferPool.length;
//				while (currBlk != lastReplacedBuff) {
//					buff = bufferPool[currBlk];
//					
//					// Get the lock of buffer if it is free
//					if (buff.getExternalLock().tryLock()) {
//						try {
//							// Check if there is no one use it
//							if (!buff.isPinned()) {
//								this.lastReplacedBuff = currBlk;
//								
//								// Swap
//								BlockId oldBlk = buff.block();
//								if (oldBlk != null)
//									blockMap.remove(oldBlk);
//								buff.assignToBlock(blk);
//								blockMap.put(blk, buff);
//								if (!buff.isPinned())
//									numAvailable.decrementAndGet();
//								
//								// Pin this buffer
//								buff.pin();
//								buffQueue.add(blk);
//								return buff;
//							}
//						} finally {
//							// Release the lock of buffer
//							buff.getExternalLock().unlock();
//						}
//					}
//					currBlk = (currBlk + 1) % bufferPool.length;
//				}
//				return null;
				
			// If it exists(no need to do I/O)
			} else {
				
				// update history access times
				
				// Get the lock of buffer
				buff.getExternalLock().lock();
				
				try {
					// Check its block id before pinning since it might be swapped
					if (buff.block().equals(blk)) {
						if (!buff.isPinned())
							numAvailable.decrementAndGet();
						
						buff.pin();
						if(historyMap.containsKey(blk)) {  // access before
							historyMap.get(blk).accessAgain(timer, LRU_K);
						} else {  // never access
							System.out.println("historMap should include the block!!!!");
						}
						return buff;
					}
					return pin(blk);
					
				} finally {
					// Release the lock of buffer
					buff.getExternalLock().unlock();
				}
			}
		}
	}
	Buffer findSwapBuffer(long time, BlockId newBlk, String fileName, PageFormatter fmtr) {
		HashMap<Integer, Long> validBuff = new HashMap<Integer, Long>();
		LinkedHashMap<Integer, Long> sortedMap = new LinkedHashMap<>();
		List<Integer> tracingBuffOrder = new ArrayList<>();
		int findIndex = -1;
		long maxdis = 0;
		for(BufferQueue buffq: buffQueue) {
			// 1. put new blk to buffQueue 2. history need to update 3. return bufferPool's  buff
			BlockId blk = buffq.getBlockId();
			int index = buffq.getIndex(); // BufferPool index
			if(historyMap.containsKey(blk)) {
				if(historyMap.get(blk).getLastKpinTime(LRU_K) < 0) { // inf(return)
					findIndex = index;
					Buffer buf = bufferPool[findIndex];
					// Get the lock of buffer if it is free
					if (buf.getExternalLock().tryLock()) {
						try {
							// Check if there is no one use it
							if (!buf.isPinned()) {
								
								// Swap
								BlockId oldBlk = buf.block();
								if (oldBlk != null)
									blockMap.remove(oldBlk);
								if(newBlk == null)
									buf.assignToNew(fileName, fmtr);
								else
									buf.assignToBlock(newBlk);
								blockMap.put(buf.block(), buf);
								if (!buf.isPinned())
									numAvailable.decrementAndGet();
								
								// Pin this buffer
								buf.pin();
								buffq.setBlockId(buf.block());
								if(historyMap.containsKey(buf.block())) {  // access before
									historyMap.get(buf.block()).accessAgain(timer, LRU_K);
								} else {  // never access
									LRUHistory his = new LRUHistory(buf.block(), timer);
									historyMap.put(buf.block(), his);
								}
								return buf;
							}
						} finally {
							// Release the lock of buffer
							buf.getExternalLock().unlock();
						}
					}
				} else { // not inf
					validBuff.put(index, historyMap.get(blk).getLastKpinTime(LRU_K));
					if(historyMap.get(blk).getLastKpinTime(LRU_K) > maxdis) { // update blk to be swapped
						findIndex = index;
						maxdis = historyMap.get(blk).getLastKpinTime(LRU_K);
					}
				}
			}
			else { // blk never been accessed
				System.out.println("historMap should include the block!!!!");
			}
		}
		// find the maxdis to replace if there is no inf
		if (findIndex < 0) System.out.println("findIndex should > 0!!!!!!!!!!");
		// sort validBuff by historyMap.get(blk).getLastKpinTime(LRU_K) -> to tracingBuffOrder
		validBuff.entrySet().stream().sorted(Map.Entry.comparingByValue()).forEachOrdered(x -> tracingBuffOrder.add(x.getKey()));
		for(int buffindex: tracingBuffOrder) {
			Buffer buf = bufferPool[buffindex];
			// Get the lock of buffer if it is free
			if (buf.getExternalLock().tryLock()) {
				try {
					// Check if there is no one use it
					if (!buf.isPinned()) {
						
						// Swap
						BlockId oldBlk = buf.block();
						if (oldBlk != null)
							blockMap.remove(oldBlk);
						if(newBlk == null)
							buf.assignToNew(fileName, fmtr);
						else
							buf.assignToBlock(newBlk);
						blockMap.put(buf.block(), buf);
						if (!buf.isPinned())
							numAvailable.decrementAndGet();
						
						// Pin this buffer
						buf.pin();
						for(BufferQueue buffq: buffQueue) {
							if(buffq.getIndex() == buffindex) {
								buffq.setBlockId(buf.block());
							}
						}
						if(historyMap.containsKey(buf.block())) {  // access before
							historyMap.get(buf.block()).accessAgain(timer, LRU_K);
						} else {  // never access
							LRUHistory his = new LRUHistory(buf.block(), timer);
							historyMap.put(buf.block(), his);
						}
						return buf;
					} 
				} finally {
					// Release the lock of buffer
					buf.getExternalLock().unlock();
				}
			}
		}
		return null;
	}
//	void dispalce(BlockId blk, Buffer buff) {
//		if(historyMap.containsKey(blk)) {
//			historyMap.get(blk).setHits();
//			historyMap.get(blk).setTime(timer);
//		}
//		int index = -1;
//		for (int i=0 ; i<bufferPool.length ; i++) {
//			if (bufferPool[i] == buff) {
//				index = i;
//			}
//		}
//		if(index == -1) {
//			System.out.println("No buff in bufferPool!!!!!!");
//		}
//		List<BufferQueue> remove = new ArrayList<BufferQueue>();
//		for (BufferQueue bfq: buffQueue) {
//			if(bfq.getBlockId() == blk) {
//				remove.add(bfq);
//			}
//		}
//		buffQueue.removeAll(remove);
//		BufferQueue bufq = new BufferQueue(blk, index);
//		buffQueue.add(bufq);
//	}
//	Buffer replaceBuff(BlockId blk) {
//		// LRU-K strategy
//		if (buffQueue.size() != bufferPool.length)
//			System.out.println("BufferQueue size is wrong!!!!!!");
//		for(int i = 0 ; i<buffQueue.size() ; i++) {
//			BufferQueue bq = buffQueue.get(i);
////			Buffer buff = blockMap.get(bq.getBlockId());
//			Buffer buff = bufferPool[bq.getIndex()];
//			
//			if (buff.getExternalLock().tryLock()) {
//				try {
//					// Check if there is no one use it
//					if (!buff.isPinned()) {
//						
//						// add bufferQueue in tail
//						BufferQueue bfq = new BufferQueue(blk, bq.getIndex());
//						buffQueue.add(bfq);
//						// Swap
//						BlockId oldBlk = buff.block();
//						if (oldBlk != null) {
//							blockMap.remove(oldBlk);
//							buffQueue.remove(bq); // remove bufferQueue from head which is unpinned
//						}
//						buff.assignToBlock(blk);
//						//add history
//						historyMap.get(blk).setHits();
//						historyMap.get(blk).setTime(timer);
//						
//						blockMap.put(blk, buff);
//						if (!buff.isPinned())
//							numAvailable.decrementAndGet();
//						
//						// Pin this buffer
//						buff.pin();
//						return buff;
//					}
//				} finally {
//					// Release the lock of buffer
//					buff.getExternalLock().unlock();
//				}
//			}
//		}
//		return null;
//	}
//	Buffer addToBuffer(BlockId blk, int lastReplacedBuff) {
//		if (lastReplacedBuff >= bufferPool.length) { // 防呆
//			System.out.println("BufferQueue size should be less than bufferPool!!!!!!");
//		}
//		Buffer buff = bufferPool[lastReplacedBuff];
//		if (buff.getExternalLock().tryLock()) {
//			try {
//				// Check if there is no one use it
//				if (!buff.isPinned()) {
//					// add BufferQueue to tail
//					BufferQueue bfq = new BufferQueue(blk, lastReplacedBuff);
//					buffQueue.add(bfq);
//					// Swap
//					BlockId oldBlk = buff.block();
//					if (oldBlk != null) {
//						blockMap.remove(oldBlk); // 理論上不需要
//					}
//					buff.assignToBlock(blk);
//					//add history
//					historyMap.get(blk).setHits();
//					historyMap.get(blk).setTime(timer);
//					
//					blockMap.put(blk, buff);
//					if (!buff.isPinned())
//						numAvailable.decrementAndGet();
//					
//					// Pin this buffer
//					buff.pin();
//					this.lastReplacedBuff++;
//					return buff;
//				}
//			} finally {
//				// Release the lock of buffer
//				buff.getExternalLock().unlock();
//			}
//		}
//		return null;
//	}
	/**
	 * Allocates a new block in the specified file, and pins a buffer to it.
	 * Returns null (without allocating the block) if there are no available
	 * buffers.
	 * 
	 * @param fileName
	 *            the name of the file
	 * @param fmtr
	 *            a pageformatter object, used to format the new block
	 * @return the pinned buffer
	 */
	Buffer pinNew(String fileName, PageFormatter fmtr) {
		// Only the txs acquiring to append the block on the same file will be blocked
		synchronized (prepareAnchor(fileName)) {
			timer++;
			// Choose Unpinned Buffer
			// LRU-K strategy
			int lastReplacedBuff = this.lastReplacedBuff;
			int currBlk = (lastReplacedBuff + 1) % bufferPool.length;
			// Clock if bufferPool is not full
			if(lastReplacedBuff <= bufferPool.length) {
				while (currBlk != lastReplacedBuff) {
					Buffer buff = bufferPool[currBlk];
					
					// Get the lock of buffer if it is free
					if (buff.getExternalLock().tryLock()) {
						try {
							if (!buff.isPinned()) {
								this.lastReplacedBuff++;
								
								// Swap
								BlockId oldBlk = buff.block();
								if (oldBlk != null)
									blockMap.remove(oldBlk);
								buff.assignToNew(fileName, fmtr);
								blockMap.put(buff.block(), buff);
								if (!buff.isPinned())
									numAvailable.decrementAndGet();
								
								// Pin this buffer
								buff.pin();
								BufferQueue bfq = new BufferQueue(buff.block(), currBlk);
								buffQueue.add(bfq);
								LRUHistory his = new LRUHistory(buff.block(), timer);
								historyMap.put(buff.block(), his);
								return buff;
							}
						} finally {
							// Release the lock of buffer
							buff.getExternalLock().unlock();
						}
					}
					currBlk = (currBlk + 1) % bufferPool.length;
				}
				return null;
			} else { // swap using LRU-K
				return findSwapBuffer(timer, null, fileName, fmtr); // LRU-K
			}
			// clock strategy
//			int lastReplacedBuff = this.lastReplacedBuff;
//			int currBlk = (lastReplacedBuff + 1) % bufferPool.length;
//			while (currBlk != lastReplacedBuff) {
//				Buffer buff = bufferPool[currBlk];
//				
//				// Get the lock of buffer if it is free
//				if (buff.getExternalLock().tryLock()) {
//					try {
//						if (!buff.isPinned()) {
//							this.lastReplacedBuff = currBlk;
//							
//							// Swap
//							BlockId oldBlk = buff.block();
//							if (oldBlk != null)
//								blockMap.remove(oldBlk);
//							buff.assignToNew(fileName, fmtr);
//							blockMap.put(buff.block(), buff);
//							if (!buff.isPinned())
//								numAvailable.decrementAndGet();
//							
//							// Pin this buffer
//							buff.pin();
//							return buff;
//						}
//					} finally {
//						// Release the lock of buffer
//						buff.getExternalLock().unlock();
//					}
//				}
//				currBlk = (currBlk + 1) % bufferPool.length;
//			}
//			return null;
		}
	}

//	void clearHistory() {
//		long timeSum = 0;
//		long timeAvg = 0;
//		long timeSquareSum = 0;
//		double timeStandard = 0;
//		for (Iterator<Entry<BlockId, LRUHistory>> it = historyMap.entrySet().iterator(); it.hasNext();){
//		    Map.Entry<BlockId, LRUHistory> item = it.next();
//		    LRUHistory val = item.getValue();
//		    timeSum += val.getTime();
//		    timeSquareSum += Math.pow(val.getTime(),2);
//		}
//		timeAvg = timeSum / historyMap.size();
//		timeStandard = Math.sqrt(timeSquareSum/historyMap.size() - Math.pow(timeAvg,2));
//		
//		for (Iterator<Entry<BlockId, LRUHistory>> it = historyMap.entrySet().iterator(); it.hasNext();){
//		    Map.Entry<BlockId, LRUHistory> item = it.next();
//		    LRUHistory val = item.getValue();
//		    if(val.getTime() < timeAvg - timeStandard) {
//		    		it.remove();
//		    }
//		}
//	}
	/**
	 * Unpins the specified buffers.
	 * 
	 * @param buffs
	 *            the buffers to be unpinned
	 */
	void unpin(Buffer... buffs) {
		for (Buffer buff : buffs) {
			try {
				// Get the lock of buffer
				buff.getExternalLock().lock();
				buff.unpin();
				if (!buff.isPinned())
					numAvailable.incrementAndGet();
			} finally {
				// Release the lock of buffer
				buff.getExternalLock().unlock();
			}
		}
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * 
	 * @return the number of available buffers
	 */
	int available() {
		return numAvailable.get();
	}

	private Buffer findExistingBuffer(BlockId blk) {
		Buffer buff = blockMap.get(blk);
		if (buff != null && buff.block().equals(blk))
			return buff;
		return null;
	}
}
