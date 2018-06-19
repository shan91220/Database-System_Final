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
	private HashMap<Integer, Long> validBuff;
	private List<Integer> buffOrder;
	private List<BufferQueue> buffQueue;
	private long timer;
	private long timer2;
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
		validBuff = new HashMap<Integer, Long>();
		buffOrder = new ArrayList<Integer>();
		numAvailable = new AtomicInteger(numBuffs);
		lastReplacedBuff = 0;
		timer = 0;
		timer2 = 0;
		maxHistory = numBuffs*2;
		buffQueue = new ArrayList<BufferQueue>();
		for (int i = 0; i < numBuffs; i++) {
			bufferPool[i] = new Buffer();
			validBuff.put(i, (long)0);
		}

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
//				synchronized (historyMap) {
//					if (historyMap.size() != 0)	clearHistory();
//				}
//			}
			// Find existing buffer
			Buffer buff = findExistingBuffer(blk);

			// If there is no such buffer // 1. buffer還沒滿（剛開機） 2. buffer滿了要swap
			if (buff == null) {
				
				buffOrder.clear();
				if (validBuff.size() != bufferPool.length) System.out.println("validBuff.size() wrong!!!!!!!");
				validBuff.entrySet().stream().sorted(Map.Entry.<Integer, Long>comparingByValue()).forEachOrdered(x -> buffOrder.add(x.getKey()));
				if (buffOrder.size() != bufferPool.length) System.out.println("buffOrder.size() wrong!!!!!!!"+buffOrder.size());

				for(Integer index: buffOrder) {
					Buffer b = bufferPool[index];
					if (b.getExternalLock().tryLock()) {
						try {
							// Check if there is no one use it
							if (!b.isPinned()) {
								
								// Swap
								BlockId oldBlk = b.block();
								if (oldBlk != null) {
									blockMap.remove(oldBlk);
									if(historyMap.containsKey(oldBlk)) {
										historyMap.get(oldBlk).setIndex(-1); // swap out of bufferPool
									} else {
										System.out.println("HistoryMap should contain the block!!!!!!!");
									}
								}
								b.assignToBlock(blk);
								if (b.block() != blk)
									System.out.println("blockId wrong!!!!!!!");
								blockMap.put(blk, b);
								if (!b.isPinned())
									numAvailable.decrementAndGet();
								
								// Pin this buffer
//								synchronized(historyMap) {
								b.pin();
								if(historyMap.containsKey(blk)) {  // access before
									historyMap.get(blk).setIndex(index);
									historyMap.get(blk).accessAgain(timer, LRU_K);
									if(validBuff.containsKey(index)) {
										validBuff.replace(index, historyMap.get(blk).getLastKpinTime());
									} else {
										System.out.println("1. validBuff should contain the index!!!!!!!");
									}
								} else {  // never access
									LRUHistory his = new LRUHistory(blk, timer, LRU_K, index);
									historyMap.put(blk, his);
									if(validBuff.containsKey(index)) {
										validBuff.replace(index, historyMap.get(blk).getLastKpinTime());
									} else {
										System.out.println("2. validBuff should contain the index!!!!!!!");
									}
//									his = null;
								}
//								}
								return b;
							}
						} finally {
							// Release the lock of buffer
							b.getExternalLock().unlock();
						}
					}
				}
				return null;
				
			// If it exists(no need to do I/O)
			} else {
				
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
							if(validBuff.containsKey(historyMap.get(blk).getIndex())) {
								validBuff.replace(historyMap.get(blk).getIndex(), historyMap.get(buff.block()).getLastKpinTime());
							} else {
								// check if historyMap.get(blk).getIndex() == -1
								System.out.println("3. validBuff should contain the index!!!!!!!"); 
							}
						} else {  // never access
							System.out.println("1.historMap should include the block!!!!");
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
//			if(timer % 1000 == 0) {
//				synchronized (historyMap) {
//					if (historyMap.size() != 0)	clearHistory();
//				}
//			}
			
			// LRU-K strategy
			buffOrder.clear();
			validBuff.entrySet().stream().sorted(Map.Entry.<Integer, Long>comparingByValue()).forEachOrdered(x -> buffOrder.add(x.getKey()));
			if (buffOrder.size() != bufferPool.length) System.out.println("2. buffOrder.size() wrong!!!!!!!");
			if (validBuff.size() != bufferPool.length) System.out.println("2. validBuff.size() wrong!!!!!!!");
			for(Integer index: buffOrder) {
				Buffer b = bufferPool[index];
				if (b.getExternalLock().tryLock()) {
					try {
						// Check if there is no one use it
						if (!b.isPinned()) {
							
							// Swap
							BlockId oldBlk = b.block();
							if (oldBlk != null) {
								blockMap.remove(oldBlk);
								if(historyMap.containsKey(oldBlk)) {
									historyMap.get(oldBlk).setIndex(-1); // swap out of bufferPool
								} else {
									System.out.println("2. HistoryMap should contain the block!!!!!!!");
								}
							}
							b.assignToNew(fileName, fmtr);
							blockMap.put(b.block(), b);
							if (!b.isPinned())
								numAvailable.decrementAndGet();
							
							// Pin this buffer
//							synchronized(historyMap) {
							b.pin();
							if(historyMap.containsKey(b.block())) {  // access before
								System.out.println("HistoryMap should not access the block before!!!!!!!");
							} else {  // never access
								LRUHistory his = new LRUHistory(b.block(), timer, LRU_K, index);
								historyMap.put(b.block(), his);
								if(validBuff.containsKey(index)) {
									validBuff.replace(index, historyMap.get(b.block()).getLastKpinTime());
								} else {
									System.out.println("4. validBuff should contain the index!!!!!!!");
								}
//								his = null;
							}
//							}
							return b;
						}
					} finally {
						// Release the lock of buffer
						b.getExternalLock().unlock();
					}
				}
			}
			return null;
		}
	}

	void clearHistory() {
//		long timeSum = 0;
//		long timeAvg = 0;
//		long timeSquareSum = 0;
//		double timeStandard = 0;
		HashMap<LRUHistory, Long> sortHistory = new HashMap<LRUHistory, Long>();
		List<LRUHistory> LRUhisOrder = new ArrayList<LRUHistory>();
		for (Iterator<Entry<BlockId, LRUHistory>> it = historyMap.entrySet().iterator(); it.hasNext();){
		    Map.Entry<BlockId, LRUHistory> item = it.next();
		    LRUHistory val = item.getValue();
		    sortHistory.put(val, val.getLastKpinTime());
		}
		sortHistory.entrySet().stream().sorted(Map.Entry.<LRUHistory, Long>comparingByValue()).forEachOrdered(x -> LRUhisOrder.add(x.getKey()));
		if (historyMap.size() > maxHistory) {
			// sort by value
			sortHistory.entrySet().stream().sorted(Map.Entry.<LRUHistory, Long>comparingByValue()).forEachOrdered(x -> LRUhisOrder.add(x.getKey()));
			for (int j = 0; j<historyMap.size()-maxHistory ;j++) {
				BlockId blk_remove = LRUhisOrder.get(j).getBlockId();
				historyMap.remove(blk_remove);
			}
		}
//		sortHistory = null;
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
	}
	/**
	 * Unpins the specified buffers.
	 * 
	 * @param buffs
	 *            the buffers to be unpinned
	 */
	void unpin(Buffer... buffs) {
		for (Buffer buff : buffs) {
			try {
				timer2++;
				// Get the lock of buffer
				buff.getExternalLock().lock();
				buff.unpin();
//				if(historyMap.containsKey(buff.block())) {
//					synchronized (historyMap.get(buff.block())) {
//						historyMap.get(buff.block()).setTime(timer2, LRU_K);
//					}
//				} //else
//					System.out.println("Unpin error!!!!!!!!!!!");
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
