package org.panterose.narniaa;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import net.openhft.lang.io.VanillaMappedBlocks;
import net.openhft.lang.io.VanillaMappedBytes;
import net.openhft.lang.io.VanillaMappedFile;
import net.openhft.lang.io.VanillaMappedMode;

public class NarniaaDB implements Closeable {
	
	protected final VanillaMappedFile indexFile;
	protected final VanillaMappedBlocks dataBlocks;
	
	protected final int blockSize;
	
	protected AtomicLong indexMaxOffset = new AtomicLong(0);
	protected AtomicLong valueMaxOffset = new AtomicLong(0);
	protected Map<BeniKey, BeniEntry> entries = new ConcurrentHashMap<>();
	
	public NarniaaDB(Path path, int blockSize) {
		this(path, blockSize, false);
	}
	
	public NarniaaDB(Path path, int blockSize, boolean deleteOnExit) {
		super();
		try {
			File parent = path.toFile().getParentFile();
			String name = path.toFile().getName();
			
			// setup index file
			File idxFile = new File(parent, name + ".vidx");
			if (!idxFile.exists()) idxFile.createNewFile();
			if (deleteOnExit) idxFile.deleteOnExit(); 
			this.indexFile = new VanillaMappedFile(idxFile, VanillaMappedMode.RW);
			
			// setup data file
			File dataFile = new File(parent, name + ".vdb");
			if (!dataFile.exists()) dataFile.createNewFile();
			if (deleteOnExit) dataFile.deleteOnExit();
			this.dataBlocks = VanillaMappedBlocks.readWrite(dataFile, blockSize);
			
			open();
		} catch (IOException e) {
			throw new RuntimeException("Can't create this DB:" + path, e);
		}
		
		this.blockSize = blockSize;
	}
	
	public void open() throws IOException {
		entries.clear();
		indexMaxOffset.set(0);
		try (VanillaMappedBytes index = indexFile.bytes(0, indexFile.size())) {
			while (indexMaxOffset.get() < indexFile.size()) {
				long valueOffset = index.readLong();
				long valueSize = index.readLong();
				int keySize = index.readInt();
				byte[] key = new byte[keySize];
				index.readFully(key);
				if ((valueOffset  + valueSize) > valueMaxOffset.get()) {
					valueMaxOffset.set(valueOffset + valueSize);  
				}
				entries.put(new BeniKey(key), new BeniEntry(valueOffset, valueSize, indexMaxOffset.getAndAdd(entrySize(keySize))));
			}
		}
	}
	
	public void close() {
		entries.clear();
		try {
			indexFile.close();
			dataBlocks.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteOnExit() {
		
	}
	
	public boolean put(byte[] key, byte[] value) throws IOException {
		Objects.requireNonNull(key);
		Objects.requireNonNull(value);
		
		//capture the offset to add the entry
		final int valueSize = value.length;
		final long valueOffset = valueMaxOffset.getAndAdd(valueSize);
		final BeniKey okey = new BeniKey(key);
		
		int blockOffset = (int)(valueOffset % blockSize);
		int blockindex = (int)((valueOffset - blockOffset) / blockSize);
		int written = 0;
		int remaining = valueSize;
		while (written < valueSize) {
			VanillaMappedBytes bytes = dataBlocks.acquire(blockindex);
			bytes.position(blockOffset);
			try {
				int towrite = Math.min((int)bytes.remaining(), (int)(remaining));
				bytes.write(value, written, towrite);
				written += towrite;
				remaining -= towrite;
			} finally {
				bytes.release();
				blockOffset = 0;
				blockindex++;
			}
			
		}
		
		//create or recyle the entry
		BeniEntry entry = entries.get(okey);
		int keySize = key.length;
		int entrySize = entrySize(keySize);
		long keyOffset = entry != null ? entry.getKeyOffset() : indexMaxOffset.getAndAdd(entrySize);
		synchronized (entries) {
			VanillaMappedBytes index = indexFile.bytes(keyOffset, entrySize);
			index.writeLong(valueOffset);
			index.writeLong(valueSize);
			index.writeInt(keySize);
			index.write(key);
			return entries.put(okey, new BeniEntry(valueOffset, valueSize, keyOffset)) != null;
		}
		
	}
	
	/**
	 * @param key: key use for lookup
	 * @return the array of byte of value matching the key, null if the key is not in the map
	 * @throws IOException 
	 */
	public byte[] get(byte[] key) throws IOException {
		BeniEntry entry = entries.get(new BeniKey(key));
		if (entry == null) return null;
		byte[] bytes = new byte[(int)entry.getValueSize()];
				
		int blockOffset = (int)(entry.getValueOffset() % blockSize);
		int blockindex = (int)((entry.getValueOffset() - blockOffset) / blockSize);
		int read = 0;
		int remaining = (int)entry.getValueSize();
		while (read < entry.getValueSize()) {
			VanillaMappedBytes mbytes = dataBlocks.acquire(blockindex++);
			if (read == 0) {
				mbytes.position(blockOffset);
			} else {
				mbytes.position(0);
			}
			try {
				int toread = Math.min((int)mbytes.capacity(), remaining);
				int bread = mbytes.read(bytes, read, toread);
				read += bread;
				remaining -= bread;
			} finally {
				mbytes.release();
			}
		}
		
		return bytes;
	}
	
	public int entrySize(int keySize) {
		return keySize + 4 + 8* 2;
	}
	
	
	
	public static class BeniEntry {
		private final long valueOffset;
		private final long valueSize;
		private final long keyOffset;
		
		public BeniEntry(long valueOffset, long valueSize, long keyOffset) {
			super();
			this.valueOffset = valueOffset;
			this.valueSize = valueSize;
			this.keyOffset = keyOffset;
		}
		
		public long getValueOffset() {
			return valueOffset;
		}
		public long getValueSize() {
			return valueSize;
		}
		public long getKeyOffset() {
			return keyOffset;
		}

		@Override
		public String toString() {
			return "BeniEntry [valueOffset=" + valueOffset + ", valueSize=" + valueSize + ", keyOffset=" + keyOffset + "]";
		}
		
	}
	
	public static class BeniKey {
		private final byte[] key;

		public BeniKey(byte[] key) {
			super();
			this.key = key;
		}

		public byte[] getKey() {
			return key;
		}

		@Override
		public int hashCode() {
			int result = 1;
			for (byte element : key)
	            result = 1021 * result + element;
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			BeniKey other = (BeniKey) obj;
			if (!Arrays.equals(key, other.key))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "BeniKey [key=" + Arrays.toString(key) + "]";
		}
		
		
	}
	
	
}

