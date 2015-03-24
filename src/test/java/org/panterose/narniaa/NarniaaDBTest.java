package org.panterose.narniaa;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import net.openhft.lang.io.Bytes;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.panterose.narniaa.NarniaaDB.BeniEntry;
import org.panterose.narniaa.NarniaaDB.BeniKey;


public class NarniaaDBTest {

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	private final byte[] key1 = new byte[]{0,0};
	private final byte[] key2 = new byte[]{0,1};
	private final byte[] key3 = new byte[]{0,2};
	
	private final byte[] val1 = new byte[]{0,1,3};
	private final byte[] val2 = new byte[]{0,2,4,8};
	private final byte[] val3 = new byte[]{0,1,2,3,5};
	
	@Test
	public void insert() throws IOException {
		File test = folder.newFile("insert");
		try (NarniaaDB db = new NarniaaDB(test.toPath(), 1024, true)) {
			//db is empty
			assertEquals(0, db.entries.size());
			
			// inserting one entry
			db.put(key1, val1);
			assertEquals(1, db.entries.size());
			assertEquals(2 + 4 + 8 * 2, db.entrySize(2));
			BeniKey key = db.entries.keySet().iterator().next();
			assertArrayEquals(key1, key.getKey()); 
			BeniEntry entry = db.entries.values().iterator().next();
			assertEquals(0, entry.getKeyOffset());
			assertEquals(0, entry.getValueOffset());
			assertEquals(3, entry.getValueSize());
			assertEquals(3, db.valueMaxOffset.get());
			
			//check inside the dataFile
			Bytes bd = db.dataBlocks.acquire(0).bytes(0, 3);
			assertEquals(bd.readByte(), val1[0]);
			assertEquals(bd.readByte(), val1[1]);
			assertEquals(bd.readByte(), val1[2]);
			assertEquals(db.entrySize(2), db.indexMaxOffset.get());
			//check inside the indexFile
			assertEquals(db.entrySize(2), db.indexFile.size());
			Bytes bi = db.indexFile.bytes(0, db.entrySize(2));
			assertEquals(0, bi.readLong());
			assertEquals(3, bi.readLong());
			assertEquals(2, bi.readInt());
			assertEquals(key1[0], bi.readByte());
			assertEquals(key1[1], bi.readByte());
			
			// inserting another entry
			db.put(key2, val2);
			assertEquals(2, db.entries.size());
			key = db.entries.keySet().iterator().next();
			assertArrayEquals(key2, key.getKey()); 
			entry = db.entries.values().iterator().next();
			assertEquals(db.entrySize(2), entry.getKeyOffset());
			assertEquals(3, entry.getValueOffset());
			assertEquals(4, entry.getValueSize());
			assertEquals(7, db.valueMaxOffset.get());
			assertEquals(db.entrySize(2) + db.entrySize(2), db.indexMaxOffset.get());
			
			//check inside the dataFile
			bd = db.dataBlocks.acquire(0).bytes(3, 4);
			assertEquals(val2[0], bd.readByte());
			assertEquals(val2[1], bd.readByte());
			assertEquals(val2[2], bd.readByte());
			assertEquals(val2[3], bd.readByte());
			
			//check inside the indexFile
			bi = db.indexFile.bytes(db.entrySize(2), 2 * db.entrySize(2));
			assertEquals(3, bi.readLong());
			assertEquals(4, bi.readLong());
			assertEquals(2, bi.readInt());
			assertEquals(key2[0], bi.readByte());
			assertEquals(key2[1], bi.readByte());
		}
	}
	
	@Test
	public void read() throws IOException {
		File test = folder.newFile("read");
		try (NarniaaDB db = new NarniaaDB(test.toPath(), 1024, true)) {
			//db is empty
			assertEquals(0, db.entries.size());
			
			// inserting and reading 1 entries
			db.put(key1, val1);
			assertArrayEquals(val1, db.get(key1));
			
			// insert and reading another entry
			db.put(key2, val2);
			assertArrayEquals(val2, db.get(key2));
			
			//checking previous entry
			assertArrayEquals(val1, db.get(key1));
		}
	}
	
	@Test
	public void reopening() throws IOException {
		File test = folder.newFile("reopening");
		try (NarniaaDB db = new NarniaaDB(test.toPath(), 1024, true)) {
			db.put(key1, val1);
			db.put(key2, val2);
		}
		
		try (NarniaaDB db = new NarniaaDB(test.toPath(), 1024, true)) {
			//only check entries
			assertArrayEquals(val1, db.get(key1));
			assertArrayEquals(val2, db.get(key2));
		}
		
		try (NarniaaDB db = new NarniaaDB(test.toPath(), 1024, true)) {
			//add a third entry after that.
			db.put(key3, val3);
			assertEquals(3, db.entries.size());
			BeniKey key = db.entries.keySet().iterator().next();
			assertArrayEquals(key3, key.getKey()); 
			BeniEntry entry = db.entries.values().iterator().next();
			assertEquals(db.entrySize(2) * 2, entry.getKeyOffset());
			assertEquals(7, entry.getValueOffset());
			assertEquals(5, entry.getValueSize());
			assertEquals(12, db.valueMaxOffset.get());
		}
	}
	
	@Test
	public void update() throws IOException {
		File test = folder.newFile("update");
		try (NarniaaDB db = new NarniaaDB(test.toPath(), 1024, true)) {
			db.put(key1, val1);
			db.put(key2, val2);
			assertEquals(2, db.entries.size());
			db.put(key1, val3);
			assertEquals(2, db.entries.size());
			assertArrayEquals(val3, db.get(key1));
			BeniEntry entry = db.entries.get(new BeniKey(key1));
			assertEquals(7, entry.getValueOffset());
			assertEquals(5, entry.getValueSize());
			assertEquals(12, db.valueMaxOffset.get());
		}
		try (NarniaaDB db = new NarniaaDB(test.toPath(), 1024, true)) {
			assertEquals(2, db.entries.size());
			assertArrayEquals(val3, db.get(key1));
		}
	}
	
	@Test
	public void accrossblocks() throws IOException {
		File test = folder.newFile("blocks");
		try (NarniaaDB db = new NarniaaDB(test.toPath(), 1024, true)) {
			byte[] bytes = new byte[1040];
			bytes[0] = 1;
			bytes[1023] = 24;
			bytes[1039] = 40;
			db.put(key1, bytes);
			assertEquals(1, db.entries.size());
			
			//check data on first block
			Bytes bd = db.dataBlocks.acquire(0).bytes();
			assertEquals(1024, bd.size());
			assertEquals(1, bd.readByte(0));
			assertEquals(24, bd.readByte(1023));
			bd.release();
			
			//check data on second block
			bd = db.dataBlocks.acquire(1).bytes();
			assertEquals(1024, bd.size());
			assertEquals(40, bd.readByte(1040 - 1024 - 1));
			bd.release();
			
			// now doing update
			bytes[1039] = 41;
			db.put(key1, bytes);
			BeniEntry entry = db.entries.get(new BeniKey(key1));
			assertEquals(1040, entry.getValueOffset());
			assertEquals(1040, entry.getValueSize());
			assertEquals(1040 * 2, db.valueMaxOffset.get());
			Bytes bi = db.indexFile.bytes(0, db.entrySize(2));
			assertEquals(1040, bi.readLong());
			assertEquals(1040, bi.readLong());
			assertEquals(2, bi.readInt());
			assertEquals(key1[0], bi.readByte());
			assertEquals(key1[1], bi.readByte());
			
			//old data still there
			bd = db.dataBlocks.acquire(1).bytes();
			assertEquals(40, bd.readByte(1040 - 1024 - 1));
			assertEquals(1, bd.readByte(1040 - 1024));
			//check new data
			bd = db.dataBlocks.acquire(2).bytes();
			assertEquals(41, bd.readByte(2 * (1040 - 1024) - 1));
			
		}
	}
}
