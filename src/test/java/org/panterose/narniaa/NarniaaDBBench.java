package org.panterose.narniaa;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Random;

import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.FastInput;
import com.esotericsoftware.kryo.io.FastOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NarniaaDBBench {
	private final String location = "F:\\Dev\\data";
	private final long millisInDay = 24 * 60 * 60 * 1000;

	private final ByteBuffer IntBuff = ByteBuffer.allocate(4);
	private final Path path = Paths.get(location, "docs");
	
	private static final Kryo kryo = new Kryo();
	private static final byte[] data = new byte[10];
	private static final Random r = new Random(0);
	{
		r.nextBytes(data);
		kryo.register(Document.class);
	}
	
	@Test
	public void insert() throws IOException, ClassNotFoundException {
		try (NarniaaDB db = new NarniaaDB(path, 1 << 8)) {
			write(0, 5, db);
			read(2, 3, db);
		}
	}

	public void write(int start, int end, NarniaaDB db) throws IOException {
		long nbbytes = 0;
		//byte[] out = new byte[data.length + 2048];
		LocalDate date = LocalDate.of(2015, 3, 1);
		long startTime = System.currentTimeMillis();
		
		System.out.print("writing: ");
		Output ko = new FastOutput(data.length + 512, 1 << 26);
		for (int i = start; i < end; i++) {
			try {
				
				//System.out.print(" " + i);
				Document doc = new Document();
				doc.setId(i);
				doc.setSym((i % 10) + "");
				doc.setTimestamp(epoch(date.plus(i / 1000, ChronoUnit.DAYS)));
				doc.setData(data);
	
				kryo.writeObject(ko, doc);
				ko.flush();
				byte[] ser = ko.toBytes();
				db.put(IntBuff.putInt(i).array(), ser);
				nbbytes += ser.length;
				if (ko.getBuffer().length > data.length * 2) {
					System.out.println("Buffer size :" + ko.getBuffer().length);
				}
				if (i % 1000 == 0) {
					System.out.print(".");
				}
				ko.clear();
				IntBuff.clear();
			} catch (RuntimeException e) {
				System.out.println("error for " + i);
				e.printStackTrace();
				throw e;
			}
		}
		long time = System.currentTimeMillis() - startTime;
		System.out.printf("%n %,d B done at %,d MB /s %n", nbbytes, nbbytes / 1000 / time);

	}
	
	public void read(int start, int end, NarniaaDB db) throws IOException {
		long nbbytes = 0;
		long startTime = System.currentTimeMillis();
		System.out.print("reading: ");
		for (int i = start; i < end; i++) {
			try {
				//System.out.print(" " + i);
				byte[] bytes = db.get(IntBuff.putInt(i).array());
				Input ki = new FastInput(bytes);
				Document doc = kryo.readObject(ki, Document.class);
				nbbytes += bytes.length;
				// oos.reset();
				// baos.reset();

				assertEquals(i, doc.getId());
				
				// assertEquals(start, d.getTimestamp());
				assertEquals((i % 10) + "", doc.getSym());
				assertArrayEquals(data, doc.getData());
				if (i % 1000 == 0) {
					System.out.print(".");
				}
				IntBuff.clear();
			} catch (RuntimeException e) {
				System.out.println("error for " + i);
				e.printStackTrace();
				throw e;
			}
		}
		long time = System.currentTimeMillis() - startTime;
		System.out.printf("%n %,d B done at %,d MB /s %n", nbbytes, nbbytes / 1000 / time);
	}
	
	protected long epoch(LocalDate date) {
		return date.getLong(ChronoField.EPOCH_DAY) * millisInDay;
	}
	
	
}
