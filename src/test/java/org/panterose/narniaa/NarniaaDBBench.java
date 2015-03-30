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
	private static final byte[] data = new byte[1_000_000];
	private static final Random r = new Random(0);
	private static final Document doci = new Document();
	private static final LocalDate date = LocalDate.of(2015, 3, 1);
	private static final byte[] docbytes = new byte[1_000_020];
	{
		r.nextBytes(data);
		doci.setId(0);
		doci.setSym("0");
		doci.setTimestamp(epoch(date));
		doci.setData(data);
		kryo.register(Document.class);
		Output ko = new FastOutput(data.length + 512, 1 << 26);
		ko.setBuffer(docbytes);
		kryo.writeObject(ko, doci);
		ko.flush();
		ko.position();
	}
	
	@Test
	public void insert() throws IOException, ClassNotFoundException {
		try (NarniaaDB db = new NarniaaDB(path, 1 << 26)) {
			write(0, 5000, db);
			read(1000, 3000, db);
		}
	}

	public void write(int start, int end, NarniaaDB db) throws IOException {
		long nbbytes = 0;
		//byte[] out = new byte[data.length + 2048];
		
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
				//db.put(IntBuff.putInt(i).array(), docbytes);
				nbbytes += ser.length;
				//nbbytes += docbytes.length;
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
		long time = System.currentTimeMillis() - startTime + 1;
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
				
				//assertArrayEquals(docbytes, bytes);
				Input ki = new FastInput(bytes);
				Document doc = kryo.readObject(ki, Document.class);
				nbbytes += bytes.length;
				// oos.reset();
				// baos.reset();

				assertEquals(i, doc.getId());
				//assertEquals(0, doc.getId());
				
				// assertEquals(start, d.getTimestamp());
				assertEquals((i % 10) + "", doc.getSym());
				//assertEquals("0", doc.getSym());
				
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
