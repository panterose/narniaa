package org.panterose.narniaa;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class Document implements Serializable, Externalizable {
	private static final long serialVersionUID = -3692308904714934271L;
	
	private int id;
	private long timestamp;
	private String sym;
	private byte[] data;
	
	public Document() {
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
	public ByteBuffer getBinary() {
		return ByteBuffer.wrap(data);
	}

	public void setBinary(ByteBuffer buf) {
		this.data = buf.array();
	}

	public String getSym() {
		return sym;
	}

	public void setSym(String sym) {
		this.sym = sym;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
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
		Document other = (Document) obj;
		if (id != other.id)
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(id);
		out.writeLong(timestamp);
		out.writeUTF(sym);
		out.writeInt(data.length);
		out.write(data, 0, data.length);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		this.id = in.readInt();
		this.timestamp = in.readLong();
		this.sym = in.readUTF();
		int size = in.readInt();
		this.data = new byte[size];
		in.readFully(this.data);
	}
}
