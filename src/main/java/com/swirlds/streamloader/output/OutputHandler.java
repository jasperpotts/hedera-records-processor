package com.swirlds.streamloader.output;


import javax.json.JsonObject;

public interface OutputHandler extends AutoCloseable {
	public void outputTransaction(JsonObject transactionJson);
	public void outputRecordFile(JsonObject recordFileJson);
}
