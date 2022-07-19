package com.swirlds.streamloader.output;


import javax.json.JsonObject;

public interface OutputHandler extends AutoCloseable {
	void outputTransaction(JsonObject transactionJson);
	void outputRecordFile(JsonObject recordFileJson);
}
