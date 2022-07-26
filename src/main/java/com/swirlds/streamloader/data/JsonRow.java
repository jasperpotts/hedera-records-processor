package com.swirlds.streamloader.data;

import javax.json.JsonArray;
import javax.json.JsonObject;

public record JsonRow(
		String topicTableName,
		String json
) {
	public JsonRow(final String topicTableName, final JsonObject json) {
		this(topicTableName,json.toString());
	}
	public JsonRow(final String topicTableName, final JsonArray json) {
		this(topicTableName,json.toString());
	}
}
