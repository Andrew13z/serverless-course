package com.task05;

import java.util.Map;

public class ApiRequest {

	private int principalId;
	private Map<String, Object> content;

	public int getPrincipalId() {
		return principalId;
	}

	public void setPrincipalId(int principalId) {
		this.principalId = principalId;
	}

	public Map<String, Object> getContent() {
		return content;
	}

	public void setContent(Map<String, Object> content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "ApiRequest{" +
				"principalId=" + principalId +
				", content=" + content +
				'}';
	}
}
