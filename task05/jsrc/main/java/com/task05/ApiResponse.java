package com.task05;

public class ApiResponse {

	private int statusCode;
	private Event event;

	public ApiResponse(int statusCode, Event event) {
		this.statusCode = statusCode;
		this.event = event;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public Event getEvent() {
		return event;
	}

	public void setEvent(Event event) {
		this.event = event;
	}
}
