package com.yuan.exception;

public class ParameterException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Integer errorCode;
	
	private String message;
	
	
	
	public Integer getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(Integer errorCode) {
		this.errorCode = errorCode;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public ParameterException(String message) {
		super(message);
	}
	
	public ParameterException(String message,Throwable throwable) {
		super(message,throwable);
	}
	
	public ParameterException(String message,Throwable throwable,Integer errorCode){
		this(message,throwable);
		this.setErrorCode(errorCode);
	}
}
