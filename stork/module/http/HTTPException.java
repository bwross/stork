package stork.module.http;

class HTTPException extends Exception {

	private static final long serialVersionUID = 1L;

	public HTTPException (String message) {
		super (message);
	}
}
