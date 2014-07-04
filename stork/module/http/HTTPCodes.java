package stork.module.http;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Convenient function for testing http response code
 */
class HTTPCodes {

	public static boolean isMoved(HttpResponseStatus status) {
		if (status.equals(HttpResponseStatus.FOUND) ||
				status.equals(HttpResponseStatus.MOVED_PERMANENTLY) ||
				status.equals(HttpResponseStatus.TEMPORARY_REDIRECT)) {
			return true;
		} else {
			return false;
		}
	}
	
	public static boolean isNotFound(HttpResponseStatus status) {
		if (status.equals(HttpResponseStatus.NOT_FOUND)) {
			return true;
		} else {
			return false;
		}
	}
	
	public static boolean isInvalid(HttpResponseStatus status) {
		if (status.equals(HttpResponseStatus.BAD_REQUEST) ||
				status.equals(HttpResponseStatus.NOT_IMPLEMENTED)) {
			return true;
		} else {
			return false;
		}
	}
	
	public static boolean isOK(HttpResponseStatus status) {
		if (status.equals(HttpResponseStatus.OK)) {
			return true;
		} else{
			return false;
		}
	}
}
