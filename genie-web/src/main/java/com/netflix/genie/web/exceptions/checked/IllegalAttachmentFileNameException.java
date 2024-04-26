package com.netflix.genie.web.exceptions.checked;

/**
 * Exception thrown when the attachment filename is illegal.
 *
 * @author bhou
 */
public class IllegalAttachmentFileNameException extends SaveAttachmentException {
    /**
     * Constructor.
     */
    public IllegalAttachmentFileNameException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param message The detail message
     */
    public IllegalAttachmentFileNameException(final String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message The detail message
     * @param cause   The root cause of this exception
     */
    public IllegalAttachmentFileNameException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     *
     * @param cause The root cause of this exception
     */
    public IllegalAttachmentFileNameException(final Throwable cause) {
        super(cause);
    }
}
