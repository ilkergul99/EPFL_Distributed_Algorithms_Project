package cs451.Connection;

/**
 * Interface to observe UDP send events.
 * Implementers of this interface can get notifications when certain actions related to UDP sending are executed.
 */
public interface NetworkObserver {

    /**
     * Callback method to be invoked when a UDP sender successfully sends a message or completes a relevant operation.
     *
     * @param receiverId    The ID of the receiver to whom the message was sent.
     * @param messageId     The ID of the message that was sent or processed.
     */
    void observe(int receiverId, int messageId);
}
