package cs451.Connection;

import cs451.DataPacket.Message;

/**
 * Interface defining the observer for UDP-related events.
 * This interface is part of the Observer design pattern, allowing objects to be notified
 * of events occurring in UDP communications, particularly in the context of sending messages.
 */
public interface NetworkObserver {

    /**
     * Callback method to be invoked when a Network sender has successfully executed a send operation.
     * Implementers of this interface can use this method to perform actions or log information
     * once a single message has been sent via UDP.
     *
     * @param message The message that was sent. This provides context about the send operation.
     */
    void onSingleMessageTransmissionComplete(Message message);

    /**
     * Callback method to be invoked when a Network send operation has been executed.
     * This method is useful for scenarios where multiple messages are sent in a batch
     * and the implementer needs to be notified after the entire batch has been processed.
     * Unlike onSingleMessageTransmissionComplete, this method does not provide individual message context,
     * implying that it is meant for more general notifications post-batch operations.
     */
    void onMessageBatchTransmissionComplete();
}
