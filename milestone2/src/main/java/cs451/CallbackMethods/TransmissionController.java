package cs451.CallbackMethods;

/**
 * TransmissionController interface for handling acknowledgment-related actions in a distributed system.
 * This interface provides methods for managing the sending window and stopping sender processes,
 * typically in the context of reliable message transmission.
 */
public interface TransmissionController {

    /**
     * Instructs the implementation to slide the sending window for a particular destination.
     * This method is usually called in response to receiving acknowledgments, indicating that
     * it's safe to send more messages to the specified destination.
     *
     * @param destinationId The identifier of the destination host for which the sending window should be slid.
     */
    void slideSendWindow(byte destinationId);

    /**
     * Requests the implementation to stop sender processes.
     * This method can be used to halt the sending of messages, typically when shutting down or
     * when it's necessary to pause message transmission.
     */
    void stopSenders();
}
