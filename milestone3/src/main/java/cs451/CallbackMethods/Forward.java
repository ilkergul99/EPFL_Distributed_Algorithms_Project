package cs451.CallbackMethods;

import cs451.DataPacket.Message;

/**
 * Forward interface for delivering messages within a distributed system.
 * This interface provides a method for handling received messages, which can be implemented
 * by various classes to define how messages should be processed upon arrival.
 */
public interface Forward {

    /**
     * Delivers a received message.
     * Implementations of this method should define the logic for processing or handling a message
     * after it has been received. This could involve passing the message to the application layer,
     * triggering a response, logging the message, etc.
     *
     * @param message The message to be Forwarded.
     */
    void forward(Message message);

    /**
     * Retrieves the maximum lattice round value.
     * This method is intended to return the highest round number of the lattice-based communication
     * process. Implementations should define how this value is determined, which could be based on
     * system configuration, dynamic network conditions, or other operational parameters. The returned
     * value plays a crucial role in managing the timing or sequencing of message forwarding within the
     * lattice framework.
     *
     * @return The maximum round number used in the lattice communication process.
     */
    public int getMaxLatticeRound();
}
