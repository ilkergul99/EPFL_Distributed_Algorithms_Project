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
}
