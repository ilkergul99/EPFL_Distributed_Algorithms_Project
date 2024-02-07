package cs451;

import cs451.DataPacket.Message;

/**
 * The Forward interface represents a component that is responsible for delivering messages.
 * Implementations of this interface should define how messages are to be processed upon delivery.
 */
public interface Forward {

    /**
     * Delivers a message to its intended recipient or process.
     * Implementing classes should specify the delivery mechanism and the action taken when a message is delivered.
     *
     * @param message The Message object that needs to be delivered.
     */
    void forward(Message message);
}
