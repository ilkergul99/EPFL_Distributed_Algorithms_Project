package cs451.CallbackMethods;

import cs451.DataPacket.Message;

/**
 * BroadcastForward interface extends the Forward interface to add a specialized method
 * for uniform message delivery in a distributed system.
 * This interface is typically used in scenarios where it's necessary to ensure that messages
 * are delivered in a uniform manner across different nodes in the system.
 */
public interface BroadcastForward extends Forward {

    /**
     * Delivers a message uniformly across the nodes in the system.
     * This method should be implemented to define the logic for handling a message
     * that needs to be delivered uniformly, ensuring that all nodes in the system
     * process the message in the same way. This could be crucial in maintaining
     * consistency and order of message delivery across the system.
     *
     * @param message The message to be uniformly delivered.
     */
    void broadcastForward(Message message);
}
