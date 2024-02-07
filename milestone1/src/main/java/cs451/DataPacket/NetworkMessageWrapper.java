package cs451.DataPacket;

import cs451.Host;

/**
 * Associates a network message with its corresponding host information.
 */
public class NetworkMessageWrapper {

    // The network message that we are adding host information to.
    private final Message networkMessage;
    // The host information, like the sender or receiver details, for the network message.
    private final Host messageHost;

    /**
     * Constructs a MessageExtension with a network message and associated host details.
     *
     * @param networkMessage The network message to associate with host details.
     * @param messageHost    The host details associated with the network message.
     */
    public NetworkMessageWrapper(Message networkMessage, Host messageHost) {
        this.networkMessage = networkMessage;
        this.messageHost = messageHost;
    }

    /**
     * Retrieves the network message part of this MessageExtension.
     *
     * @return The network message associated with this MessageExtension.
     */
    public Message getMessage() {
        return networkMessage;
    }

    /**
     * Retrieves the host details part of this MessageExtension.
     *
     * @return The host details associated with this MessageExtension.
     */
    public Host getHost() {
        return messageHost;
    }

    /**
     * Compares this MessageExtension to another object to determine equality.
     * Equality is based solely on the network message, not the host details.
     *
     * @param otherObject The object to compare with this MessageExtension.
     * @return true if the other object is a MessageExtension with an equivalent network message.
     */
    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) return true; // If they are the same instance, they are inherently equal.
        if (otherObject == null || getClass() != otherObject.getClass()) return false; // If otherObject is null or not the same type, they can't be equal.

        // Cast the Object to a NetworkMessageWrapper to compare the contained messages.
        NetworkMessageWrapper otherNetworkMessageWrapper = (NetworkMessageWrapper) otherObject;

        // Return true only if the contained network messages are equal.
        return this.getMessage().equals(otherNetworkMessageWrapper.getMessage());
    }
}
