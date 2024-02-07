package cs451.DataPacket;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Represents a message in the system. This class encapsulates the details of a message,
 * including IDs and sender/receiver information. It implements Serializable to allow
 * the message to be easily transmitted over networks or saved to files.
 */
public class Message implements Serializable {
    public static final int BYTE_SIZE = 8; // Constant for byte size, used in serialization/deserialization

    // Attributes of the Message class
    private final int messageId; // Unique identifier for the message
    private final byte senderIdentifier; // ID of the sender of the message
    private final byte receiverIdentifier; // ID of the intended receiver of the message
    private final byte initialSender; // ID of the original sender (useful in forwarding scenarios)
    private final Boolean isAcknowledgement; // Flag to indicate if this message is an acknowledgment

    /**
     * Constructor for creating a new message.
     *
     * @param messageId            Unique identifier for the message.
     * @param senderIdentifier      ID of the sender.
     * @param receiverIdentifier    ID of the receiver.
     * @param initialSender ID of the original sender of the message.
     */
    public Message(int messageId, byte senderIdentifier, byte receiverIdentifier, byte initialSender) {
        this.messageId = messageId;
        this.senderIdentifier = senderIdentifier;
        this.receiverIdentifier = receiverIdentifier;
        this.initialSender = initialSender;
        this.isAcknowledgement = false; // Default ack value is false
    }

    /**
     * Overloaded constructor for creating a new message with an acknowledgment flag.
     *
     * @param messageId            Unique identifier for the message.
     * @param senderIdentifier      ID of the sender.
     * @param receiverIdentifier    ID of the receiver.
     * @param initialSender ID of the original sender of the message.
     * @param isAcknowledgement           Flag indicating if the message is an acknowledgment.
     */
    public Message(int messageId, byte senderIdentifier, byte receiverIdentifier, byte initialSender, Boolean isAcknowledgement) {
        this.messageId = messageId;
        this.senderIdentifier = senderIdentifier;
        this.receiverIdentifier = receiverIdentifier;
        this.initialSender = initialSender;
        this.isAcknowledgement = isAcknowledgement;
    }

    /**
     * Constructor for creating a new message by modifying sender and receiver of an existing message.
     *
     * @param message      The original message to be modified.
     * @param newSender    New sender's ID for the message.
     * @param newReceiver  New receiver's ID for the message.
     */
    public Message(Message message, byte newSender, byte newReceiver) {
        this.messageId = message.getId();
        this.senderIdentifier = newSender;
        this.receiverIdentifier = newReceiver;
        this.initialSender = message.getOriginalSender();
        this.isAcknowledgement = true; // By default, this constructor sets the ack flag to true
    }

    /**
     * Overloaded constructor for creating a new message by modifying sender, receiver,
     * and acknowledgment status of an existing message.
     *
     * @param message      The original message to be modified.
     * @param newSender    New sender's ID for the message.
     * @param newReceiver  New receiver's ID for the message.
     * @param isAcknowledgement          Acknowledgment status for the new message.
     */
    public Message(Message message, byte newSender, byte newReceiver, Boolean isAcknowledgement) {
        this.messageId = message.getId();
        this.senderIdentifier = newSender;
        this.receiverIdentifier = newReceiver;
        this.initialSender = message.getOriginalSender();
        this.isAcknowledgement = isAcknowledgement;
    }

    /**
     * Gets the unique identifier of the message.
     *
     * @return The message ID as an integer.
     */
    public int getId() {
        // return the id of the message
        return messageId;
    }

    /**
     * Gets the ID of the sender of this message.
     *
     * @return The sender ID as a byte.
     */
    public byte getSenderId() {
        return senderIdentifier;
    }

    /**
     * Gets the ID of the intended receiver of this message.
     *
     * @return The receiver ID as a byte.
     */
    public byte getReceiverId() {
        return receiverIdentifier;
    }

    /**
     * Gets the ID of the original sender of the message. This is particularly
     * useful in message forwarding scenarios.
     *
     * @return The original sender ID as a byte.
     */
    public byte getOriginalSender() {
        return initialSender;
    }

    /**
     * Checks if this message is an acknowledgment message.
     *
     * @return True if this message is an acknowledgment, false otherwise.
     */
    public Boolean isAckMessage() {
        return isAcknowledgement;
    }

    /**
     * Swaps the sender and receiver of this message.
     * Useful for creating a response or acknowledgment message.
     *
     * @return A new Message object with swapped sender and receiver.
     */
    public Message createResponseMessage() {
        return new Message(this.messageId, this.receiverIdentifier, this.senderIdentifier, this.initialSender, false);
    }

    /**
     * Creates a copy of this Message.
     * Useful for when a distinct copy of the message is needed.
     *
     * @return A new Message object that is a copy of this message.
     */
    public Message copy() {
        return new Message(this.messageId, this.senderIdentifier, this.receiverIdentifier, this.initialSender, this.isAcknowledgement);
    }

    /**
     * Compares this message to another object for equality. Two messages are considered equal
     * if they have the same ID, original sender, sender ID, receiver ID, and acknowledgment status.
     *
     * @param o The object to compare with this message.
     * @return True if the objects are equal, false otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Message message = (Message) o;

        // Compare all fields for equality
        return messageId == message.getId() &&
                initialSender == message.getOriginalSender() &&
                senderIdentifier == message.getSenderId() &&
                receiverIdentifier == message.getReceiverId() &&
                Objects.equals(isAcknowledgement, message.isAckMessage()); // Use Objects.equals for Boolean comparison
    }

    /**
     * Generates a hash code for this message based on its fields.
     *
     * @return A hash code as an integer.
     */
    @Override
    public int hashCode() {
        // Generate a hash code that includes all relevant fields
        return Objects.hash(messageId, initialSender, senderIdentifier, receiverIdentifier, isAcknowledgement);
    }

    /**
     * Returns a string representation of this Message, which includes all of its properties.
     *
     * @return A string representation of the Message.
     */
    @Override
    public String toString() {
        return String.format(
                "Message{id=%d, senderId=%d, receiverId=%d, originalSender=%d, isAck=%b}",
                messageId, senderIdentifier, receiverIdentifier, initialSender, isAcknowledgement
        );
    }

    /**
     * Converts this Message object into a byte array for serialization.
     * This method serializes the message's ID, senderId, receiverId, originalSender, and ack flag.
     *
     * @return A byte array representing the serialized form of this Message.
     */
    public byte[] toByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(messageId);
        buffer.put(senderIdentifier);
        buffer.put(receiverIdentifier);
        buffer.put(initialSender);
        buffer.put(isAcknowledgement ? (byte) 1 : (byte) 0);
        return buffer.array();
    }

    /**
     * Converts a byte array back into a Message object.
     * This method deserializes the byte array into a Message object.
     *
     * @param bytes The byte array to be deserialized.
     * @return A Message object represented by the byte array.
     */
    public static Message fromByteArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        // Extract fields from the ByteBuffer
        int id = buffer.getInt();
        byte senderId = buffer.get();
        byte receiverId = buffer.get();
        byte originalSender = buffer.get();
        byte ackByte = buffer.get();
        return new Message(id, senderId, receiverId, originalSender, ackByte == 1);
    }

}
