package cs451.DataPacket;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This class represents a message that can be sent over a network.
 * It is made Serializable so that Java can convert it into a series of bytes
 * that can be sent over a network or saved to a file.
 */
public class Message implements Serializable {

    // The total number of bytes that this message will be converted to.
    public static final int BYTE_SIZE = 16;

    // Unique identifier for this message.
    private final int messageId;
    // The identifier of the sender of this message.
    private final int senderIdentifier;
    // The identifier of the receiver of this message.
    private final int receiverIdentifier;
    // The identifier of the original sender if this is a forwarded message.
    private final int initialSender;
    // A flag indicating whether this message is an acknowledgement.
    private final boolean isAcknowledgement;

    /**
     * Constructs a new Message with the specified identifiers.
     *
     * @param messageId           The unique identifier for this message.
     * @param senderIdentifier    The identifier of the sender.
     * @param receiverIdentifier  The identifier of the intended receiver.
     * @param initialSender       The identifier of the original sender.
     */
    public Message(int messageId, int senderIdentifier, int receiverIdentifier, int initialSender) {
        this.messageId = messageId;
        this.senderIdentifier = senderIdentifier;
        this.receiverIdentifier = receiverIdentifier;
        this.initialSender = initialSender;
        this.isAcknowledgement = false; // Default to not being an acknowledgement.
    }

    /**
     * Constructs a new Message as an acknowledgement for the given message.
     *
     * @param originalMessage The message to acknowledge.
     * @param newSenderId     The identifier of the sender for this acknowledgement.
     * @param newReceiverId   The identifier of the receiver for this acknowledgement.
     */
    public Message(Message originalMessage, int newSenderId, int newReceiverId){
        this.messageId = originalMessage.getId();
        this.senderIdentifier = newSenderId;
        this.receiverIdentifier = newReceiverId;
        this.initialSender = originalMessage.getOriginalSender();
        this.isAcknowledgement = true; // This constructor is used for creating acknowledgement messages.
    }

    // The following methods are "getters" that provide read-only access to the properties of this class.

    public int getId() {
        return messageId;
    }

    public int getSenderId() {
        return senderIdentifier;
    }

    public int getReceiverId() {
        return receiverIdentifier;
    }

    public int getOriginalSender() {
        return initialSender;
    }

    public boolean isAckMessage(){
        return isAcknowledgement;
    }

    /**
     * Checks if this Message is equal to another Object.
     *
     * @param other The Object to compare with this Message.
     * @return true if the other Object is a Message with the same id and originalSender.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        Message message = (Message) other;
        return messageId == message.getId() && initialSender == message.getOriginalSender();
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
     * Converts this Message object into a byte array.
     *
     * @return A byte array representing this Message.
     */
    public byte[] toByteArray() {
        byte[] bytes = new byte[16];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(messageId);
        buffer.putInt(senderIdentifier);
        buffer.putInt(receiverIdentifier);
        buffer.putInt(initialSender);
        // Note: We are not including the isAcknowledgement field in the byte array.
        return bytes;
    }

    /**
     * Creates a Message object from an array of bytes.
     *
     * @param bytes The byte array to convert to a Message object.
     * @return A new Message object based on the byte array.
     */
    public static Message fromByteArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int messageId = buffer.getInt();
        int senderId = buffer.getInt();
        int receiverId = buffer.getInt();
        int initialSender = buffer.getInt();
        // Note: Since we are not including the isAcknowledgement in the byte array, all messages
        // created with this method will not be acknowledgements.
        return new Message(messageId, senderId, receiverId, initialSender);
    }
}
