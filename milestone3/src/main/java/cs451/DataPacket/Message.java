package cs451.DataPacket;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * The Message class represents a communication unit used in lattice-based communication processes.
 * It is designed to be serializable so that it can be easily transmitted over networks or stored.
 * Each message contains several key pieces of information that are essential for communication
 * in distributed systems or network protocols.
 */
public class Message implements Serializable {
    private final int messageId; // Unique identifier for the message.
    private final int latticeRound; // The round number in the lattice-based communication or process.
    private final byte senderIdentifier; // Identifier of the entity sending the message.
    private final byte receiverIdentifier; // Identifier of the entity receiving the message.
    private final byte acknowledgementMessageType; // Type of message: 0 for proposal, 1 for ack, 2 for negative_ack, 3 for delivery confirmation, 4 for ack of delivery.
    private Set<Integer> proposals; // Set of integers representing proposals or data associated with the message.
    private final int hashCode; // Hash code of the message object, typically used for hash-based collections.

    /**
     * Constructs a Message with an optional type of acknowledgment.
     *
     * @param uniqueMessageId    The unique identifier for the message.
     * @param currentSenderId    The identifier of the entity sending the message.
     * @param currentRecipientId The identifier of the entity receiving the message.
     * @param roundNumber        The round number in the lattice communication process.
     * @param ackType            (Optional) A byte value representing the type of acknowledgment.
     *                           It can be 0 for a Proposal, 1 for ACK (Acknowledgment), 2 for NACK (Negative Acknowledgment), etc.
     *                           If not provided, defaults to 0 (Proposal type).
     * @param proposedValues     A collection of integer values representing proposed data or actions.
     */
    public Message(int uniqueMessageId, byte currentSenderId, byte currentRecipientId, int roundNumber, Set<Integer> proposedValues, byte... ackType) {
        // Assigning values to class members based on provided parameters.
        this.messageId = uniqueMessageId;
        this.senderIdentifier = currentSenderId;
        this.receiverIdentifier = currentRecipientId;
        this.latticeRound = roundNumber;
        this.proposals = proposedValues;

        // Check if an acknowledgment type is provided; if not, default to 0 (Proposal type).
        this.acknowledgementMessageType = (ackType.length > 0) ? ackType[0] : (byte) 0;

        // Calculating a hash code for the object based on several attributes.
        // This is commonly used for efficient storage and retrieval in data structures like HashMaps.
        this.hashCode = Objects.hash(uniqueMessageId, roundNumber, currentSenderId, currentRecipientId, this.acknowledgementMessageType);
    }

    /**
     * Constructs a Message with a specific acknowledgment type.
     *
     * @param uniqueId The unique identifier for the message.
     * @param currentSenderId The identifier of the message sender.
     * @param currentReceiverId The identifier of the message receiver.
     * @param roundNumber The lattice round number associated with the message.
     * @param ackTypeFlag A boolean to determine the type of acknowledgment message.
     *                    If true, sets type to 4 (ACK for Lattice Round), else to 3 (Lattice Round Delivered).
     */
    public Message(int uniqueId, byte currentSenderId, byte currentReceiverId, int roundNumber, boolean ackTypeFlag) {
        this.messageId = uniqueId; // Assigning the unique message identifier.
        this.senderIdentifier = currentSenderId; // Assigning the identifier of the sender.
        this.receiverIdentifier = currentReceiverId; // Assigning the identifier of the receiver.
        this.latticeRound = roundNumber; // Assigning the lattice round number.
        // Setting the acknowledgement message type based on the ackTypeFlag.
        if (ackTypeFlag) {
            this.acknowledgementMessageType = (byte) 4; // Sets to 4 if ackTypeFlag is true
        } else {
            this.acknowledgementMessageType = (byte) 3; // Sets to 3 if ackTypeFlag is false
        }
        // Calculating and assigning the hash code for the message.
        this.hashCode = Objects.hash(this.messageId, this.latticeRound, this.senderIdentifier, this.receiverIdentifier, this.acknowledgementMessageType);
    }

    /**
     * Constructs a new Message based on an existing message but with a new sender and receiver.
     *
     * @param baseMessage The original message to be duplicated.
     * @param updatedSenderIdentifier The identifier of the new sender.
     * @param updatedReceiverIdentifier The identifier of the new receiver.
     * @param typeOfAcknowledgement The type of acknowledgment (e.g., 1 for ACK, 2 for NACK).
     */
    public Message(Message baseMessage, byte updatedSenderIdentifier, byte updatedReceiverIdentifier, byte typeOfAcknowledgement) {
        this.messageId = baseMessage.getId(); // Copying the message ID from the base message.
        this.senderIdentifier = updatedSenderIdentifier; // Assigning the updated sender's identifier.
        this.receiverIdentifier = updatedReceiverIdentifier; // Assigning the updated receiver's identifier.
        this.latticeRound = baseMessage.getLatticeRound(); // Copying the lattice round from the base message.
        this.acknowledgementMessageType = typeOfAcknowledgement; // Setting the type of acknowledgment.
        this.proposals = baseMessage.copyProposals(); // Creating a copy of the proposals from the base message.
        // Calculating the hash code based on the message's properties.
        this.hashCode = Objects.hash(this.messageId, this.latticeRound, this.senderIdentifier, this.receiverIdentifier, this.acknowledgementMessageType);
    }

    public Set<Integer> getProposals() {
        return proposals;
    }

    private Set<Integer> copyProposals(){
        return new HashSet<>(this.proposals);
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

    public int getLatticeRound() {
        return latticeRound;
    }

    /**
     * Gets the ID of the original sender of the message. This is particularly
     * useful in message forwarding scenarios.
     *
     * @return The original sender ID as a byte.
     */
    public byte getOriginalSender() {
        // Return the original sender ID
        return senderIdentifier;
    }

    public Boolean isAckMessage(){
        return this.acknowledgementMessageType == 1 || this.acknowledgementMessageType == 2;
    }

    public Boolean isDeliveredMessage(){
        return this.acknowledgementMessageType == 3;
    }
    public Boolean isDeliveredAck(){
        return this.acknowledgementMessageType == 4;
    }

    public byte getAcknowledgementMessageType(){
        return this.acknowledgementMessageType;
    }

    @Override
    public boolean equals(Object o) {
        // Check if the current instance is the same as the object being compared with.
        if (this == o) return true;

        // Check if the object is null or if the classes of the objects are different.
        // This ensures type safety.
        if (o == null || getClass() != o.getClass()) return false;

        // Cast the object to a Message type since we now know o is a non-null Message instance.
        Message message = (Message) o;

        // Compare all relevant fields for equality.
        // Fields are compared in an order that is likely to fail fast if the objects are not equal.
        return messageId == message.getId()
                && latticeRound == message.getLatticeRound()
                && senderIdentifier == message.getSenderId()
                && receiverIdentifier == message.getReceiverId()
                && acknowledgementMessageType == message.getAcknowledgementMessageType();
    }

    @Override
    public int hashCode() {
        // Returns the pre-calculated hash code. This approach is efficient for immutable objects.
        return this.hashCode;
    }

    @Override
    public String toString() {
        // enhances readability and maintainability of the code.
        return String.format(
                // Format string with placeholders: %d for integers, %s for the Set<Integer>.
                "Message{messageId=%d, senderId=%d, receiverId=%d, latticeRound=%d, ackType=%d, proposals=%s}",
                // messageId: Unique identifier of the message, displayed as an integer.
                messageId,
                // senderIdentifier: Identifier for the sender of the message, displayed as an integer.
                senderIdentifier,
                // receiverIdentifier: Identifier for the receiver of the message, displayed as an integer.
                receiverIdentifier,
                // latticeRound: The lattice round number associated with the message, displayed as an integer.
                latticeRound,
                // acknowledgementMessageType: Represents the type of acknowledgment, displayed as an integer.
                // Different values indicate different types of messages, like ACK, NACK, etc.
                acknowledgementMessageType,
                // proposals: A set of integers representing proposals or data associated with the message.
                proposals
        );
    }

    /**
     * Converts the Message object's data into a byte array. This method is useful for
     * serializing the Message object for network transmission or storage. The byte array
     * includes the messageId, latticeRound, senderIdentifier, receiverIdentifier,
     * acknowledgementMessageType, and all proposals (if any). The byte order is set to BIG_ENDIAN
     * to ensure consistent interpretation across different systems.
     *
     * @param proposalSetSize The expected number of proposals to include in the serialization.
     * @return A byte array representing the serialized form of the Message object.
     */
    public byte[] toByteArray(int proposalSetSize) {
        // Allocating a ByteBuffer with enough space for all the fields and the proposals.
        // 4 bytes each for messageId and latticeRound, 1 byte each for senderIdentifier,
        // receiverIdentifier, and acknowledgementMessageType. Plus, 4 bytes per proposal.
        ByteBuffer messageBufferforProposals = ByteBuffer.allocate(4 + 4 + 1 + 1 + 1 + proposalSetSize * 4);

        // Setting the byte order to BIG_ENDIAN for consistent network transmission.
        messageBufferforProposals.order(ByteOrder.BIG_ENDIAN);

        // Encoding the messageId and latticeRound as integers (4 bytes each).
        messageBufferforProposals.putInt(messageId);
        messageBufferforProposals.putInt(latticeRound);

        // Encoding the senderIdentifier, receiverIdentifier, and acknowledgementMessageType as bytes.
        messageBufferforProposals.put(senderIdentifier);
        messageBufferforProposals.put(receiverIdentifier);
        messageBufferforProposals.put(acknowledgementMessageType);

        // Adding proposals to the messageBufferforProposals if the set is not null.
        if (proposals != null) {
            for (Integer proposal : proposals) {
                // Each proposal is encoded as an integer (4 bytes).
                messageBufferforProposals.putInt(proposal);
            }
        }

        // Returning the byte array representation of the messageBufferforProposals.
        return messageBufferforProposals.array();
    }

    /**
     * Converts a byte array back into a Message object. This static method is used to deserialize
     * a Message object that has been previously serialized into a byte array, typically for
     * network transmission or storage purposes. The method reconstructs the Message object using
     * the provided byte array and the expected size of the proposal set.
     *
     * @param bytes The byte array representing the serialized Message object.
     * @param teklifSize The expected number of proposals contained within the serialized data.
     * @return A Message object reconstructed from the byte array.
     */
    public static Message fromByteArray(byte[] bytes, int teklifSize) {
        // Wrapping the byte array in a ByteBuffer to facilitate reading structured data.
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // Setting the byte order to BIG_ENDIAN to ensure consistent interpretation of multi-byte values.
        buffer.order(ByteOrder.BIG_ENDIAN);

        // Reading the first 4 bytes as an integer to get the message ID.
        int id = buffer.getInt();

        // Reading the next 4 bytes as an integer to get the lattice round number.
        int latticeRound = buffer.getInt();

        // Reading the next byte to get the sender's identifier.
        byte senderId = buffer.get();

        // Reading the following byte to get the receiver's identifier.
        byte receiverId = buffer.get();

        // Reading the next byte to get the acknowledgement type.
        byte acknowledgementType = buffer.get();

        // Initializing a set to hold the proposal values.
        Set<Integer> proposals = new HashSet<>();

        // Looping through the remaining bytes to read proposal values, based on the specified set size.
        for (int i = 0; i < teklifSize; i++) {
            // Reading the next 4 bytes as an integer for each proposal value.
            int value = buffer.getInt();

            // Adding the value to the proposals set if it's not zero.
            if (value != 0) {
                proposals.add(value);
            }
        }

        // Creating and returning a new Message object with the extracted data.
        return new Message(id, senderId, receiverId, latticeRound, proposals, acknowledgementType);
    }

    /**
     * Creates a new Message with swapped sender and receiver identifiers. If the original message
     * is an acknowledgment message, the acknowledgment type is set to 0 (proposal). Otherwise, it's set
     * to 3 (indicating delivery of a lattice round).
     *
     * @return A new Message instance with sender and receiver identifiers swapped.
     */
    public Message createResponseMessage() {
        // Determine the new acknowledgement type: if the original is an ack message,
        // set it to 0 (proposal), otherwise set it to 3 (delivery of lattice round).
        var confirmedAcknowledgement = this.isAckMessage() ? (byte) 0 : (byte) 3;

        // Create a new Message with sender and receiver identifiers swapped,
        // and the rest of the properties copied from the current object.
        return new Message(this.messageId, this.receiverIdentifier, this.senderIdentifier,
                this.latticeRound, this.getProposals(), confirmedAcknowledgement);
    }


    /**
     * Creates a copy of the current Message object. This method duplicates all fields
     * of the existing Message into a new instance, ensuring that the new instance is
     * a distinct object but with identical properties.
     *
     * @return A new Message instance that is a copy of the current Message.
     */
    public Message copy() {
        // Create a new Message with all fields duplicated from the current object.
        // The proposals set is also copied to ensure it's a distinct object.
        return new Message(this.messageId, this.senderIdentifier, this.receiverIdentifier,
                this.latticeRound, this.copyProposals(), this.acknowledgementMessageType);
    }

}
