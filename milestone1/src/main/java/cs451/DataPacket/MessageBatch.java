package cs451.DataPacket;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The MessageBatch class bundles multiple Message objects together for serialization.
 * This is particularly useful for batch processing or transmission of multiple messages.
 */
public class MessageBatch implements Serializable {

    // Holds a batch of messages to be sent or that have been received.
    private List<Message> messageBatch;

    /**
     * Constructor for creating an empty MessagePackage.
     * Use this when you're starting to collect messages to bundle together.
     */
    public MessageBatch() {
        this.messageBatch = new ArrayList<>();
    }

    /**
     * Constructor for creating a MessagePackage with a predefined list of messages.
     *
     * @param messages A list of Message objects to be included in the package.
     */
    public MessageBatch(List<Message> messages) {
        this.messageBatch = new ArrayList<>(messages);
    }

    /**
     * Serializes the batch of messages into a byte array for network transmission.
     *
     * @return A byte array representing all the Message objects in this package.
     */
    public byte[] toBytes() {
        // Initialize an empty array for accumulating the bytes of all messages.
        byte[] packageBytes = new byte[0];

        // Iterate through all messages and convert each to bytes.
        for (Message singleMessage : messageBatch) {
            byte[] messageBytes = singleMessage.toByteArray();
            byte[] combinedBytes = new byte[packageBytes.length + messageBytes.length];

            // Copy current package bytes to the new array.
            System.arraycopy(packageBytes, 0, combinedBytes, 0, packageBytes.length);
            // Copy the single message bytes to the end of the new array.
            System.arraycopy(messageBytes, 0, combinedBytes, packageBytes.length, messageBytes.length);

            // Update the packageBytes to include the new message.
            packageBytes = combinedBytes;
        }
        return packageBytes;
    }

    /**
     * Deserializes a byte array back into a MessagePackage object.
     *
     * @param bytes The byte array to deserialize into Message objects.
     * @return A MessagePackage containing all the deserialized messages.
     */
    public static MessageBatch fromBytes(byte[] bytes) {
        MessageBatch reconstructedPackage = new MessageBatch();
        int currentIndex = 0;

        // Keep extracting messages until we've processed all bytes.
        while (currentIndex < bytes.length) {
            byte[] singleMessageBytes = new byte[Message.BYTE_SIZE];
            // Copy a slice of the array that should correspond to a single message.
            System.arraycopy(bytes, currentIndex, singleMessageBytes, 0, Message.BYTE_SIZE);
            // Convert the bytes to a Message object and add it to our package.
            reconstructedPackage.addMessage(Message.fromByteArray(singleMessageBytes));
            // Move to the next message in the byte array.
            currentIndex += Message.BYTE_SIZE;
        }
        return reconstructedPackage;
    }

    /**
     * Adds a single Message to the message package.
     *
     * @param message The Message object to add to the batch.
     */
    public void addMessage(Message message) {
        messageBatch.add(message);
    }

    /**
     * Retrieves all messages currently in the package.
     *
     * @return A list of Message objects in the package.
     */
    public List<Message> getMessages() {
        return messageBatch;
    }

    /**
     * Extracts and returns a list of the IDs of all messages in the package.
     *
     * @return A list of message IDs.
     */
    public List<Integer> getMessageIds() {
        List<Integer> ids = new ArrayList<>();
        for (Message message : messageBatch) {
            ids.add(message.getId());
        }
        return ids;
    }

    /**
     * Gets the number of messages currently in the message package.
     *
     * @return The size of the message package as an integer.
     */
    public int size() {
        return messageBatch.size();
    }
}
