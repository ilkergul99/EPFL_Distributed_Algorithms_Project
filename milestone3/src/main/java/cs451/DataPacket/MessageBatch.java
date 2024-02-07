package cs451.DataPacket;

import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The MessageBatch class represents a collection of Message objects. It is used to group
 * multiple Message instances together, which can be useful for batch processing, transmission,
 * or storage in distributed systems and network communications.
 */
public class MessageBatch implements Serializable {
    // A List to hold Message objects. The list is declared as final, meaning that
    // while the contents of the list can change, the list itself cannot be replaced.
    private final List<Message> messages;

    /**
     * Default constructor for MessageBatch. Initializes an empty MessageBatch with no messages.
     * This constructor is useful when you want to create a MessageBatch and add messages to it later.
     */
    public MessageBatch() {
        // Initializing the messages list as a new, empty ArrayList.
        // This setup allows for Messages to be added to the batch subsequently.
        this.messages = new ArrayList<>();
    }

    /**
     * Constructor for MessageBatch that takes an existing list of Message objects.
     * This constructor is useful when you already have a collection of Message objects
     * and you want to create a MessageBatch from them.
     *
     * @param messages A list of Message objects to be included in the new MessageBatch.
     */
    public MessageBatch(List<Message> messages) {
        // Assigning the provided list of messages to the messages field.
        // This approach allows for existing Message collections to be used directly.
        this.messages = messages;
    }

    /**
     * Converts a collection of Message objects into a single byte array. Each Message object
     * is individually serialized into a byte array and then all are concatenated together.
     * This method is useful for preparing a series of Message objects for network transmission
     * or storage, ensuring all individual messages are combined into one continuous byte stream.
     *
     * @param proposalSetSize The size of the proposal set in each Message, required for serialization.
     * @return A byte array containing the serialized form of all Message objects in the collection.
     */
    public byte[] toBytes(int proposalSetSize) {
        // Using ByteArrayOutputStream to handle dynamic byte array construction.
        // This is more efficient than repeatedly creating new arrays and copying data.
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // Iterating through each message in the 'messages' collection.
        for (Message message : messages) {
            // Converting each message to a byte array using its toByteArray method.
            byte[] messageBytes = message.toByteArray(proposalSetSize);

            // Writing the byte array of the message to the ByteArrayOutputStream.
            // The ByteArrayOutputStream automatically expands as needed.
            try {
                outputStream.write(messageBytes);
            } catch (IOException ioException) {
                // Handling IOException, though it's rare in the context of ByteArrayOutputStream.
                ioException.printStackTrace();
            }
        }

        // Converting the ByteArrayOutputStream to a byte array.
        // This byte array represents the concatenated byte arrays of all messages.
        return outputStream.toByteArray();
    }

    /**
     * Constructs a MessageBatch object from a byte array. This method is used to deserialize
     * a collection of Message objects that have been serialized and concatenated into a single byte array.
     * It assumes each Message object has a fixed size in bytes, as determined by the messageSize calculation.
     *
     * @param bytes The byte array representing the serialized Message objects.
     * @param teklifSize The size of the proposal set in each Message, required for deserialization.
     * @return A MessageBatch object containing all the deserialized Message objects.
     */
    public static MessageBatch fromBytes(byte[] bytes, int teklifSize) {
        MessageBatch messageBatch = new MessageBatch();

        // Calculating the size of each serialized Message object.
        // This is based on the fixed sizes of its components plus the variable size of the proposal set.
        int messageSize = 11 + teklifSize * 4;

        // Iterating through the byte array, extracting each serialized Message object.
        for (int i = 0; i < bytes.length; i += messageSize) {
            // Creating a new array to hold the bytes for a single Message.
            byte[] messageBytes = new byte[messageSize];

            // Copying the relevant portion of the byte array into messageBytes.
            System.arraycopy(bytes, i, messageBytes, 0, messageSize);

            // Deserializing the byte array back into a Message object and adding it to the MessageBatch.
            messageBatch.addMessage(Message.fromByteArray(messageBytes, teklifSize));
        }

        // Returning the MessageBatch containing all deserialized Messages.
        return messageBatch;
    }

    public void addMessage(Message message){
        messages.add(message);
    }

    public List<Message> getMessages(){
        return this.messages;
    }

    /**
     * Extracts and returns the IDs of all Message objects in the collection. This method iterates
     * through each Message in the 'messages' collection and collects their IDs in a list.
     *
     * @return A list of integers representing the IDs of each Message in the collection.
     */
    public List<Integer> getMessageIds() {
        // Initialize an ArrayList to hold the message IDs.
        List<Integer> messageIds = new ArrayList<>();

        // Iterate through each message in the 'messages' collection.
        for (Message message : messages) {
            // Add the ID of the current message to the list.
            messageIds.add(message.getId());
        }

        // Return the list of message IDs.
        return messageIds;
    }


    public int size(){
        return messages.size();
    }

    /**
     * Creates and returns a copy of the current MessageBatch object. This method duplicates
     * the list of Message objects contained in the batch, creating a new MessageBatch instance
     * with the same messages.
     *
     * @return A new MessageBatch instance that contains copies of all Message objects in the original batch.
     */
    public MessageBatch copy() {
        // Creating a new ArrayList, initializing it with the messages from the current batch.
        // This creates a shallow copy of the list, where the message objects themselves are not duplicated,
        // but the list container is a new instance.
        List<Message> messagesCopy = new ArrayList<>(messages);

        // Returning a new MessageBatch object containing the copied list of messages.
        // This ensures that modifications to the new batch won't affect the original batch.
        return new MessageBatch(messagesCopy);
    }

}
