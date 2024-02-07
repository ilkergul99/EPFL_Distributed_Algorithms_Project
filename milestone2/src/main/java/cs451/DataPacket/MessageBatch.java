package cs451.DataPacket;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;

public class MessageBatch implements Serializable {
    private final List<Message> messageBatch;

    /**
     * Default constructor for creating an empty MessageBatch.
     * Use this when you're starting to collect messages to bundle together.
     */
    public MessageBatch() {
        this.messageBatch = new ArrayList<>();
    }

    /**
     * Constructor for creating a MessageBatch with a list of messages.
     *
     * @param messageBatch The list of Message objects to include in the package.
     */
    public MessageBatch(List<Message> messageBatch) {
        this.messageBatch = messageBatch;
    }

    /**
     * Converts the MessageBatch to a byte array for transmission.
     * This method serializes all messages in the package into a single byte array.
     *
     * @return A byte array representing all the messages serialized.
     */
    public byte[] toBytes() {
        // Calculate the total size of the byte array needed
        int totalSize = messageBatch.size() * Message.BYTE_SIZE;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        // Serialize each message and add it to the ByteBuffer
        for (Message message : messageBatch) {
            buffer.put(message.toByteArray());
        }

        return buffer.array();
    }

    /**
     * Converts a byte array into a MessageBatch.
     * This method deserializes the byte array back into a collection of Message objects.
     *
     * @param bytes The byte array to be deserialized.
     * @return A MessageBatch containing the deserialized messages.
     */
    public static MessageBatch fromBytes(byte[] bytes) {
        MessageBatch messageBatch = new MessageBatch();
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        while (buffer.hasRemaining()) {
            byte[] messageBytes = new byte[Message.BYTE_SIZE];
            buffer.get(messageBytes, 0, Message.BYTE_SIZE);
            messageBatch.addMessage(Message.fromByteArray(messageBytes));
        }

        return messageBatch;
    }

    /**
     * Adds a Message to the batch.
     *
     * @param message The Message to be added.
     */
    public void addMessage(Message message) {
        messageBatch.add(message);
    }

    /**
     * Gets the list of messages in the batch.
     *
     * @return The list of Message objects.
     */
    public List<Message> getMessages() {
        return this.messageBatch;
    }

    /**
     * Retrieves the IDs of all messages in the batch.
     *
     * @return A list of message IDs.
     */
    public List<Integer> getMessageIds() {
        List<Integer> messageIds = new ArrayList<>();
        for (Message message : messageBatch) {
            messageIds.add(message.getId());
        }
        return messageIds;
    }

    /**
     * Creates a copy of the MessageBatch.
     *
     * @return A new MessageBatch containing copies of the messages.
     */
    public MessageBatch copy() {
        List<Message> messagesCopy = new ArrayList<>(messageBatch);
        return new MessageBatch(messagesCopy);
    }

    /**
     * Gets the number of messages in the batch.
     *
     * @return The size of the message list.
     */
    public int size() {
        return messageBatch.size();
    }

}
