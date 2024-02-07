package cs451.milestone1;

import cs451.Host;
import cs451.DataPacket.Message;
import cs451.CallbackMethods.TransmissionController;
import cs451.CallbackMethods.Forward;
import cs451.CallbackMethods.BroadcastForward;

import java.util.*;

/**
 * PerfectLinks class is designed to ensure reliable and orderly delivery of messages.
 * It builds upon the StubbornLinks to add guarantees of no duplication and message ordering.
 */
public class PerfectChannel implements Forward {
    private final StubbornChannel stubbornChannel; // Underlying StubbornLinks for ensuring message reliability
    private final BroadcastForward broadcastForwarder; // BroadcastForward interface for uniform message delivery
    private final HashMap<Integer, Set<Byte>>[] deliveredMesssages; // Array of maps to keep track of delivered messages
    private int[] slidingWindowStart; // Array to keep track of the sliding window start for each host
    private final int slidingWindowSize; // Size of the sliding window
    private final HashMap<Byte, Host> hosts; // Mapping of host identifiers to Host objects
    private final byte currentNodeId; // Identifier of the current node

    /**
     * Constructor for the PerfectLinks class.
     *
     * @param port The port number for the underlying StubbornLinks.
     * @param currentNodeId The identifier of the current node.
     * @param broadcastForwarder The broadcastForwarder interface for message delivery.
     * @param hosts A map of host identifiers to Host objects.
     * @param slidingWindowSize The size of the sliding window for message transmission.
     * @param transmissionController The TransmissionController for confirming message deliveries.
     * @param messageCount The total number of messages to be handled.
     */
    public PerfectChannel(int port,
                          byte currentNodeId,
                          BroadcastForward broadcastForwarder,
                          HashMap<Byte, Host> hosts,
                          int slidingWindowSize,
                          TransmissionController transmissionController,
                          int messageCount) {
        // Initialize StubbornLinks for reliable message transmission
        this.stubbornChannel = new StubbornChannel(port, hosts, this, hosts.size(), slidingWindowSize, transmissionController, messageCount);
        this.broadcastForwarder = broadcastForwarder; // Set the uniform broadcastForwarder
        this.currentNodeId = currentNodeId; // Set the current node's ID
        this.slidingWindowSize = slidingWindowSize; // Set the sliding window size
        this.hosts = hosts; // Set the hosts map

        // Initialize the delivered map array and sliding window start array for each host
        deliveredMesssages = new HashMap[hosts.size()];
        this.slidingWindowStart = new int[hosts.size()];
        for (int i = 0; i < hosts.size(); i++) {
            this.slidingWindowStart[i] = 0; // Initialize sliding window start
            this.deliveredMesssages[i] = new HashMap<>(); // Initialize delivered map for each host
        }
    }

    /**
     * Sends a message to a specified host. This method delegates the sending operation
     * to the underlying StubbornLinks instance, which ensures reliable transmission.
     *
     * @param message The message to be sent.
     * @param host The destination host for the message.
     */
    public void send(Message message, Host host) {
        // Delegates the message sending to StubbornLinks
        stubbornChannel.send(message, host);
    }

    /**
     * Stops the PerfectLinks operation, including the underlying StubbornLinks.
     * This is typically used for shutting down the link's operations cleanly.
     */
    public void stop() {
        // Delegates the stop operation to StubbornLinks
        stubbornChannel.stop();
    }

    /**
     * Stops the message sending operations of StubbornLinks.
     * This method is useful for controlling the message sending operations without
     * completely stopping the entire PerfectLinks instance.
     */
    public void stopSenders() {
        // Delegates the stop sender operation to StubbornLinks
        stubbornChannel.stopSenders();
    }

    /**
     * Starts the PerfectLinks operation, including the underlying StubbornLinks.
     * This is used to initiate the link's operations, making it ready to send and receive messages.
     */
    public void start() {
        // Delegates the start operation to StubbornLinks
        stubbornChannel.start();
    }

    @Override
    public void forward(Message message) {
        // Check if the message has already been delivered by this process
        acknowledgeIfDelivered(message);

        // Check if the message ID is within the sliding window range
        if (message.getId() > slidingWindowStart[message.getOriginalSender()] &&
                message.getId() <= slidingWindowStart[message.getOriginalSender()] + slidingWindowSize) {

            // Send an acknowledgment message back to the sender
            send(new Message(message, message.getReceiverId(), message.getSenderId()), hosts.get(message.getSenderId()));

            // Create a set for this message ID in the delivered map if it doesn't exist
            deliveredMesssages[message.getOriginalSender()].computeIfAbsent(message.getId(), k -> new HashSet<>());

            // Add the sender ID to the set of hosts that have seen this message
            if (deliveredMesssages[message.getOriginalSender()].get(message.getId()).add(message.getSenderId())) {
                // Check if it's the first time this message is being delivered
                deliverIfFirstTime(message);

                // Check if the message has been seen by a majority of the hosts
                deliverUniformlyIfMajoritySeen(message);

                // Check if you can update and clean the sliding window
                updateAndCleanSlidingWindow(message.getOriginalSender());
            }
        }
    }

    /**
     * Checks if the message has already been delivered and sends an acknowledgment if so.
     * This method verifies if the ID of the received message is less than or equal to
     * the start of the sliding window for the original sender. If the message has already
     * been delivered, it sends an acknowledgment back to the sender to inform them of this,
     * but does not re-forward the message. This helps in maintaining the reliability of
     * message delivery without unnecessary repetitions.
     *
     * @param message The message to check and acknowledge.
     */
    private void acknowledgeIfDelivered(Message message) {
        // Check if the message has already been delivered by this process
        if (message.getId() <= slidingWindowStart[message.getOriginalSender()]) {
            // Inform the sender that the message has been delivered but don't forward it again.
            // Sending back an acknowledgment message
            send(new Message(message, message.getReceiverId(), message.getSenderId()), hosts.get(message.getSenderId()));
        }
    }

    /**
     * Delivers the message if it's being received for the first time.
     * This method checks if the size of the set of hosts that have seen the message is one,
     * indicating it's the first time this message is being received. If it is, the method then
     * checks if the original sender of the message is not the current host. If both conditions are
     * true, the message is delivered. Finally, it adds the ID of the current host to the set of hosts
     * that have received this message, indicating that the message has been seen by this host.
     *
     * @param message The message to be potentially delivered.
     */
    private void deliverIfFirstTime(Message message) {
        if (deliveredMesssages[message.getOriginalSender()].get(message.getId()).size() == 1) {
            // Deliver the message if this is the first time it's being received
            if (message.getOriginalSender() != currentNodeId) {
                broadcastForwarder.forward(message);
            }
            // Add this host's ID to the set of hosts that have the message
            deliveredMesssages[message.getOriginalSender()].get(message.getId()).add(currentNodeId);
        }
    }

    /**
     * Delivers the message uniformly if it has been seen by a majority of the hosts.
     * This method checks if the number of hosts that have seen the message exceeds half of the total number of hosts.
     * If the condition is true, it indicates that a majority of hosts have received the message, and the method then
     * triggers uniform delivery of the message to ensure all hosts are synchronized with this information.
     *
     * @param message The message to be checked and potentially delivered uniformly.
     */
    private void deliverUniformlyIfMajoritySeen(Message message) {
        // Check if the message has been seen by a majority of the hosts
        if (deliveredMesssages[message.getOriginalSender()].get(message.getId()).size() > (hosts.size() / 2)) {
            // Deliver the message uniformly if it has been seen by a majority
            broadcastForwarder.broadcastForward(message);
        }
    }


    /**
     * Updates the sliding window for a specific sender and cleans up related data.
     * This method first checks if it's possible to slide the sliding window for the given sender.
     * If so, it increments the start of the sliding window for this sender by the predefined sliding window size.
     * It then clears the data related to delivered messages for this sender to free up memory and maintain accuracy.
     * Lastly, it suggests to the JVM that it may be a good time for garbage collection, which can help manage memory usage
     *
     * @param originalSender The identifier (ID) of the original sender of the messages.
     */
    private void updateAndCleanSlidingWindow(byte originalSender) {
        // Check if it's possible to slide the sliding window for this sender
        if (updateSlidingWindows(originalSender)) {
            // Increment the sliding window start for this sender
            slidingWindowStart[originalSender] += slidingWindowSize;

            // Clear the delivered map for this sender
            for (int messageId : deliveredMesssages[originalSender].keySet()) {
                deliveredMesssages[originalSender].get(messageId).clear();
            }
            deliveredMesssages[originalSender].clear();

            // Suggest a garbage collection
            System.gc();
        }
    }

    /**
     * Determines whether the sliding window for a given original sender is ready to be moved forward.
     * This is the case when all messages in the current window have been received and acknowledged by a majority of hosts.
     *
     * @param originalSender The byte identifier of the original sender.
     * @return True if the sliding window is ready to be moved, false otherwise.
     */
    private boolean updateSlidingWindows(byte originalSender) {
        // Check if all messages in the current sliding window have been received
        if (deliveredMesssages[originalSender].size() < slidingWindowSize) {
            return false; // Not all messages in the window have been received
        }

        // Check if all received messages have been delivered (i.e., acknowledged by at least half the hosts)
        for (int messageId : deliveredMesssages[originalSender].keySet()) {
            if (deliveredMesssages[originalSender].get(messageId).size() < (hosts.size() / 2) + 1) {
                return false; // This message has not been delivered to a majority yet
            }
        }
        return true; // All messages in the window have been delivered to a majority
    }

    /**
     * Retrieves the delivery status for a given original sender.
     * This is used to check which messages from a particular sender have been delivered and to which hosts.
     *
     * @param origSender The byte identifier of the original sender.
     * @return A map of message IDs to sets of host IDs that have received and acknowledged the message.
     */
    public HashMap<Integer, Set<Byte>> getDelivered(byte origSender) {
        return this.deliveredMesssages[origSender];
    }

}
