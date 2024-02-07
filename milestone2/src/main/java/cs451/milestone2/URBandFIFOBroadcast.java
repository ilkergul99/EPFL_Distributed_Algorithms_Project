package cs451.milestone2;

import cs451.Process;
import cs451.Host;
import cs451.DataPacket.Message;
import cs451.CallbackMethods.Forward;
import cs451.CallbackMethods.BroadcastForward;

import java.util.*;

/**
 * URBandFIFOBroadcast class provides the functionality for uniform reliable broadcasting in a distributed system.
 * It ensures that messages are delivered uniformly to all processes, and no message is lost or duplicated.
 */
public class URBandFIFOBroadcast implements BroadcastForward {
    private final BestEffort bestEffortBroadcast; // Best-effort broadcast underlying mechanism
    private final int[] lastDeliveredMessageId; // Array to track the last delivered message ID for each host
    private final int slidingWindowSize; // Size of the sliding window
    private final Message[][] waitingMessages; // 2D array to hold pending messages
    private final Forward forwarder; // Deliverer for handling message delivery

    /**
     * Constructor for URBandFIFOBroadcast.
     * Initializes the necessary components for uniform reliable broadcasting of messages.
     *
     * @param id The identifier of the current node.
     * @param port The port number for the underlying BestEffortBroadcast.
     * @param hostList A list of hosts participating in the broadcast.
     * @param slidingWindowSize The size of the sliding window for message transmission.
     * @param forwarder The Forward interface for message delivery.
     * @param messageCount The total number of messages to be handled.
     */
    public URBandFIFOBroadcast(byte id, int port, List<Host> hostList, int slidingWindowSize, Forward forwarder, int messageCount){
        this.forwarder = forwarder;
        // Create a map of hosts from the host list
        HashMap<Byte, Host> hostMap = new HashMap<>();
        this.waitingMessages = new Message[hostList.size()][slidingWindowSize]; // Initialize pending messages array
        this.lastDeliveredMessageId = new int[hostList.size()]; // Initialize last delivered message ID array
        this.slidingWindowSize = slidingWindowSize; // Set sliding window size

        // Populate the host map and initialize the last delivered message IDs
        for (Host host : hostList) {
            hostMap.put((byte)host.getId(), host);
            this.lastDeliveredMessageId[host.getId()] = 0;
        }

        // Initialize BestEffortBroadcast for message transmission
        this.bestEffortBroadcast = new BestEffort(id, port, hostMap, slidingWindowSize, this, messageCount);
    }

    /**
     * Initiates the sending of messages.
     * This method delegates the message sending task to the underlying BestEffortBroadcast instance.
     */
    public void send() {
        // Delegates the send operation to the BestEffortBroadcast
        bestEffortBroadcast.send();
    }

    /**
     * Stops the broadcasting operation.
     * This method stops the underlying BestEffortBroadcast instance, halting all message transmission.
     */
    public void stop() {
        // Delegates the stop operation to the BestEffortBroadcast
        bestEffortBroadcast.stop();
    }

    /**
     * Starts the broadcasting operation.
     * This method starts the underlying BestEffortBroadcast instance, making it ready for message transmission.
     */
    public void start() {
        // Delegates the start operation to the BestEffortBroadcast
        bestEffortBroadcast.start();
    }

    /**
     * Delivers a received message uniformly to all nodes.
     * After delivering the message, it rebroadcasts the message using the underlying BestEffortBroadcast.
     *
     * @param message The message to be delivered.
     */
    @Override
    public void forward(Message message) {
        // Rebroadcast the message using BestEffortBroadcast
        bestEffortBroadcast.rebroadcast(message);
    }

    /**
     * Ensures uniform distribution of messages across all network nodes.
     * Adheres to the protocols of uniform reliable broadcasting by delivering messages in a sequential order.
     *
     * @param message The message that needs to be distributed uniformly.
     */
    @Override
    public void broadcastForward(Message message) {
        // Calculate the expected next message ID from the sender
        int expectedNextMsgId = lastDeliveredMessageId[message.getOriginalSender()] + 1;

        // Check if the incoming message is the next sequential message
        if (message.getId() == expectedNextMsgId) {
            // Process and forward the message if it's in the correct sequence
            processAndDeliverMessage(message);
            // Handle any subsequent messages that are now in sequence
            handleSequentialMessages(message);
        } else {
            // Queue the message if it's not the next expected message
            queueMessageIfNotInSequence(message);
        }
    }

    /**
     * Processes and delivers a given message, then updates the tracking for the last delivered message ID.
     *
     * @param message The message to be processed and delivered.
     */
    private void processAndDeliverMessage(Message message) {
        // Deliver the message to its destination
        forwarder.forward(message);
        // Update the last delivered message ID for the sender
        lastDeliveredMessageId[message.getOriginalSender()] += 1;
    }

    /**
     * Handles the delivery of messages that were on hold, waiting for the previous messages to be delivered.
     * Ensures messages are delivered in the correct sequence.
     *
     * @param message The most recently delivered message, which might allow subsequent messages to be delivered.
     */
    private void handleSequentialMessages(Message message) {
        // Retrieve the array of pending messages from this sender
        Message[] messagesFromSender = waitingMessages[message.getOriginalSender()];
        for (int idx = 0; idx < messagesFromSender.length; idx++) {
            // Deliver messages that are next in the sequence
            if (isNextSequentialMessage(messagesFromSender[idx], message.getOriginalSender())) {
                forwarder.forward(messagesFromSender[idx]);
                // Update the last delivered message ID
                lastDeliveredMessageId[message.getOriginalSender()] += 1;
                // Clear the delivered message from the pending array
                messagesFromSender[idx] = null;
            }
        }
    }

    /**
     * Determines if a pending message is the next sequential message expected from a specific sender.
     *
     * @param pendingMessage The message in the pending queue to be checked.
     * @param originalSender The original sender of the message.
     * @return boolean indicating whether the message is the next in the sequence.
     */
    private boolean isNextSequentialMessage(Message pendingMessage, byte originalSender) {
        // Check if the pending message is the next expected message in the sequence
        return pendingMessage != null && pendingMessage.getId() == lastDeliveredMessageId[originalSender] + 1;
    }

    /**
     * Queues a message in the pending queue if it's not in the correct sequence.
     * Ensures out-of-sequence messages are stored for later processing.
     *
     * @param message The message that is out of sequence and needs to be queued.
     */
    private void queueMessageIfNotInSequence(Message message) {
        // Calculate the index for the pending message based on its ID
        int pendingIndex = (message.getId() - 1) % slidingWindowSize;
        // Place the message in the pending array at the calculated index
        this.waitingMessages[message.getOriginalSender()][pendingIndex] = message;
    }

}
