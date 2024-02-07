package cs451.milestone2;

import cs451.Host;
import cs451.DataPacket.Message;
import cs451.milestone1.PerfectChannel;
import cs451.CallbackMethods.TransmissionController;
import cs451.CallbackMethods.Forward;
import cs451.CallbackMethods.BroadcastForward;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * BestEffort class provides the functionality of best-effort broadcast over a distributed system.
 * It relies on PerfectLinks to send messages to other nodes, ensuring that messages are sent reliably.
 */
public class BestEffort implements TransmissionController {
    private PerfectChannel perfectChannel; // Underlying PerfectChannels for reliable message transmission
    private byte id; // Identifier of the current node
    private int[] lastSentMessageId; // Array to track the last sent message ID for each host
    private final int messageCount; // Total number of messages to be handled
    private AtomicIntegerArray sendWindow; // Atomic array to manage the sending window for each host
    private final int slidingWindowSize; // Size of the sliding window
    private final HashMap<Byte, Host> hosts; // Mapping of host identifiers to Host objects

    /**
     * Constructor for BestEffort.
     * Initializes the necessary components for best-effort broadcasting of messages.
     *
     * @param id The identifier of the current node.
     * @param port The port number for the underlying PerfectLinks.
     * @param hosts A map of host identifiers to Host objects.
     * @param slidingWindowSize The size of the sliding window for message transmission.
     * @param deliverer The BroadcastForward interface for message delivery.
     * @param messageCount The total number of messages to be handled.
     */
    public BestEffort(byte id, int port, HashMap<Byte, Host> hosts, int slidingWindowSize,
                      BroadcastForward deliverer, int messageCount) {
        // Initialize atomic array for managing the sending window and array for last sent message IDs
        this.sendWindow = new AtomicIntegerArray(hosts.size());
        this.lastSentMessageId = new int[hosts.size()];
        this.slidingWindowSize = slidingWindowSize;
        this.messageCount = messageCount;
        this.id = id;
        this.hosts = hosts;

        // Set up the initial values for the sending window size and the ID of the last message sent to each host.
        for (Host currentHost : hosts.values()) {
            int hostId = currentHost.getId();
            sendWindow.set(hostId, slidingWindowSize); // Assign sliding window size for each host
            lastSentMessageId[hostId] = 1; // Start tracking the last sent message ID from 1
        }

        // Initialize PerfectLinks for reliable message transmission
        this.perfectChannel = new PerfectChannel(port, id, deliverer, this.hosts, slidingWindowSize, this, messageCount);
    }

    /**
     * Continuously sends messages to all hosts except for the current node itself.
     * The messages are sent in a sequence determined by their IDs, managed by the lastSentMessageId array.
     * The method continues to send messages until all messages have been sent according to the message count.
     */
    public void send() {
        boolean allMessagesSent; // Indicates if all messages have been sent

        // Continuously process each host for message sending
        do {
            boolean shouldPause = true; // Determines if a pause in processing is needed
            allMessagesSent = true; // Reset the flag for each iteration

            // Loop through each host to manage message sending
            for (byte currentHostId : hosts.keySet()) {
                if (currentHostId == id) continue; // Skip sending to self

                // Validate if there are remaining messages to send to this host
                if (lastSentMessageId[currentHostId] <= messageCount) {
                    int windowLimit = sendWindow.get(currentHostId);
                    // Proceed if within the window limit
                    if (lastSentMessageId[currentHostId] <= windowLimit) {
                        perfectChannel.send(new Message(lastSentMessageId[currentHostId], id, currentHostId, id), hosts.get(currentHostId));
                        lastSentMessageId[currentHostId]++; // Update message ID for the host
                        shouldPause = false; // Continue sending without pause
                    }
                    allMessagesSent = false; // Update flag as not all messages are sent
                }
            }

            // Pause if no messages were sent in this iteration
            if (shouldPause) {
                performSleep(); // Encapsulated sleep logic
            }
        } while (!allMessagesSent); // Continue until all messages are sent
    }

    /**
     * Pauses the thread execution to regulate the message sending rate.
     * This method is invoked to introduce a delay in the sending loop, helping to manage the system's resource usage
     * and control the rate at which messages are sent. It triggers garbage collection and then pauses the thread.
     */
    private void performSleep() {
        try {
            System.gc(); // Trigger garbage collection
            Thread.sleep(1000); // Pause the thread to regulate sending rate
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Starts the underlying PerfectLinks, enabling message transmission.
     */
    public void start() {
        // Start the PerfectLinks instance for message transmission
        perfectChannel.start();
    }

    /**
     * Stops the underlying PerfectLinks, halting all message transmission.
     */
    public void stop() {
        // Stop the PerfectLinks instance to halt message transmission
        perfectChannel.stop();
    }

    /**
     * Retrieves the delivery status of messages for a given original sender.
     * This method is useful for checking which messages have been delivered and acknowledged by other nodes.
     *
     * @param origSender The byte identifier of the original sender.
     * @return A map of message IDs to sets of host IDs indicating which hosts have received and acknowledged the message.
     */
    public HashMap<Integer, Set<Byte>> getDelivered(byte origSender) {
        // Delegate the request to the PerfectLinks instance
        return perfectChannel.getDelivered(origSender);
    }

    /**
     * Distributes the specified message to all participating nodes except itself.
     * Useful for propagating information across the network, this method ensures that
     * every other node receives the message.
     *
     * @param message The message to distribute to other nodes.
     */
    public void rebroadcast(Message message) {
        // Loop through each host in the network
        for (byte currentHostId : hosts.keySet()) {
            // Exclude the current node from receiving the message
            if (currentHostId == id) continue;

            // Prepare a new instance of the message for distribution
            Message outgoingMessage = new Message(message, id, currentHostId, false);
            // Send the message using PerfectLinks for reliable delivery
            perfectChannel.send(outgoingMessage, hosts.get(currentHostId));
        }
    }

    /**
     * Slides the sending window forward for a given host.
     * This method increases the sending window size, allowing more messages to be sent to the specified host.
     *
     * @param hostId The identifier of the host for which the sending window is to be slid.
     */
    @Override
    public void slideSendWindow(byte hostId) {
        // Increase the sending window limit for the specified host
        sendWindow.addAndGet(hostId, slidingWindowSize);
    }

    /**
     * Stops the sending processes. This method is part of the TransmissionController interface but is not implemented here.
     */
    @Override
    public void stopSenders() {
        // Not implemented in this class
    }

}
