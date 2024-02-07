package cs451.milestone1;

import cs451.Forward;
import cs451.Host;
import cs451.DataPacket.Message;

import java.util.HashMap;
import java.util.Map;

// PerfectLinks ensures that messages are delivered once and only once, in the face of network failures.
public class PerfectChannel implements Forward {
    private final StubbornChannel stubbornChannel;
    private final Forward messageForwarder; // Next layer to deliver messages after ensuring reliability
    private final Map<Integer, Map<Integer, Boolean>> deliveredMessagesMap;
    private int[] slidingWindowStartIndices; // Indices where the sliding window starts for each sender
    private final int slidingWindowSize; // Size of the sliding window
    private int[] deliveredMessageCounts; // Counts of messages delivered for each sender
    private final HashMap<Integer, Host> hostMap; // Map of hosts by their IDs

    // Constructor
    public PerfectChannel(int port, Forward deliverer, HashMap<Integer, Host> hosts) {
        // Set sliding window size directly
        this.slidingWindowSize = 2000;

        // Initialize other components and structures
        this.stubbornChannel = new StubbornChannel(port, this, hosts.size(), this.slidingWindowSize);
        this.hostMap = hosts;
        this.messageForwarder = deliverer;
        this.deliveredMessagesMap = new HashMap<>();
        this.slidingWindowStartIndices = new int[hosts.size()+1];
        this.deliveredMessageCounts = new int[hosts.size()+1];

        // Initialize sliding window starts and delivery counts for each host
        for (int i = 0; i <= hosts.size(); i++) {
            this.slidingWindowStartIndices[i] = 0;
            this.deliveredMessageCounts[i] = 0;
        }

        // Any additional initialization can be done here if necessary
    }

    // Sends a message via stubborn links.
    public void send(Message message, Host targetHost) {
        stubbornChannel.send(message, targetHost);
    }

    // Stops the underlying stubborn links.
    public void stop() {
        stubbornChannel.stop();
    }

    // Starts the underlying stubborn links.
    public void start() {
        stubbornChannel.start();
    }

    // Processes and delivers messages ensuring exactly-once delivery semantics.
    @Override
    public void forward(Message message) {
        // Ensure the sender's entry in the delivered messages map is initialized
        ensureDeliveredMessagesMapInitialized(message.getSenderId());

        // If the message is within the old window, we still send an acknowledgment
        if (isMessageWithinOldWindow(message)) {
            sendAcknowledgment(message);
        }

        // If the message is within the current window, we may need to deliver it and send an acknowledgment
        if (isMessageWithinCurrentWindow(message)) {
            sendAcknowledgment(message);

            // Process the message delivery only if it has not been delivered before
            if (!isMessageDeliveredBefore(message)) {
                processMessageDelivery(message);
            }
        }
    }

    private void ensureDeliveredMessagesMapInitialized(int senderId) {
        // Initialize the map for this sender if it hasn't been already
        if (!deliveredMessagesMap.containsKey(senderId)) {
            deliveredMessagesMap.put(senderId, new HashMap<>());
        }
    }

    private boolean isMessageWithinOldWindow(Message message) {
        // A message is considered within the old window if its ID is not greater than the start of the sliding window
        return message.getId() <= slidingWindowStartIndices[message.getSenderId()];
    }

    private boolean isMessageWithinCurrentWindow(Message message) {
        // A message is considered within the current window if it falls between the start and the end of the sliding window
        return message.getId() > slidingWindowStartIndices[message.getSenderId()]
                && message.getId() <= slidingWindowStartIndices[message.getSenderId()] + slidingWindowSize;
    }

    private boolean isMessageDeliveredBefore(Message message) {
        // Check the delivered messages map to see if this message ID has been processed before
        return deliveredMessagesMap.get(message.getSenderId()).containsKey(message.getId());
    }

    private void sendAcknowledgment(Message message) {
        // Send an acknowledgment back to the sender to confirm receipt of the message
        send(new Message(message, message.getReceiverId(), message.getOriginalSender()),
                hostMap.get(message.getSenderId()));
    }

    private void processMessageDelivery(Message message) {
        // Deliver the message to the upper layer for further processing
        messageForwarder.forward(message);
        // Mark the message as delivered in our map
        markMessageAsDelivered(message);

        // If the window is fully processed, prepare for the next set of messages
        if (isCurrentWindowFullyProcessed(message.getSenderId())) {
            adjustForNextSlidingWindow(message.getSenderId());
        }
    }

    private void markMessageAsDelivered(Message message) {
        // Mark the message as delivered by adding it to the map with a flag of true
        deliveredMessagesMap.get(message.getSenderId()).put(message.getId(), true);
        // Increment the count of delivered messages for this sender
        deliveredMessageCounts[message.getSenderId()]++;
    }

    private boolean isCurrentWindowFullyProcessed(int senderId) {
        // Determine if all messages within the current window have been delivered
        return deliveredMessagesMap.get(senderId).size() == slidingWindowSize;
    }

    private void adjustForNextSlidingWindow(int senderId) {
        // Move the start of the sliding window forward by the size of the window
        slidingWindowStartIndices[senderId] += slidingWindowSize;
        // Clear the delivered messages map for the next window of messages
        deliveredMessagesMap.get(senderId).clear();
        // Reset the delivered message count for the next window
        deliveredMessageCounts[senderId] = 0;
    }

}
