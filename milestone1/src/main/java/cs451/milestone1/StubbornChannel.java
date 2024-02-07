package cs451.milestone1;

import cs451.Host;
import cs451.Forward;
import cs451.DataPacket.Message;
import cs451.DataPacket.NetworkMessageWrapper;
import cs451.DataPacket.MessageBatch;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class StubbornChannel implements Forward {
    // Communication link that simulates fair loss link behavior.
    private final FairLossChannel fairLossChannel;

    // Upstream deliverer to handle message delivery.
    private final Forward messageForwarder;

    // Total number of hosts in the network.
    private final int totalHosts;

    // Array to keep track of sliding window size for each host.
    private final int[] hostSlidingWindows;

    // Array to keep track of the number of messages delivered for each host.
    private final int[] deliveryCounts;

    // The size of the sliding window used for message delivery.
    private final int slidingWindowCapacity;

    // Stores messages that are pending to be sent reliably.
    private final ConcurrentHashMap<Integer, NetworkMessageWrapper> pendingMessages;

    // Stores acknowledgment messages that are pending to be sent.
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, NetworkMessageWrapper>> pendingAckMessages;

    // Counter to maintain the number of operations or messages handled.
    private int operationCount;

    // Thread responsible for sending out messages continuously.
    private final Runnable messageSender;

    // Thread responsible for sending out acknowledgments continuously.
    private final Runnable ackSender;

    // Flag to control the running state of the sending threads.
    AtomicBoolean isActive;

    public StubbornChannel(int port, Forward deliverer, int hostCount, int windowSize) {
        // Calculate maximum memory allocation based on the number of hosts.

        // Initialize the fair loss link with the given parameters.
        this.fairLossChannel = new FairLossChannel(port, this, hostCount);

        // Set the upstream deliverer.
        this.messageForwarder = deliverer;

        // Prepare concurrent maps for storing messages and acknowledgments.
        this.pendingMessages = new ConcurrentHashMap<>();
        this.pendingAckMessages = new ConcurrentHashMap<>();

        // Initialize sliding window sizes and delivery counts for each host.
        this.hostSlidingWindows = new int[hostCount+1];
        this.deliveryCounts = new int[hostCount+1];
        this.slidingWindowCapacity = windowSize;
        Arrays.fill(hostSlidingWindows, slidingWindowCapacity); // Initialize all sliding windows with the given capacity.
        Arrays.fill(deliveryCounts, 0); // Initialize all delivery counts to zero.

        this.totalHosts = hostCount;
        this.operationCount = 0;
        this.isActive = new AtomicBoolean(true);

        this.messageSender = () -> runPeriodicTask(this::sendPendingMessages, 300);
        this.ackSender = () -> runPeriodicTask(this::sendPendingAckMessages, 200);

    }

    private void runPeriodicTask(Runnable task, int sleepDuration) {
        while (isActive.get()) {
            try {
                task.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(sleepDuration);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }
    }

    private void sendPendingMessages() {
        sendMessagesToBeSent(new ArrayList<>(pendingMessages.values()));
    }

    private void sendPendingAckMessages() {
        pendingAckMessages.keySet().forEach(hostKey ->
                sendAckMessagesToBeSent(new ArrayList<>(pendingAckMessages.get(hostKey).values()))
        );
    }

    // Helper method to send a list of messages to a specific host.
    private void sendBatchToHost(List<Message> batch, Host destinationHost) {
        if (!batch.isEmpty()) {
            fairLossChannel.send(new MessageBatch(batch), destinationHost);
        }
    }

    // Public method to send messages considering the sliding window.
    public void sendMessagesToBeSent(ArrayList<NetworkMessageWrapper> messages) {
        if (messages.isEmpty()) return;

        List<Message> batch = new ArrayList<>();
        Host destinationHost = messages.get(0).getHost(); // All messages assumed to be for the same host.

        for (NetworkMessageWrapper wrapper : messages) {
            Message message = wrapper.getMessage();
            if (!fairLossChannel.isMessageScheduledForSending(message.getReceiverId(), message.getId())) {
                int receiverSlidingWindowSize = hostSlidingWindows[message.getReceiverId()];
                if (message.getId() <= receiverSlidingWindowSize) {
                    batch.add(message);
                    if (batch.size() == 8) {
                        sendBatchToHost(batch, destinationHost);
                        batch.clear();
                    }
                }
            }
        }
        sendBatchToHost(batch, destinationHost); // Send any remaining messages.
        if (!batch.isEmpty()){
            batch.clear();
        }

    }

    // Tries to send the acknowledgment messages that are pending.
    private void sendAckMessagesToBeSent(ArrayList<NetworkMessageWrapper> ackMessages) {
        // If the sending queue is full, do not proceed with sending acks.
        if (fairLossChannel.isWorkloadAboveThreshold()) {
            return;
        }

        // Prepare a list to batch acknowledgment messages for sending.
        List<Message> acksToSend = new ArrayList<>();

        for (NetworkMessageWrapper wrapper : ackMessages) {
            if (!fairLossChannel.isMessageScheduledForSending(wrapper.getMessage().getReceiverId(), wrapper.getMessage().getId())) {
                acksToSend.add(wrapper.getMessage());
                // Remove the ack message from the pending list once queued for sending.
                pendingAckMessages.get(wrapper.getMessage().getReceiverId()).remove(wrapper.getMessage().getId());

                // If we have reached a batch size of 8, send the package.
                if (acksToSend.size() == 8) {
                    sendBatchToHost(acksToSend, wrapper.getHost());
                    acksToSend.clear();
                }
            }
        }
        // Send any remaining ack messages if they didn't make up a full batch.
        sendBatchToHost(acksToSend, ackMessages.get(0).getHost());
        if (!acksToSend.isEmpty()){
            acksToSend.clear();
        }
    }

    // Starts the message and acknowledgement sending threads.
    public void start() {
        fairLossChannel.start(); // Start the underlying fair loss link.
        new Thread(messageSender).start(); // Start thread to handle sending of messages.
        new Thread(ackSender).start(); // Start thread to handle sending of acknowledgments.
    }

    // Schedules a message for sending.
    public void send(Message message, Host recipientHost) {
        NetworkMessageWrapper wrapper = new NetworkMessageWrapper(message, recipientHost);
        if (message.isAckMessage()) {
            // Use 'compute' to atomically modify the inner map for acknowledgments.
            // This block does not 'return' anything in the context of the 'send' method.
            // It simply instructs 'compute' on how to update the map.
            pendingAckMessages.compute(message.getReceiverId(), (receiverId, messageMap) -> {
                // Create a new map if there isn't one already for this receiver ID.
                if (messageMap == null) {
                    messageMap = new ConcurrentHashMap<>();
                }
                // Place the wrapper into this map.
                messageMap.put(message.getId(), wrapper);
                // Return the modified map to update the 'pendingAckMessages'.
                return messageMap;
            });
        } else {
            // If it's not an ack message, just put it in the 'pendingMessages'.
            pendingMessages.put(message.getId(), wrapper);
        }
        // The method is 'void' and does not return anything.
    }


    // Stops the message and acknowledgement sending threads.
    public void stop() {
        this.isActive.set(false); // Signal the threads to stop running.
        fairLossChannel.stop(); // Stop the underlying fair loss link.
    }

    // Processes a received message.
    @Override
    public void forward(Message message) {
        // If the original sender is also the receiver, it means we have sent this message.
        if (message.getOriginalSender() == message.getReceiverId()) {
            // Check if we have indeed sent this message and it's waiting for acknowledgment.
            if (pendingMessages.containsKey(message.getId())) {
                try {
                    // Attempt to cancel any pending resend tasks for this message (if implemented).
                    // runnerTasks.get(message.getId()).forEach(f -> f.cancel(false));
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    // Once delivered, remove the message from pending and update delivered count.
                    pendingMessages.remove(message.getId());
                    deliveryCounts[message.getSenderId()]++; // Increment delivered count.
                    // If we have delivered enough messages, move the sliding window.
                    if(deliveryCounts[message.getSenderId()] >= hostSlidingWindows[message.getSenderId()]){
                        hostSlidingWindows[message.getSenderId()] += this.slidingWindowCapacity;
                    }
                }

            }
        } else {
            // If the message is not an echo of our own message, pass it to the next layer.
            messageForwarder.forward(message);
        }
    }

}
