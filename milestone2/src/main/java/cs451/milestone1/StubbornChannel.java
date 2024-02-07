package cs451.milestone1;

import cs451.Host;
import cs451.DataPacket.Message;
import cs451.DataPacket.MessageBatch;
import cs451.CallbackMethods.TransmissionController;
import cs451.CallbackMethods.Forward;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * StubbornLinks class ensures that messages are delivered despite potential losses in the network.
 * It uses FairLossLinks to repeatedly send messages until they are acknowledged, ensuring reliability.
 */
public class StubbornChannel implements Forward {
    private final FairLossChannel fairLossChannel; // Underlying fair loss links used for actual message transmission
    private final Forward forwarder; // Forwarder for handling received messages
    private final TransmissionController transmissionController; // TransmissionController interface for acknowledging messages
    private final AtomicIntegerArray slidingWindows; // Array to manage sliding windows for each host
    private final AtomicIntegerArray messagesDelivered; // Array to track delivered messages for each host

    private final int slidingWindowSize; // Size of the sliding window for message transmission
    private final int hostSize; // Size of the current hosts
    private final List<Message>[] messageQueue; // Array of lists holding messages to be sent for each host
    private final ConcurrentHashMap<Byte, ConcurrentHashMap<Message, Boolean>> awaitingAcknowledgments; // Map for tracking acknowledgment messages to be sent
    private int count; // Counter for tracking some activity
    private final Runnable messageSender; // Thread for sending messages
    private final Runnable ackSender; // Thread for sending acknowledgments
    private final AtomicBoolean isActive; // Flag to control the running state of threads
    private final HashMap<Byte, Host> hosts; // Map of host identifiers to Host objects
    private final Lock sourceLock; // Lock for thread synchronization
    private final int messageCount; // Total number of messages to handle

    /**
     * Constructor for StubbornLinks.
     * Sets up the necessary environment for reliable message delivery over potentially lossy links.
     *
     * @param port The port number for the underlying FairLossLinks.
     * @param hosts A map of host identifiers to Host objects.
     * @param forwarder The Forward for handling received messages.
     * @param hostSize The number of hosts in the network.
     * @param slidingWindowSize The size of the sliding window for message transmission.
     * @param transmissionController The TransmissionController for confirming message deliveries.
     * @param messageCount The total number of messages to be handled.
     */
    public StubbornChannel(int port, HashMap<Byte, Host> hosts, Forward forwarder, int hostSize,
                           int slidingWindowSize, TransmissionController transmissionController, int messageCount) {
        this.hostSize = hostSize; // Set the size of current hosts
        // Initialize FairLossLinks for actual message transmission
        this.fairLossChannel = new FairLossChannel(port, this);
        this.forwarder = forwarder; // Set forwarder for handling message deliveries
        this.transmissionController = transmissionController; // Set TransmissionController for acknowledging message deliveries
        this.sourceLock = new ReentrantLock(); // Initialize the lock for thread safety
        this.hosts = hosts; // Host map
        this.messageCount = messageCount; // Set the total message count

        // Initialize data structures for managing messages
        this.messageQueue = new List[this.hostSize];
        this.awaitingAcknowledgments = new ConcurrentHashMap<>();
        this.slidingWindows = new AtomicIntegerArray(this.hostSize); // Sliding window for each host
        this.messagesDelivered = new AtomicIntegerArray(this.hostSize); // Messages delivered per host

        // This sets the size of the sliding window, which is used to control the flow of message transmission
        this.slidingWindowSize = slidingWindowSize;

        // Setup sliding windows and message lists for each host
        for (int i = 0; i < this.hostSize; i++) {
            slidingWindows.set(i, slidingWindowSize); // Initialize sliding window size
            messagesDelivered.set(i, 0); // Initialize delivered messages count
            this.messageQueue[i] = new ArrayList<>(); // Initialize message list
        }

        this.count = 0; // Initialize the counter
        this.isActive = new AtomicBoolean(true); // Flag to control the running state of threads

        // Define the message sending thread
        this.messageSender = () -> runPeriodicTask(this::processMessageSending, 800);
        // Define the acknowledgment sending thread
        this.ackSender = () -> runPeriodicTask(this::processAckSending, 300);

    }

    /**
     * Runs a specified task periodically based on the given sleep duration. The task is executed
     * repeatedly as long as the 'isActive' condition remains true. This method is designed to facilitate
     * the execution of background tasks at regular intervals, such as sending acknowledgments or
     * performing periodic checks.
     *
     * The method ensures that the provided task is executed in a safe manner by catching and logging
     * any exceptions that may occur during its execution. After each execution, the thread is put to sleep
     * for a specified duration to control the rate of task execution.
     *
     * @param task The Runnable task to be executed periodically.
     * @param sleepDuration The duration (in milliseconds) for which the thread should sleep between consecutive task executions.
     */
    private void runPeriodicTask(Runnable task, int sleepDuration) {
        while (isActive.get()) {
            try {
                task.run();
            } catch (Exception e) {
                e.printStackTrace();
            }
            sleepThread(sleepDuration); // Sleep to control acknowledgment sending rate
        }
    }
    /**
     * Processes the message sending for each host.
     * This method iterates through all the hosts in the network and checks if there are any messages
     * in the queue for each host. If there are messages, it creates a temporary list of these messages
     * and sends them using the `sendMessagesToBeSent` method. This ensures that messages are sent to each
     * host as needed, based on the content of their respective message queues.
     */
    private void processMessageSending() {
        for (int host = 0; host < this.hostSize; host++) {
            // Check if there are messages in the queue for the current host
            if (messageQueue[host].size() > 0) {
                // Create a temporary list of messages to be sent
                var temp = new ArrayList<>(messageQueue[host]);
                // Send the messages to the corresponding host
                sendMessagesToBeSent(temp, this.hosts.get((byte) host));
            }
        }
    }

    /**
     * Processes the acknowledgment sending for each host.
     * This method iterates through all the hosts that are awaiting acknowledgments and checks if there
     * are any acknowledgment messages to be sent. If there are, it creates a temporary list of these
     * acknowledgment messages and sends them using the `sendAckMessagesToBeSent` method. This is essential
     * to ensure that acknowledgments are sent back to the hosts that are awaiting them, maintaining the
     * reliability and consistency of the messaging system.
     */
    private void processAckSending() {
        for (byte hostId : awaitingAcknowledgments.keySet()) {
            // Check if there are acknowledgment messages in the queue for the current host
            if (awaitingAcknowledgments.get(hostId).size() > 0) {
                // Create a temporary list of acknowledgment messages to be sent
                var temp = Collections.list(awaitingAcknowledgments.get(hostId).keys());
                // Send the acknowledgment messages to the corresponding host
                sendAckMessagesToBeSent(temp, this.hosts.get(hostId));
            }
        }
    }

    /**
     * Causes the currently executing thread to sleep for a specified number of milliseconds.
     * This method is used to pause the execution of the thread for a given duration, which is
     * useful for controlling the rate of message sending or acknowledgments in the system.
     * If the thread is interrupted while sleeping, the interruption is handled gracefully:
     * the interrupt flag is set again on the thread, and the exception is logged, ensuring
     * that the thread's interrupted status is properly maintained.
     *
     * @param milliseconds The duration for which the thread should sleep, in milliseconds.
     */
    private void sleepThread(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            // Re-interrupt the thread to maintain the interrupted status
            Thread.currentThread().interrupt();
            // Log the interrupted exception
            e.printStackTrace();
        }
    }

    /**
     * Sends a list of messages to the specified host. Only sends messages that are not already in the FairLossLinks queue
     * and are within the sliding window size. Messages are batched up to a size of 8 before being sent.
     *
     * @param messages The list of messages to be sent.
     * @param host The destination host for the messages.
     */
    private void sendMessagesToBeSent(List<Message> messages, Host host) {
        if (messages.size() == 0) return; // No action if there are no messages to send

        List<Message> messagesToSend = new ArrayList<>(); // Temporary list for batching messages
        messages.forEach(m -> {
            // Check if the message is not already queued for sending and is within the sliding window
            if (!fairLossChannel.isinQueue(m)) {
                int slidingWindowSize = slidingWindows.get(m.getReceiverId());
                // If the message is from the original sender and outside the sliding window, skip it
                if (m.getSenderId() == m.getOriginalSender() && m.getId() > slidingWindowSize) {
                    return;
                }
                // Add the message to the batch
                messagesToSend.add(m);
                // Send a batch of 8 messages at once
                if (messagesToSend.size() == 8) {
                    fairLossChannel.send(new MessageBatch(messagesToSend), host);
                    messagesToSend.clear(); // Clear the batch after sending
                }
            }
        });
        // Send any remaining messages in the final batch
        if (messagesToSend.size() > 0) {
            fairLossChannel.send(new MessageBatch(messagesToSend), host);
        }
    }

    /**
     * Sends a list of acknowledgment messages to the specified host. Only sends messages that are not already
     * in the FairLossLinks queue. Acknowledgments are batched up to a size of 8 before being sent.
     *
     * @param messages The list of acknowledgment messages to be sent.
     * @param host The destination host for the messages.
     */
    private void sendAckMessagesToBeSent(List<Message> messages, Host host) {
        List<Message> messagesToSend = new ArrayList<>(); // Temporary list for batching messages
        messages.forEach(m -> {
            // Check if the acknowledgment message is not already queued for sending
            if (!fairLossChannel.isinQueue(m)) {
                messagesToSend.add(m); // Add the message to the batch
                awaitingAcknowledgments.get(m.getReceiverId()).remove(m); // Remove from the pending acknowledgments
                // Send a batch of 8 messages at once
                if (messagesToSend.size() == 8) {
                    fairLossChannel.send(new MessageBatch(messagesToSend), host);
                    messagesToSend.clear(); // Clear the batch after sending
                }
            }
        });
        // Send any remaining messages in the final batch
        if (messagesToSend.size() > 0) {
            fairLossChannel.send(new MessageBatch(messagesToSend), host);
        }
    }

    /**
     * Starts the StubbornLinks, including the underlying FairLossChannel and the message sending threads.
     */
    public void start() {
        fairLossChannel.start(); // Start the FairLossChannel
        // Start the message sending thread
        new Thread(messageSender, "Message send thread").start();
        // Start the acknowledgment sending thread
        new Thread(ackSender, "Ack message send thread").start();
    }

    /**
     * Sends a message to the specified host. If the message is an acknowledgment, it's added to the
     * acknowledgment queue. Otherwise, it's added to the regular message queue.
     *
     * @param message The message to be sent.
     * @param host The destination host for the message.
     */
    public void send(Message message, Host host) {
        // Check if the message is an acknowledgment
        if (message.isAckMessage()) {
            // Add to the acknowledgment queue if it is an acknowledgment message
            awaitingAcknowledgments.computeIfAbsent(message.getReceiverId(), k -> new ConcurrentHashMap<>())
                    .put(message, true);
        } else {
            // Add to the regular message queue
            sourceLock.lock();
            try {
                messageQueue[message.getReceiverId()].add(message);
            } finally {
                sourceLock.unlock();
            }
        }
    }

    /**
     * Stops the StubbornLinks operations, including the underlying FairLossLinks and sending threads.
     */
    public void stop() {
        // Stop the sending threads
        this.isActive.compareAndSet(true, false);
        // Stop the underlying FairLossLinks
        fairLossChannel.stop();
    }

    /**
     * Stops the message and acknowledgment sending threads.
     */
    public void stopSenders() {
        // Stop the sending threads
        this.isActive.compareAndSet(true, false);
    }

    /**
     * Checks if the sliding windows are ready to be moved forward.
     * This occurs when enough messages have been delivered, or if the count
     * of delivered messages meets certain criteria related to the hosts.
     */
    private void updateSlidingWindows() {
        int readyHostsCount = 0;
        // Evaluate each host to determine if their sliding window can advance
        for (int hostIndex = 0; hostIndex < hosts.size(); hostIndex++) {
            if (messagesDelivered.get(hostIndex) >= slidingWindows.get(hostIndex) || messagesDelivered.get(hostIndex) == messageCount) {
                readyHostsCount++;
            }
        }

        // Adjust the sliding windows if enough hosts are ready
        if (readyHostsCount >= hosts.size() / 2) {
            for (int hostIndex = 0; hostIndex < hosts.size(); hostIndex++) {
                if (messagesDelivered.get(hostIndex) >= slidingWindows.get(hostIndex)) {
                    // Increment the sliding window start point for each ready host
                    slidingWindows.addAndGet(hostIndex, slidingWindowSize);
                    // Notify the TransmissionController about the window update
                    transmissionController.slideSendWindow((byte) hostIndex);
                }
            }
            // Request garbage collection to optimize memory usage
            System.gc();
        }
    }

    /**
     * Handles an acknowledgment message by removing the corresponding original
     * message from the queue and processing it.
     *
     * @param message The acknowledgment message received.
     */
    private void handleAckMessage(Message message) {
        // Create a response message
        Message initialMessage = message.createResponseMessage();
        sourceLock.lock();
        try {
            // Remove the original message from the queue and process if successful
            if (removeOriginalMessageFromQueue(initialMessage)) {
                processOriginalMessage(initialMessage);
            }
        } finally {
            // Ensure the lock is released
            sourceLock.unlock();
        }
    }

    /**
     * Attempts to remove the original message from the queue.
     *
     * @param initialMessage The original message corresponding to the acknowledgment.
     * @return boolean indicating whether the message was successfully removed from the queue.
     */
    private boolean removeOriginalMessageFromQueue(Message initialMessage) {
        // Attempt to remove the original message from the queue
        return messageQueue[initialMessage.getReceiverId()].remove(initialMessage);
    }

    /**
     * Processes the original message upon receiving its acknowledgment.
     * This involves recording its successful delivery, updating sliding windows, and tracking the count.
     *
     * @param initialMessage The original message that was acknowledged.
     */
    private void processOriginalMessage(Message initialMessage) {
        // Check if the original sender is the same as the sender ID in the message
        if (initialMessage.getOriginalSender() == initialMessage.getSenderId()) {
            // Increment the count of messages delivered
            messagesDelivered.incrementAndGet(initialMessage.getReceiverId());
            // Update sliding windows based on the delivery
            updateSlidingWindows();
            // Increment the total count of messages processed
            count++;
        }
    }

    /**
     * Routes non-acknowledgment messages to the designated forwarder.
     *
     * @param message The non-acknowledgment message to be delivered.
     */
    private void routeNonAckMessage(Message message) {
        // Deliver the message using the specified forwarder
        forwarder.forward(message);
    }

    /**
     * Handles the delivery of a message. If the message is an acknowledgment, it processes the acknowledgment.
     * Otherwise, it forwards the message to the forwarder for further processing.
     *
     * @param message The message to be delivered.
     */
    @Override
    public void forward(Message message) {
        // Check if the message is an acknowledgment and handle accordingly
        if (message.isAckMessage()) {
            handleAckMessage(message);
        } else {
            // Route non-acknowledgment messages to the designated forwarder
            routeNonAckMessage(message);
        }
    }

}
