package cs451;

import cs451.milestone1.PerfectChannel;
import cs451.DataPacket.Message;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.nio.charset.StandardCharsets;

public class Process implements Forward {
    private final int id; // Unique identifier for the process
    private final HashMap<Integer, Host> hostMap; // Map of process IDs to Host objects for communication
    private final PerfectChannel perfectChannel; // PerfectLinks ensure reliable message delivery
    private final String outputPath; // File path for logging output
    private int lastSentMessageId; // ID of the last message that was sent by this process
    private long totalMessagesToSend; // Total number of messages this process will send
    private final ConcurrentLinkedQueue<String> logQueue; // Queue for log entries
    private ConcurrentLinkedQueue<String> bufferLogQueue; // Buffer to hold logs temporarily before writing to file
    private long receivedMessageCount; // Counter for received messages
    private final AtomicBoolean isWritingToFile; // Flag indicating if log writing to file is in progress

    private final Timer logWriteTimer; // Timer to schedule log writing tasks

    // ReentrantLock for thread-safe operation on logs
    Lock logLock = new ReentrantLock();

    /**
     * Constructor for the Process class.
     *
     * @param id            The unique identifier for this process.
     * @param port          The network port that the process listens on.
     * @param hostList      List of all hosts in the distributed system.
     * @param output        Path to the output file for logging.
     *
     */
    public Process(int id, int port, List<Host> hostList, String output) {
        this.id = id;
        this.hostMap = new HashMap<>();
        for (Host host : hostList) {
            this.hostMap.put(host.getId(), host);
        }
        this.perfectChannel = new PerfectChannel(port, this, this.hostMap);
        this.outputPath = output;
        this.receivedMessageCount = 0;
        this.lastSentMessageId = 1;
        this.logQueue = new ConcurrentLinkedQueue<>();
        this.isWritingToFile = new AtomicBoolean(false);

        // Initialize a timer to periodically check and write logs to the output file
        this.logWriteTimer = new Timer();
        logWriteTimer.schedule(new LogWriterTask(), 4000, 5000); // Schedule with an initial delay of 4 seconds and a subsequent rate of 5 seconds.

    }

    class LogWriterTask extends TimerTask {
        @Override
        public void run() {
            try {
                attemptToWriteLogs();
            } catch (Exception e) {
                System.err.println("An error occurred during log writing: " + e.getMessage());
            }
        }

        private void attemptToWriteLogs() {
            if (shouldWriteLogs()) {
                List<String> logsToWrite = safelyDequeueLogs();
                if (!logsToWrite.isEmpty()) {
                    writeLogsToFile(logsToWrite);
                }
            }
        }

        private boolean shouldWriteLogs() {
            return logQueue.size() > calculateThreshold() && isWritingToFile.compareAndSet(false, true);
        }

        private int calculateThreshold() {
            return 3000000 / hostMap.size();
        }

        private List<String> safelyDequeueLogs() {
            List<String> logsToWrite = new ArrayList<>();
            logLock.lock();
            try {
                if (!logQueue.isEmpty()) {
                    logsToWrite.addAll(logQueue);
                    logQueue.clear();
                }
            } finally {
                logLock.unlock();
                isWritingToFile.set(false);
            }
            return logsToWrite;
        }

        private void writeLogsToFile(List<String> logsToWrite) {
            try (FileOutputStream outputStream = new FileOutputStream(outputPath, true)) {
                for (String log : logsToWrite) {
                    outputStream.write(log.getBytes());
                }
            } catch (IOException e) {
                System.err.println("Failed to write logs to file: " + e.getMessage());
            }
        }
    }
    /**
     * Attempts to send a sequence of messages to a specified destination.
     *
     * @param targetMessageCount The total number of messages to send.
     * @param destinationId      The identifier of the destination host.
     */
    public void send(int targetMessageCount, int destinationId) {
        this.totalMessagesToSend = targetMessageCount;
        Host destinationHost = this.hostMap.get(destinationId);

        // If no host corresponds to the destination ID, stop the sending process.
        if (destinationHost == null) return;

        // Continue sending messages until the last sent message ID reaches the target count.
        while (lastSentMessageId <= targetMessageCount) {
            // Send the message using the reliable link and log the action.
            perfectChannel.send(new Message(lastSentMessageId, id, destinationId, id), destinationHost);
            logQueue.add("b " + lastSentMessageId + "\n");
            lastSentMessageId++;
        }
        //System.out.println("Broadcasted all messages!");
    }


    /**
     * Retrieves the unique identifier of this process.
     *
     * @return the process ID as an integer.
     */
    public int getId() {
        return id;
    }

    /**
     * Sets the total number of messages this process is supposed to send.
     *
     * @param messageCount The total number of messages to set.
     */
    public void setMessageCount(long messageCount) {
        this.totalMessagesToSend = messageCount;
    }

    /**
     * Retrieves the PerfectChannel object associated with this process.
     *
     * @return the PerfectLinks object.
     */
    public PerfectChannel getLinks() {
        return perfectChannel;
    }

    /**
     * Stops the message processing and logging tasks.
     */
    public void stopProcessing() {
        perfectChannel.stop(); // Stops the PerfectLinks component.
        logWriteTimer.cancel(); // Cancels the log writing timer task.
    }

    /**
     * Starts the message processing within PerfectLinks.
     */
    public void startProcessing() {
        perfectChannel.start(); // Starts the PerfectLinks component.
    }

    /**
     * Writes the log messages to the output file.
     */
    public void writeOutput() {
        try (FileOutputStream logOutput = new FileOutputStream(outputPath, true)) {
            // Write and remove each entry from the primary log queue.
            while (!logQueue.isEmpty()) {
                logOutput.write(logQueue.poll().getBytes());
            }

            // If intermediate logs exist, continue to write them to the output.
            if (bufferLogQueue == null) {
                return;
            }
            while (!bufferLogQueue.isEmpty()) {
                logOutput.write(bufferLogQueue.poll().getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks if the process is currently writing to the output file.
     *
     * @return true if writing is in progress, false otherwise.
     */
    public Boolean isWriting() {
        return isWritingToFile.get();
    }

    /**
     * Receives and processes incoming messages.
     *
     * @param message The message that has been delivered.
     */
    @Override
    public void forward(Message message) {
        // Acquire the lock to ensure thread-safe access to the logs.
        logLock.lock();
        try {
            // Log the delivery of the message.
            logQueue.add("d " + message.getSenderId() + " " + message.getId() + "\n");
        } finally {
            // Always ensure the lock is released to avoid deadlocks.
            logLock.unlock();
        }
        // Increment the count of received messages.
        receivedMessageCount++;
    }
}
