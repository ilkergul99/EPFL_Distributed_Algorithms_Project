package cs451;

import cs451.DataPacket.Message;
import cs451.milestone2.URBandFIFOBroadcast;
import cs451.CallbackMethods.Forward;
import cs451.CallbackMethods.BroadcastForward;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The Process class encapsulates a unique entity in a distributed system, providing mechanisms for message broadcasting and log management.
 * Key features include:
 * - Reliable message broadcasting across the network.
 * - Thread-safe logging and log file management.
 * - Synchronization and thread safety ensured through concurrent data structures, atomic variables, and locks.
 * This class is tailored for distributed environments where efficient and reliable communication, along with accurate logging, are essential.
 */
public class Process implements Forward {
    private final byte processId; // Unique identifier for the process
    private final URBandFIFOBroadcast fifoBroadcast; // Broadcast mechanism for the process
    private final String outputPath; // File path for output logs

    // Concurrent queue for thread-safe logging
    private final ConcurrentLinkedQueue<String> logQueue;
    // Temporary queue for transferring logs before writing to a file
    private ConcurrentLinkedQueue<String> intermediateLogQueue;
    private long count; // A counter variable (its usage is not clear from this snippet)
    // AtomicBoolean to safely check and update the state of 'writing' across threads
    private final AtomicBoolean isWritingToFile;

    // Timer for periodic tasks
    private final Timer logWriteTimer;
    // AtomicBoolean to safely update and check the state of 'sendersStopped'
    private AtomicBoolean checkProcesses;
    private final int messageCount; // Total number of messages to handle
    // Reentrant lock for managing concurrent access to shared resources
    Lock outputLock = new ReentrantLock();

    // Constructor for the Process class
    public Process(byte processId, int port, List<Host> hostList, String outputPath, int messageCount) {
        this.processId = processId;
        this.outputPath = outputPath;
        this.count = 0;
        this.checkProcesses = new AtomicBoolean(false);

        // Set the sliding window size based on the size of the host list:
        // - If the size of hostList is 20 or fewer, the window size is set to 2000.
        // - If the size of hostList is more than 20 but less than 40, the window size is set to 1000.
        // - If the size of hostList is 40 or more, the window size is reduced to 400.
        int slidingWindowSize = hostList.size() <= 20 ? 2000 : (hostList.size() < 40 ? 1000 : 400);

        logQueue = new ConcurrentLinkedQueue<>(); // Initialize a thread-safe queue for storing log messages.
        this.messageCount = messageCount; // Set the total number of messages this process will handle.
        this.isWritingToFile = new AtomicBoolean(false); // Initialize a flag to track the state of log writing activity.

        // Initialize the broadcast mechanism with provided parameters
        this.fifoBroadcast = new URBandFIFOBroadcast(processId, port, hostList, slidingWindowSize, this, messageCount);

        // Setting up a timer task to periodically check and manage logs
        logWriteTimer = new Timer();
        logWriteTimer.schedule(new LogWriterTask(), 4000, 5000); // Schedule with an initial delay of 4 seconds and a subsequent rate of 5 seconds.

    }

    /**
     * LogWriterTask is a TimerTask that periodically checks a log queue and writes logs to a file.
     * It ensures that log writing is performed in a thread-safe manner and only when certain
     * conditions are met, such as the log queue reaching a specified threshold. The class also
     * handles necessary cleanup activities, including triggering garbage collection under certain conditions.
     */
    class LogWriterTask extends TimerTask {

        /**
         * Main method executed by the timer task. Manages the entire log processing workflow.
         */
        @Override
        public void run() {
            try {
                checkAndProcessLogs();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                performCleanup();
            }
        }

        /**
         * Checks if the logs are ready for processing and initiates the process.
         */
        private void checkAndProcessLogs() {
            if (shouldProcessLogs()) {
                Queue<String> logsToWrite = safelyExtractLogs();
                writeLogsToFile(logsToWrite);
            }
        }

        /**
         * Determines whether the current conditions require log processing.
         * @return true if logs should be processed, false otherwise.
         */
        private boolean shouldProcessLogs() {
            return logQueue.size() > 10000 && isWritingToFile.compareAndSet(false, true);
        }

        /**
         * Extracts logs from the main queue safely for writing.
         * @return A queue of logs ready to be written to the file.
         */
        private Queue<String> safelyExtractLogs() {
            outputLock.lock();
            try {
                if (!logQueue.isEmpty()) {
                    intermediateLogQueue = new ConcurrentLinkedQueue<>(logQueue);
                    logQueue.clear();
                }
            } finally {
                outputLock.unlock();
            }
            return intermediateLogQueue;
        }

        /**
         * Writes the provided logs to a file.
         * @param logsToWrite The queue of logs to be written.
         */
        private void writeLogsToFile(Queue<String> logsToWrite) {
            if (logsToWrite != null && !logsToWrite.isEmpty()) {
                try (var outputStream = new FileOutputStream(outputPath, true)) {
                    while (!logsToWrite.isEmpty()) {
                        outputStream.write(logsToWrite.poll().getBytes());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    isWritingToFile.set(false);
                }
            } else {
                isWritingToFile.set(false);
            }
        }

        /**
         * Performs necessary cleanup actions, such as triggering garbage collection.
         */
        private void performCleanup() {
            if (checkProcesses.get()) {
                System.gc();
            }
        }
    }

    /**
     * Initiates the sending of messages and logs the broadcast.
     * It first logs all broadcasts up to the specified message count,
     * then triggers the broadcast to send the messages.
     */
    public void send() {
        // Log all broadcasts up to the message count
        this.logAllBroadcast(this.messageCount);
        // Trigger the broadcast to send messages
        fifoBroadcast.send();
    }

    /**
     * Returns the unique identifier of the process.
     *
     * @return The process ID as an integer.
     */
    public int getId() {
        // Casting byte ID to int to match the return type
        return processId & 0xFF;
    }

    /**
     * Stops the processing by halting the broadcast and cancelling
     * the scheduled log writing task.
     */
    public void stopProcessing() {
        // Stop the broadcast mechanism
        fifoBroadcast.stop();
        // Cancel the scheduled log writing task
        logWriteTimer.cancel();
    }

    /**
     * Starts the processing by initiating the broadcast mechanism.
     */
    public void startProcessing() {
        // Start the broadcast mechanism
        fifoBroadcast.start();
    }

    /**
     * Writes the accumulated logs to the output file.
     * This method flushes both the main log queue and the intermediate log queue.
     */
    public void writeOutput() {
        try (var outputStream = new FileOutputStream(outputPath, true)) {
            // Write logs from the main log queue to the file
            while (!logQueue.isEmpty()) {
                outputStream.write(logQueue.poll().getBytes());
            }
            // Check if intermediate logs exist and write them to the file
            if (intermediateLogQueue != null) {
                while (!intermediateLogQueue.isEmpty()) {
                    outputStream.write(intermediateLogQueue.poll().getBytes());
                }
            }
        } catch (IOException e) {
            // Print the stack trace in case of an IOException
            e.printStackTrace();
        }
    }

    /**
     * Logs all broadcast messages up to a specified count in a thread-safe manner.
     *
     * @param messageCount The total number of messages to log.
     */
    private void logAllBroadcast(int messageCount) {
        // Acquire the lock to ensure thread-safe operation
        outputLock.lock();
        try {
            // Log each message from 1 to messageCount
            for (int i = 1; i <= messageCount; i++) {
                logQueue.add("b " + i + "\n");
            }
        } finally {
            // Ensure the lock is always released
            outputLock.unlock();
        }
    }

    /**
     * Checks if the process is currently writing to the output.
     *
     * @return True if writing is in progress, false otherwise.
     */
    public Boolean isWriting() {
        // Retrieve and return the current state of the 'writing' flag
        return isWritingToFile.get();
    }

    /**
     * Handles the delivery of a message. It logs the delivery,
     * increments the message count, and prints a status message
     * every 5000 messages.
     *
     * @param message The message to be delivered.
     */
    public void forward(Message message) {
        // Acquire the lock to ensure thread-safe operation
        outputLock.lock();
        try {
            // Log the delivery of the message
            logQueue.add("d " + (message.getOriginalSender() + 1) + " " + message.getId() + "\n");
        } finally {
            // Ensure the lock is always released
            outputLock.unlock();
        }
        // Increment the count of received messages
        count += 1;
    }

}
