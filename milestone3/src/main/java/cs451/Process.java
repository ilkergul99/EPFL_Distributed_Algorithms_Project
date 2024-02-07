package cs451;

import cs451.milestone3.LatticeAgreement;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The Process class represents a single process instance in a distributed system.
 * It uniquely identifies each process, handles the lattice agreement protocol,
 * manages logging of proposals to a file, and ensures thread-safe operations.
 * This class encapsulates the logic for:
 * - Maintaining a unique identifier for the process.
 * - Handling the intricacies of the lattice agreement protocol through the LatticeAgreement object.
 * - Writing log entries to a specified output path in a thread-safe manner.
 * - Storing log entries in a concurrent queue for processing and eventual persistence to the file system.
 * - Managing a timer to schedule periodic tasks for writing logs.
 * - Utilizing a lock to ensure thread safety when accessing and modifying log-related data structures.
 */
public class Process {
    // Flag to indicate whether the process is currently writing to the file.
    private final AtomicBoolean isWritingToFile;

    // Lock to ensure thread-safe operations on the log queue.
    Lock loggingLock = new ReentrantLock();

    // Handles the lattice agreement protocol for this process.
    private final LatticeAgreement latticeAgreement;

    // Timer to schedule periodic log writing tasks.
    private final Timer logWriteTimer;

    // Path to the file where logs will be written.
    private final String outputPath;

    // Unique identifier for this process instance.
    private final byte processId;

    // Queue for storing log entries before processing.
    private final ConcurrentLinkedQueue<String> proposalLogQueue;

    // Temporary queue for logs that are ready to be written to file.
    private ConcurrentLinkedQueue<String> temporaryLogQueue;

    /**
     * Constructs a new Process instance with specified configurations.
     * This constructor initializes the process with a unique identifier, network configurations,
     * a list of participating hosts, and parameters necessary for the lattice agreement protocol and logging.
     *
     * @param processId The unique identifier for this process instance.
     * @param port The network port number used for communications in the distributed system.
     * @param hostList A list of hosts (other processes) participating in the distributed system.
     * @param outputPath The file path where the process will write its log entries.
     * @param parallelRoundCount The number of rounds that can be executed in parallel in the lattice agreement protocol.
     * @param proposalSetSize The size of the proposal set used in each round of the agreement.
     * @param latticeRoundCount The total number of rounds in the lattice agreement process.
     */
    public Process(byte processId, int port, List<Host> hostList, String outputPath,
                   int parallelRoundCount, int proposalSetSize, int latticeRoundCount) {
        // Assigns the provided process ID to the current instance.
        this.processId = processId;

        // Sets the output path for log writing.
        this.outputPath = outputPath;

        // Determines the sliding window size based on the number of hosts.
        // Smaller for larger numbers of hosts to manage resource usage effectively.
        int slidingWindowSize = hostList.size() <= 20 ? 1000 : (hostList.size() < 40 ? 400 : 80);

        // Initializes the concurrent queue for storing log entries.
        proposalLogQueue = new ConcurrentLinkedQueue<>();

        // Sets up an AtomicBoolean to manage the state of whether the process is currently writing to the file.
        this.isWritingToFile = new AtomicBoolean(false);

        // Initializes the LatticeAgreement object with necessary parameters.
        this.latticeAgreement = new LatticeAgreement(processId, port, hostList, this, parallelRoundCount, proposalSetSize, latticeRoundCount);

        // Creates a Timer object for scheduling log writing tasks.
        logWriteTimer = new Timer();

        // Schedules a new LogWriterTask for execution.
        // Starts immediately (0 milliseconds delay) and repeats every 5 seconds (5000 milliseconds).
        logWriteTimer.schedule(new LogWriterTask(), 0, 5000);
    }


    /**
     * LogWriterTask is a TimerTask that periodically checks a log queue and writes logs to a file.
     * It ensures that log writing is performed in a thread-safe manner and only when certain
     * conditions are met, such as the log queue reaching a specified threshold. The class also
     * handles necessary cleanup activities, including triggering garbage collection under certain conditions.
     */
    class LogWriterTask extends TimerTask{
        /**
         * Main method executed by the timer task. Manages the entire log processing workflow.
         */
        @Override
        public void run() {
            try {
                // Initiates the process of checking and writing logs.
                // This method encapsulates the logic to decide whether logs need to be processed
                // and to carry out the processing if necessary.
                checkAndProcessLogs();
            }
            catch (Exception exception) {
                // Catches and handles any exceptions that might occur during the log checking and processing.
                exception.printStackTrace();
            }
        }

        /**
         * Checks if the logs are ready for processing and initiates the process.
         */
        private void checkAndProcessLogs() {
            // Checks whether the logs should be processed.
            // This typically involves checking if certain conditions like queue size or state flags are met.
            if (shouldProcessLogs()) {
                // Attempt to set the isWritingToFile flag to true.
                // This is an atomic operation to ensure that no other thread is currently writing to the file.
                isWritingToFile.compareAndSet(false, true);

                // Extracts logs from the queue in a thread-safe manner.
                // safelyExtractLogs is expected to handle synchronization to prevent concurrent modifications.
                Queue<String> logsToWrite = safelyExtractLogs();

                // Writes the extracted logs to a file.
                // This method should handle opening and closing of the file stream, and ensure that each log entry is written properly.
                writeLogsToFile(logsToWrite);
            }
        }

        /**
         * Determines whether the current conditions require log processing.
         * @return true if logs should be processed, false otherwise.
         */
        private boolean shouldProcessLogs() {
            // Returns true if the log queue has more than 30 items and if no other process is currently writing logs to file.
            return proposalLogQueue.size() > 30 && !isWritingToFile.get();
        }

        /**
         * Extracts logs from the main queue safely for writing.
         * @return A queue of logs ready to be written to the file.
         */
        private Queue<String> safelyExtractLogs() {
            // Locks the loggingLock to ensure exclusive access to the log queue.
            // This is essential to prevent concurrent modifications which could lead to data inconsistencies or crashes.
            loggingLock.lock();
            try {
                // Checks if the proposalLogQueue is not empty.
                // If there are logs present, it creates a new queue (intermediateLogQueue) and copies all the logs to it.
                // Then, it clears the proposalLogQueue to make room for new incoming logs.
                if (!proposalLogQueue.isEmpty()) {
                    temporaryLogQueue = new ConcurrentLinkedQueue<>(proposalLogQueue);
                    proposalLogQueue.clear();
                }
            } finally {
                // Unlocks the loggingLock regardless of how the try block exits (either normally or due to an exception).
                // This ensures that the lock is always released, preventing deadlock situations.
                loggingLock.unlock();
            }
            // Returns the intermediate queue containing the logs ready for writing.
            // This queue is now decoupled from the main log queue, allowing safe writing without blocking log addition.
            return temporaryLogQueue;
        }

        /**
         * Writes the provided logs to a file.
         * @param logsToWrite The queue of logs to be written.
         */
        private void writeLogsToFile(Queue<String> logsToWrite) {
            // Check if the queue is not null and not empty. Proceed only if there are logs to write.
            if (logsToWrite != null && !logsToWrite.isEmpty()) {
                try {
                    // Open a FileOutputStream in append mode.
                    // This allows the method to add new log entries to the end of the existing file.
                    var outputStream = new FileOutputStream(outputPath, true);

                    // Iterate over the queue and write each log entry to the file.
                    while (!logsToWrite.isEmpty()) {
                        // Convert each log entry to bytes and write it to the file.
                        outputStream.write(logsToWrite.poll().getBytes());
                    }
                } catch (IOException ioException) {
                    // Handle any IOExceptions that might occur during file operations.
                    ioException.printStackTrace();
                } finally {
                    // Ensure that the isWritingToFile flag is set to false after the writing operation,
                    // indicating that the file is no longer being written to.
                    isWritingToFile.compareAndSet(true, false);
                }
            }
            else {
                // If the queue is empty or null, simply reset the isWritingToFile flag.
                isWritingToFile.compareAndSet(true, false);
            }
        }

    }

    /**
     * The send method is designed for broadcasting a set of proposalSet along with the consensusRound information
     * in a lattice agreement context. This method abstracts the complexity of the broadcast operation
     * and provides a simple interface for sending proposalSet.
     *
     * Parameters:
     * - consensusRound: An integer representing the current consensusRound in the lattice agreement process.
     * - proposalSet: A Set of Integers, where each integer represents a unique proposal.
     *
     * Note: The actual implementation of how proposalSet are broadcasted is handled by the latticeAgreement
     *       object, allowing for separation of concerns and easier maintenance.
     */
    public void send(int consensusRound, Set<Integer> proposalSet){
        latticeAgreement.distributeRoundProposals(consensusRound, proposalSet);
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
     * Terminates the lattice agreement processing and stops the log writing timer.
     * This method is used to gracefully shut down the process.
     */
    public void terminateProcess(){
        // Calls the stop method on the latticeAgreement object to halt its processing.
        // This is necessary to gracefully terminate any ongoing agreement protocols.
        latticeAgreement.stop();

        // Cancels the logWriteTimer, stopping any scheduled log writing tasks.
        // This helps in ensuring no further log writing occurs after calling this method.
        logWriteTimer.cancel();
    }

    /**
     * Starts the processing by initiating the lattice mechanism.
     */
    public void startProcessing(){
        // Start the lattice mechanism
        latticeAgreement.start();
    }

    /**
     * Writes the accumulated logs to the output file.
     * This method flushes both the main log queue and the intermediate log queue.
     */
    public void prepareOutputFile() {
        try (var outputStream = new FileOutputStream(outputPath, true)) {
            // Write logs from the main log queue to the file
            while(!proposalLogQueue.isEmpty()){
                outputStream.write(proposalLogQueue.poll().getBytes());
            }
            // Check if intermediate logs exist and write them to the file
            if (temporaryLogQueue != null) {
                while (!temporaryLogQueue.isEmpty()) {
                    outputStream.write(temporaryLogQueue.poll().getBytes());
                }
            }
        } catch (IOException e) {
            // Print the stack trace in case of an IOException
            e.printStackTrace();
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
     * The logProposalSet method is responsible for creating a log entry from a set of proposal numbers.
     * It is designed to be used in distributed systems, particularly in consensus algorithms or similar
     * scenarios where tracking and logging proposal numbers is essential.
     *
     * Parameters:
     * - round: An integer representing the current round in the consensus process. In this implementation,
     *          it's not used directly but could be included in future log entries for more detailed tracking.
     * - proposalNumbers: A Set of Integers, each representing a unique proposal number. These numbers are
     *                    central to the consensus process, and logging them helps in monitoring and debugging.
     *
     * Note: The method uses a StringBuilder to efficiently construct the log entry string and a lock to
     *       manage concurrent access to the logging mechanism.
     */
    public void logProposalSet(int round, Set<Integer> proposalNumbers) {
        // Acquire the lock before modifying the log to ensure thread safety.
        loggingLock.lock();

        // Initialize a StringBuilder to create the log entry.
        StringBuilder logEntry = new StringBuilder();

        // Convert the Set of proposal numbers to an array for easy iteration.
        Integer[] proposalArray = proposalNumbers.toArray(new Integer[0]);

        // Iterate over the array of proposal numbers.
        for (int i = 0; i < proposalArray.length; i++) {
            // Append the current proposal number to the log entry.
            logEntry.append(proposalArray[i]);

            // If this is not the last element, append a space for separation.
            if (i < proposalArray.length - 1) {
                logEntry.append(" ");
            }
        }

        // Append a newline character to mark the end of this log entry.
        logEntry.append("\n");

        // Add the constructed log entry to the proposal log queue.
        proposalLogQueue.add(logEntry.toString());

        // Release the lock as the logging operation is complete.
        loggingLock.unlock();
    }

    /**
     * Retrieves the maximum number of rounds for the lattice agreement.
     * This method delegates to the LatticeAgreement instance to obtain the maximum round count.
     *
     * @return The maximum number of lattice rounds as determined by the LatticeAgreement.
     */
    public int getMaxLatticeRound(){
        // Calls the getMaxLatticeRound method on the latticeAgreement object.
        // This returns the maximum number of rounds that the lattice agreement process can take.
        return latticeAgreement.getMaxLatticeRound();
    }
}
