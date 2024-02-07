package cs451;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class Main {

    static Process dist_proc;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        dist_proc.terminateProcess();   // Call the method to terminate the network process.

        //write/flush output file if necessary
        System.out.println("Writing output.");
        while(dist_proc.isWriting()){} // Wait for the current writing to finish
        // Once writing is complete, prepare the output file.
        // This could involve finalizing, flushing buffers, or closing file handles.
        dist_proc.prepareOutputFile();
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    /**
     * Adjusts the IDs of a list of Host objects by decrementing each ID by 1.
     * This change significantly reduces the memory footprint, as a byte occupies far less space than an integer.
     *
     * @param hostList The list of Host objects whose IDs are to be adjusted.
     */
    public static void adjustHostIds(List<Host> hostList) {
        for (Host host : hostList) {
            // Decrementing the host ID to fit within the byte range
            host.setId(host.getId() - 1);
        }
    }

    /**
     * Reads the initial configuration from a BufferedReader and returns the parsed values.
     * Expects the first line of the file to contain three space-separated numbers.
     *
     * @param currentFile The BufferedReader linked to the configuration file.
     * @return An array of integers containing the parsed values.
     * @throws IOException If there's an issue reading from the file or if the first line is empty.
     */
    private static int[] readInitialConfig(BufferedReader currentFile) throws IOException {
        // Read the first line from the BufferedReader.
        String text = currentFile.readLine();

        // Check if the line read is not null (i.e., the file is not empty).
        if (text != null) {
            // Split the read line into an array of strings based on spaces.
            String[] test = text.split(" ");

            // Parse the first number from the split string as latticeRoundCount.
            // It represents the total number of rounds for lattice agreement.
            int latticeRoundCount = Integer.parseInt(test[0]);

            // Parse the second number from the split string as lineCount.
            int lineCount = Integer.parseInt(test[1]);

            // Parse the third number from the split string as proposalSetSize.
            // This indicates the size of the proposal set in each lattice round.
            int proposalSetSize = Integer.parseInt(test[2]);

            // Return an array containing the parsed configuration values.
            return new int[]{latticeRoundCount, lineCount, proposalSetSize};
        } else {
            // If the first line is empty, throw an IOException.
            throw new IOException("Initial configuration line is empty");
        }
    }


    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        BufferedReader latticeFile = null;
        int[] config = new int[3];

        try {
            latticeFile = new BufferedReader(new FileReader(parser.config()));
            config = readInitialConfig(latticeFile);
        } catch (IOException e) {
            e.printStackTrace();
            // Handle exceptions or errors here as needed.
            throw new RuntimeException(e);
        }

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");
        System.out.println("OUTPUT:  " + parser.output());
        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");

        // Each Host object represents a node in the distributed system.
        List<Host> distributedSystemNodes = parser.hosts();
        // Adjust the IDs of each host in the list.
        // This is necessary to fit the host IDs within the byte data type range,
        adjustHostIds(distributedSystemNodes);

        // This represents the total number of rounds for lattice agreement.
        int toplamRoundCount = config[0];
        // This might represent the number of lines (or entries) to be processed in each round.
        int lineCount = config[1];
        // This indicates the size of the proposal set in each lattice round.
        int proposalSetSize = config[2];

        for (Host host: distributedSystemNodes) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
            if(host.getId() == (parser.myId()-1)){
                dist_proc = new Process((byte)host.getId(), host.getPort(), distributedSystemNodes, parser.output(), 8, proposalSetSize,toplamRoundCount);
            }
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("===============");
        System.out.println("Doing some initialization\n");
        dist_proc.startProcessing();
        System.out.println("Broadcasting and delivering messages...\n");

        try {
            // Call the broadcastInRounds function with the initialized BufferedReader,
            // the total number of lattice rounds, and the static dist_proc instance.
            broadcastInRounds(latticeFile, toplamRoundCount, dist_proc);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            // Handle exceptions or errors here as needed.
        } finally {
            // Close resources if necessary, like the BufferedReader.
            if (latticeFile != null) {
                try {
                    latticeFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        while(true){
            Thread.sleep(60 * 60 * 1000);
        }
    }

    /**
     * Manages the process of broadcasting messages in sequential rounds within a distributed system.
     * After each broadcast, the function waits for message delivery acknowledgments.
     * The broadcasting continues until either the maximum number of lattice rounds is reached
     * or the specified count of lattice rounds is completed.
     *
     * @param inputReader BufferedReader to read input data for broadcasting messages.
     * @param totalLatticeRounds The total number of lattice rounds to be processed.
     * @param distributedHandler The handler managing the distributed process.
     * @throws InterruptedException If the thread sleep is interrupted.
     * @throws IOException If an I/O error occurs while reading input.
     */
    private static void broadcastInRounds(BufferedReader inputReader, int totalLatticeRounds, Process distributedHandler)
            throws InterruptedException, IOException {
        int currentRoundIndex = 0; // Tracks the current round of broadcasting.
        boolean continueBroadcasting = true; // Flag to control the broadcasting loop.

        // Loop to manage broadcasting across different rounds.
        while (continueBroadcasting) {
            while (currentRoundIndex < distributedHandler.getMaxLatticeRound()) {
                // Check if the broadcasting has reached the specified total number of rounds.
                if (currentRoundIndex == totalLatticeRounds) {
                    continueBroadcasting = false;
                    break;
                }
                // Read the next line of input which contains the data to be broadcast.
                String inputData = inputReader.readLine();

                // Convert the read line into a set of integers for broadcasting.
                Set<Integer> numberSet = parseNumbers(inputData);

                // Send the set of integers for the current broadcasting round.
                distributedHandler.send(currentRoundIndex, numberSet);
                currentRoundIndex++; // Move to the next round.
            }

            // Pause briefly to allow the system to process messages.
            Thread.sleep(200);
        }
    }

    /**
     * Parses a given string containing space-separated numeric values and converts it into a set of integers.
     * If a value in the string is not a valid integer, it catches the NumberFormatException and logs the error.
     *
     * @param inputLine The string containing the numbers to be parsed.
     * @return A set containing the parsed integers.
     */
    private static Set<Integer> parseNumbers(String inputLine) {
        // Split the input string into an array of strings based on spaces.
        String[] numberStrings = inputLine.split(" ");

        // Initialize an empty set to store the parsed integers.
        Set<Integer> parsedNumbers = new HashSet<>();

        // Iterate over each split string.
        for (String numStr : numberStrings) {
            try {
                // Attempt to parse the string as an integer and add it to the set.
                parsedNumbers.add(Integer.parseInt(numStr));
            } catch (NumberFormatException e) {
                // If parsing fails, print the stack trace of the exception.
                e.printStackTrace(); // This indicates that numStr was not a valid integer.
            }
        }

        // Return the set of parsed integers.
        return parsedNumbers;
    }


}
