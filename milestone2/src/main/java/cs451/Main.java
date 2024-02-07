package cs451;

import java.io.*;
import java.util.List;

public class Main {

    static Process dist_process;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        dist_process.stopProcessing();

        //write/flush output file if necessary
        System.out.println("Writing output.");
        while(dist_process.isWriting()){} // Wait for the current writing to finish
        dist_process.writeOutput();
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");
        System.out.println("OUTPUT:  " + parser.output());
        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        // Retrieve a list of Host objects from the parser.
        // Each Host object represents a node in the distributed system.
        List<Host> distributedSystemNodes = parser.hosts();

        // Adjust the IDs of each host in the list.
        // This is necessary to fit the host IDs within the byte data type range,
        // as initially they were represented as integers.
        adjustHostIds(distributedSystemNodes);

        // The integer represents the number of messages
        int numberOfMessages = readNumberOfMessages(parser);
        for (Host host: distributedSystemNodes) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
            if(host.getId() == (parser.myId()-1)){
                dist_process = new Process((byte)host.getId(), host.getPort(), distributedSystemNodes, parser.output(), numberOfMessages);
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
        dist_process.startProcessing();

        System.out.println("Broadcasting and delivering messages...\n");
        // Initiate the sending of messages or data from the distributed process.
        // This method triggers the process's logic to transmit information to other nodes in the system.
        dist_process.send();

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    /**
     * Reads the first line from a configuration file and parses it into an integer.
     * The configuration file path is obtained from the provided parser.
     *
     * @param parser The parser object used to obtain the configuration file path.
     * @return The parsed integer from the first line of the configuration file.
     * @throws RuntimeException if there's an issue reading the file or parsing the integer.
     */
    public static int readNumberOfMessages(Parser parser) {
        String firstLine;
        try (BufferedReader configReader = new BufferedReader(new FileReader(parser.config()))) {
            firstLine = configReader.readLine();
            if (firstLine == null) {
                throw new IOException("Configuration file is empty.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read the first line from the configuration file.", e);
        }

        try {
            return Integer.parseInt(firstLine);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Failed to parse the number of messages from the configuration file.", e);
        }
    }

    /**
     * Adjusts the IDs of a list of Host objects by decrementing each ID by 1.
     * This adjustment is particularly important due to a change in the data type used for IDs.
     * In the first milestone, the ID values were represented as integers. However, for memory optimization,
     * these IDs are being converted from 'int' to 'byte'. Since the maximum value for a byte in Java is 127,
     * decrementing each ID by 1 helps in preventing overflow, especially considering the maximum ID is 128.
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


}
