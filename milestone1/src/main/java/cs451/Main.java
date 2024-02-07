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

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host: parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
            if(host.getId() == parser.myId()){
                dist_process = new Process(host.getId(), host.getPort(), parser.hosts(), parser.output());
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
        // Read configuration and parse the first line.
        int[] configValues;
        try {
            configValues = readConfigAndParse(parser);
        } catch (RuntimeException e) {
            e.printStackTrace(); 
            return; // Exit if configuration could not be read.
        }
        // The first integer represents the number of messages,
        // and the second integer represents the delivery target ID.
        int numberOfMessages = configValues[0];
        int deliveryTargetId = configValues[1];
        System.out.println("Broadcasting and delivering messages...\n");
        // Check if the current process's ID is not the delivery target.
        if (dist_process.getId() != deliveryTargetId) {
            // If the current process is not the target, print the information and send the messages.
            dist_process.send(numberOfMessages, deliveryTargetId); // Cast to byte
        } else {
            // If the current process is the target, set the message count accordingly.
            dist_process.setMessageCount(numberOfMessages);
        }

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
        while (true) {
            // Sleep for 1 hour
            Thread.sleep(60 * 60 * 1000);
        }
    }

    /**
     * Reads the first line from the configuration file and parses it to get the number of messages
     * and the delivery target ID.
     *
     * @param parser The parser object to obtain the configuration file path.
     * @return An array where the first element is the number of messages and the second is the delivery target ID.
     * @throws RuntimeException If an I/O error occurs or the configuration is malformed.
     */
    public static int[] readConfigAndParse(Parser parser) {
        String firstLine;
        try {
            // Get the configuration file path from the parser.
            String configFilePath = parser.config();

            // Read the first line from the configuration file.
            try (BufferedReader configReader = new BufferedReader(new FileReader(configFilePath))) {
                firstLine = configReader.readLine();
                if (firstLine == null || firstLine.trim().isEmpty()) {
                    throw new IOException("Configuration file is empty or the first line is blank.");
                }
            }
        } catch (IOException e) {
            // Rethrow with additional context.
            throw new RuntimeException("Failed to read the first line from the configuration file.", e);
        }

        // Parse the number of messages and delivery target ID.
        int spaceIndex = firstLine.indexOf(" ");
        if (spaceIndex == -1) {
            throw new RuntimeException("The configuration line is malformed. It should contain two integers separated by space.");
        }

        int numberOfMessages = Integer.parseInt(firstLine.substring(0, spaceIndex));
        int deliveryTargetId = Integer.parseInt(firstLine.substring(spaceIndex + 1));

        return new int[]{numberOfMessages, deliveryTargetId};
    }
}