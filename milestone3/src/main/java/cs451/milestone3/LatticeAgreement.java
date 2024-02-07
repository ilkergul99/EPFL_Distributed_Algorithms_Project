package cs451.milestone3;

import cs451.Process;
import cs451.CallbackMethods.*;
import cs451.Host;
import cs451.DataPacket.Message;
import cs451.milestone1.PerfectChannel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * The LatticeAgreement class implements the LatticeForward interface,
 * providing functionality for reaching agreement in a distributed system
 * using lattice-based consensus. This class is responsible for managing
 * the process of proposing, deciding, and achieving consensus on values
 * across different nodes in a distributed network, ensuring that all
 * participating nodes eventually agree on the same value or set of values
 * despite potential failures or message delays.
 */
public class LatticeAgreement implements LatticeForward {

    private final AtomicIntegerArray activeProposalNumber;  // An array to keep track of the active proposal numbers in a thread-safe manner.
    private final List<Host> hosts; // List of all the hosts (nodes) participating in the distributed system.
    private final AtomicInteger lastDecidedLatticeRound;    // Stores the last completed round number in the lattice agreement process.
    private final int latticeRoundCount;    // The total number of rounds in the lattice agreement process.
    private final AtomicInteger maxLatticeRound;    // Keeps track of the maximum round number reached in the lattice agreement.
    private final PerfectChannel perfectChannel;    // A communication channel that is assumed to be reliable and error-free.
    private final int parallelRoundCount;   // The number of rounds that can be executed in parallel in the algorithm.
    private final Process currentProcess;  // Represents the current process with its own execution logic and state.
    private final byte processId;   // Identifier for this process, unique within the distributed system.
    private final Set<Integer>[] proposals; // An array of sets, each containing the proposal numbers made by the processes.
    private final Set<Byte>[] proposalDeliveredAcks;    // An array of sets, each storing acknowledgments for delivered proposals.
    private final boolean readyToDeliver[]; // An array indicating whether each process is ready to deliver its message.

    /**
     * Constructs a new LatticeAgreement instance, configuring it with necessary parameters
     * for participating in a lattice-based consensus algorithm within a distributed system.
     * This constructor sets up the agreement protocol with network settings, process information,
     * and specific parameters required for managing the consensus process.
     *
     * @param processId The unique identifier for the current process.
     * @param port The network port number used for inter-process communication.
     * @param hostList A list of all hosts (or processes) participating in the distributed system.
     * @param currentProcess The Process instance that this LatticeAgreement is associated with.
     * @param parallelRoundCount The number of agreement rounds that can be executed in parallel.
     * @param teklifSize The size of the proposal set used in each round of the agreement.
     * @param latticeRoundCount The total number of rounds in the lattice agreement process.
     */
    public LatticeAgreement(byte processId, int port, List<Host> hostList, Process currentProcess,
                            int parallelRoundCount, int teklifSize, int latticeRoundCount) {
        // An atomic array to keep track of the active proposal numbers in a thread-safe manner.
        this.activeProposalNumber = new AtomicIntegerArray(parallelRoundCount);

        // The list of all the hosts (nodes) participating in the distributed system.
        this.hosts = hostList;

        // Create a HashMap to map process IDs to their respective Host objects for easy lookup.
        HashMap<Byte, Host> hostMap = new HashMap<>();
        for (Host host : hostList) {
            // Populate the hostMap with host IDs as keys and Host objects as values.
            hostMap.put((byte) host.getId(), host);
        }

        // The instance of Process associated with this LatticeAgreement.
        this.currentProcess = currentProcess;

        // The count of the last decided round in the lattice agreement process.
        this.lastDecidedLatticeRound = new AtomicInteger(0);

        // The maximum round number reached in the lattice agreement.
        this.maxLatticeRound = new AtomicInteger(parallelRoundCount);

        // Initialize the perfectChannel with network settings and other parameters necessary for communication.
        this.perfectChannel = new PerfectChannel(port, processId, this, hostMap, parallelRoundCount, teklifSize);

        // The number of rounds that can be executed in parallel in the algorithm.
        this.parallelRoundCount = parallelRoundCount;

        // The unique identifier for the current process.
        this.processId = processId;

        // An array indicating whether each process is ready to deliver its message.
        this.readyToDeliver = new boolean[latticeRoundCount];

        // An array of sets, each storing the proposal numbers made by the processes.
        this.proposals = new Set[latticeRoundCount];

        // An array of sets, each storing acknowledgments for delivered proposals.
        this.proposalDeliveredAcks = new HashSet[latticeRoundCount];

        // The total number of rounds in the lattice agreement process.
        this.latticeRoundCount = latticeRoundCount;

        for (int i = 0; i < latticeRoundCount; i++) {
            // Initialize each index of the proposals array with a new concurrent hash set for thread-safe operations.
            this.proposals[i] = ConcurrentHashMap.newKeySet();

            // Initially set all rounds as not ready to deliver.
            this.readyToDeliver[i] = false;

            // Initialize each index of the proposalDeliveredAcks array with a new hash set to store acknowledgments.
            this.proposalDeliveredAcks[i] = new HashSet<>();
        }

    }

    /**
     * Distributes the set of proposals for a given lattice round to all participating hosts.
     * This method increments the proposal number for the round, updates the round's proposals,
     * and sends them to each host in the distributed system, excluding the host itself.
     *
     * @param round The lattice round for which the proposals are being distributed.
     * @param proposals The set of proposals to be distributed for the round.
     */
    public void distributeRoundProposals(int round, Set<Integer> proposals){
        // Increment the active proposal number for the current round, ensuring thread safety.
        var proposalNumber = activeProposalNumber.incrementAndGet(round % parallelRoundCount);

        // Add all new proposals to the set of proposals for the current round.
        this.proposals[round].addAll(proposals);

        // Retrieve the current set of proposals for the given round.
        var currentProposals = this.getProposal(round);

        // Iterate over all hosts in the distributed system.
        for(Host host : this.hosts){
            // Skip sending the message to the host itself (no need to send a message to oneself).
            if(host.getId() == processId) continue;

            // Construct a new message with the current proposal number, process ID, recipient's host ID, round, and current proposals.
            perfectChannel.send(new Message(proposalNumber, processId, (byte)host.getId(), round, currentProposals), host);
        }
    }

    /**
     * Sends a 'delivered' notification for a specified round to all hosts except the current process.
     *
     * @param round The round number for which the delivered status is being broadcasted.
     */
    public void notifyDelivery(int round) {
        for (Host targetHost : this.hosts) {
            // Skip sending the message to the process itself.
            if (targetHost.getId() == processId) continue;

            // Create a new message indicating the delivery status and send it to the target host.
            Message deliveryMessage = new Message(1, processId, (byte) targetHost.getId(), round, false);
            perfectChannel.send(deliveryMessage, targetHost);
        }
    }

    /**
     * Activates the perfectChannel.
     * This method should be invoked to begin operations of the perfectChannel.
     */
    public void start(){
        perfectChannel.start();
    }

    /**
     * Terminates the perfectChannel.
     * This method should be called to cease operations of the perfectChannel.
     */
    public void stop(){
        perfectChannel.stop();
    }

    /**
     * Retrieves the active proposal number for a specific lattice round.
     * This method accounts for parallel execution by using the modulus operation
     * with the parallel round count to determine the correct proposal number.
     *
     * @param latticeRound The lattice round for which the active proposal number is requested.
     * @return The active proposal number for the given round.
     */
    @Override
    public int fetchCurrentProposalNumber(int latticeRound) {
        // Returns the active proposal number for the given lattice round, considering parallel round execution.
        return this.activeProposalNumber.get(latticeRound % parallelRoundCount);
    }


    /**
     * Retrieves an immutable copy of the set of proposals for a specified lattice round.
     * This method is useful for operations that require a read-only view of the proposals.
     *
     * @param latticeRound The lattice round for which to retrieve the proposals.
     * @return An immutable copy of the set of proposals for the specified round.
     */
    @Override
    public Set<Integer> getCopyOfProposal(int latticeRound){
        return Set.copyOf(this.proposals[latticeRound]);
    }

    /**
     * Finalizes the decision on a value or proposal for a given lattice round.
     * This method is key in concluding the consensus process for the specified round,
     * ensuring that all participating nodes agree on a specific value or set of values.
     *
     * @param latticeRound The specific round of the lattice agreement process for which the decision is being finalized.
     */
    @Override
    public void finalizeRoundDecision(int latticeRound) {
        // Reset the active proposal number for the given lattice round.
        activeProposalNumber.set(latticeRound % parallelRoundCount, 0);

        // Mark the current round as ready to deliver.
        this.readyToDeliver[latticeRound] = true;

        // Continue processing as long as the last decided round is less than the total lattice round count
        // and the round is marked as ready to deliver.
        while (lastDecidedLatticeRound.get() < latticeRoundCount && readyToDeliver[lastDecidedLatticeRound.get()]) {
            // Retrieve and increment the last decided lattice currentRound number.
            int currentRound = lastDecidedLatticeRound.getAndIncrement();

            // Log the proposal set for the current currentRound in the current process.
            currentProcess.logProposalSet(currentRound, this.proposals[currentRound]);

            // Notify all relevant entities about the delivery for this currentRound.
            notifyDelivery(currentRound);

            // Check for completed proposals that can be cleared to free up resources.
            checkForDeletion();

            // Increment the max lattice currentRound, allowing the next currentRound to be broadcast.
            maxLatticeRound.incrementAndGet();
        }
    }

    /**
     * Retrieves the current set of proposals for a specified lattice round.
     * This method provides access to the modifiable set of proposals.
     *
     * @param latticeRound The lattice round for which to retrieve the proposals.
     * @return The set of proposals for the specified round.
     */
    @Override
    public Set<Integer> getProposal(int latticeRound) {
        return this.proposals[latticeRound];
    }

    /**
     * Sets or updates the proposals for a given lattice round.
     * This method is used to modify the current set of proposals for the specified round.
     *
     * @param proposals The set of proposals to be set for the round.
     * @param latticeRound The round for which the proposals are to be set.
     */
    @Override
    public void setProposals(Set<Integer> proposals, int latticeRound) {
        this.proposals[latticeRound] = proposals;
    }


    /**
     * Distributes the new proposal to all participating hosts for a given lattice round.
     *
     * @param latticeRound The current lattice round for which the proposal is being broadcast.
     */
    @Override
    public void sendNewRoundProposal(int latticeRound) {
        // Obtain the next proposal number based on the lattice round.
        int nextProposalNum = activeProposalNumber.incrementAndGet(latticeRound % parallelRoundCount);
        Set<Integer> proposalsForRound = this.getProposal(latticeRound);

        // Loop through each host in the network.
        for (Host targetHost : this.hosts) {
            // Exclude the current process from receiving its own message.
            if (targetHost.getId() == processId) continue;

            // Construct a new message with the proposal and send it to each host.
            Message proposalMessage = new Message(nextProposalNum, processId, (byte) targetHost.getId(), latticeRound, proposalsForRound);
            perfectChannel.send(proposalMessage, targetHost);
        }
    }

    @Override
    public int getMaxLatticeRound() {
        // Returns the maximum lattice round that has been reached so far.
        // This value is important to understand the progress of the lattice agreement process.
        return this.maxLatticeRound.get();
    }

    @Override
    public int getMinLatticeRound() {
        // Returns the minimum lattice round that is currently being considered.
        // This round represents the earliest round in the sequence of rounds still in decision or agreement phase.
        return this.lastDecidedLatticeRound.get();
    }

    /**
     * Checks and returns whether the proposals for the specified lattice round have been delivered.
     *
     * @param latticeRound The current lattice round for which the proposal is being broadcast.
     */
    @Override
    public boolean isDelivered(int latticeRound){
        // A value of 'true' indicates that the round's proposals are ready for further processing or have been processed.
        return this.readyToDeliver[latticeRound];
    }

    /**
     * Increments the acknowledgment count for a delivered proposal for a specific lattice round.
     * This method updates the set of acknowledgments received for the given round.
     *
     * @param latticeRound The current lattice round for which the proposal is being broadcast.
     * @param senderIdentifier Representing the identifier of the node acknowledging the delivery)
     */
    @Override
    public void acknowledgeProposalDelivery(int latticeRound, byte senderIdentifier){
        // Checks if the senderIdentifier (representing the identifier of the node acknowledging the delivery)
        // is successfully added to the set of delivered acknowledgments for the specified round.
        if(this.proposalDeliveredAcks[latticeRound].add(senderIdentifier)){
            // If a new acknowledgment is added, it triggers a check for completed proposals
            // to see if any can be deleted or cleared from memory.
            checkForDeletion();
        }
    }


    /**
     * Iterates through the proposal rounds to identify and clear out completed proposals.
     * A proposal round is considered complete when all hosts, except the originating host, have acknowledged delivery.
     */
    private void checkForDeletion() {
        int decidedRoundThreshold = lastDecidedLatticeRound.get(); // Get the threshold round up to which to check.
        int requiredAcks = hosts.size() - 1; // Calculate required acknowledgements.

        // Loop through each round up to the last decided lattice round
        for (int currentRound = 0; currentRound < decidedRoundThreshold; currentRound++) {
            // Retrieve acknowledgements and proposals for the current round
            Set<Byte> currentRoundAcks = this.proposalDeliveredAcks[currentRound];
            Set<Integer> currentRoundProposals = this.proposals[currentRound];

            // Check if all hosts (except the sender) have acknowledged
            if (currentRoundAcks != null && currentRoundAcks.size() == requiredAcks) {
                // Clear proposals if they exist and are not already cleared
                if (currentRoundProposals != null && !currentRoundProposals.isEmpty()) {
                    currentRoundProposals.clear();
                    this.proposals[currentRound] = null; // Free the memory by setting to null
                }

                // Clear the acknowledgements for this round
                currentRoundAcks.clear();
            }
        }
    }
}
