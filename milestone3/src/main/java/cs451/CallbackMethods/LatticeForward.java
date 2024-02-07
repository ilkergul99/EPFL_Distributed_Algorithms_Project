package cs451.CallbackMethods;

import java.util.Set;

/**
 * The LatticeForward interface defines the essential operations required for
 * implementing a lattice agreement protocol in a distributed system. This protocol
 * is crucial for achieving consensus among distributed nodes on a sequence of values.
 * The interface provides methods for decision-making, proposal management, broadcasting,
 * and tracking the status of each round in the lattice agreement process. It ensures
 * that distributed nodes can propose, decide, and acknowledge values consistently and
 * reliably across multiple rounds of communication and agreement.
 */
public interface LatticeForward {
    /**
     * Increments the acknowledgment count for a delivered proposal for a specific lattice round.
     *
     * @param latticeRound The lattice round for which the acknowledgment is incremented.
     * @param senderIdentifier The identifier of the sender acknowledging the delivery.
     */
    void acknowledgeProposalDelivery(int latticeRound, byte senderIdentifier);

    /**
     * Finalizes the decision on a value or proposal for a given lattice round.
     * This method is crucial in achieving consensus for the specified round.
     *
     * @param latticeRound The specific round of the lattice agreement process.
     */
    void finalizeRoundDecision(int latticeRound);

    /**
     * Retrieves the active proposal number for a specified lattice round.
     *
     * @param latticeRound The lattice round for which the active proposal number is requested.
     * @return The active proposal number for the given round.
     */
    int fetchCurrentProposalNumber(int latticeRound);

    /**
     * Obtains a copy of the set of proposals for a specific lattice round.
     * Useful for operations that require a non-modifiable view of the proposals.
     *
     * @param latticeRound The round for which to retrieve the proposals.
     * @return A copy of the set of proposals for the specified round.
     */
    Set<Integer> getCopyOfProposal(int latticeRound);

    /**
     * Retrieves the set of current proposals for a specified lattice round.
     *
     * @param latticeRound The round for which to retrieve the proposals.
     * @return The set of proposals for the specified round.
     */
    Set<Integer> getProposal(int latticeRound);

    /**
     * Gets the maximum lattice round number that has been reached so far.
     *
     * @return The maximum lattice round number.
     */
    int getMaxLatticeRound();

    /**
     * Gets the minimum lattice round number that is currently active.
     *
     * @return The minimum lattice round number.
     */
    int getMinLatticeRound();

    /**
     * Checks whether the proposals for a specific lattice round have been delivered.
     *
     * @param latticeRound The lattice round to check for delivery.
     * @return true if the proposals for the round have been delivered, false otherwise.
     */
    boolean isDelivered(int latticeRound);

    /**
     * Broadcasts a new proposal for the specified currentRound to all participating nodes.
     *
     * @param currentRound The currentRound for which a new proposal is to be broadcast.
     */
    void sendNewRoundProposal(int currentRound);

    /**
     * Sets or updates the proposals for a given lattice round.
     *
     * @param proposals The set of proposals to be set for the round.
     * @param latticeRound The round for which the proposals are to be set.
     */
    void setProposals(Set<Integer> proposals, int latticeRound);
}
