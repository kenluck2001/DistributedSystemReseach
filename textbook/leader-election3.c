/*****************************************************************
Name: Kenneth Emeka Odoh
This is a demonstrations of leader election

How to run the source code
===========================
mpicc leader-election3.c && mpiexec -n 4 ./a.out
mpicc leader-election3.c && mpiexec -n 8 ./a.out

*****************************************************************/

#include <math.h>
#include <mpi.h>
#include <stddef.h>  // For offsetof
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

// A macro for safe MPI calls with error checking and abortion.
#define SAFE_MPI_CALL(call)                                                \
    do {                                                                   \
        int ret = (call);                                                  \
        if (ret != MPI_SUCCESS) {                                          \
            char error_string[MPI_MAX_ERROR_STRING];                       \
            int length_of_error_string;                                    \
            MPI_Error_string(ret, error_string, &length_of_error_string);  \
            fprintf(stderr, "MPI Error (%s:%d): %s\n", __FILE__, __LINE__, \
                    error_string);                                         \
            MPI_Abort(MPI_COMM_WORLD, ret);                                \
        }                                                                  \
    } while (0)

// Assuming the existence of create_shared_var and atomic counter primitives
// as demonstrated in the Paxos implementation. These would typically use
// MPI Windows for shared memory access and synchronization.

typedef struct {
    int ballot;  // A unique identifier for the election attempt
    int pid;     // Process ID of the candidate
} data;

enum msgTag {
    mSetLeader  // Message tag indicating a leader declaration
};

// Structure for shared counter using MPI Window
typedef struct mpi_counter_t {
    int hostrank;
    int rank;
    int size;
    int *data;
    MPI_Win win;
    int value;  // Local cached value for convenience (not directly shared)
} mpi_counter_t;

// Atomically increments a shared counter using MPI Window operations.
int increment_counter(mpi_counter_t *counter, int inc) {
    int value;
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, counter->hostrank, 0, counter->win);
    MPI_Get(&value, 1, MPI_INT, counter->hostrank, 0, 1, MPI_INT, counter->win);
    value += inc;
    MPI_Put(&value, 1, MPI_INT, counter->hostrank, 0, 1, MPI_INT, counter->win);
    MPI_Win_unlock(counter->hostrank, counter->win);
    return value;
}

// Resets a shared variable to a specified value using MPI Window operations.
int reset_var(mpi_counter_t *counter, int reset_val) {
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, counter->hostrank, 0, counter->win);
    MPI_Put(&reset_val, 1, MPI_INT, counter->hostrank, 0, 1, MPI_INT,
            counter->win);
    MPI_Win_unlock(counter->hostrank, counter->win);
    return reset_val;
}

mpi_counter_t *create_shared_var(MPI_Comm comm, int hostrank) {
    mpi_counter_t *count;

    count = (mpi_counter_t *)malloc(sizeof(mpi_counter_t));
    count->hostrank = hostrank;
    MPI_Comm_rank(comm, &(count->rank));
    MPI_Comm_size(comm, &(count->size));
    count->value = 0;  // Initialize local cached value
    count->data = NULL;
    count->win = MPI_WIN_NULL;

    if (count->rank == hostrank) {
        SAFE_MPI_CALL(
            MPI_Alloc_mem(sizeof(int), MPI_INFO_NULL, &(count->data)));
        count->data[0] = 0;  // Initialize shared value
        SAFE_MPI_CALL(MPI_Win_create(count->data, sizeof(int), sizeof(int),
                                     MPI_INFO_NULL, comm, &(count->win)));
    } else {
        SAFE_MPI_CALL(MPI_Win_create(count->data, 0, 1, MPI_INFO_NULL, comm,
                                     &(count->win)));
    }

    return count;
}

// A simplified placeholder for detecting leader failure. In a real system,
// this would involve a more robust mechanism like heartbeat monitoring,
// which is a common way a failure detector might operate.
int isLeaderFailed(int leader_pid) { return (leader_pid == MPI_PROC_NULL); }

// Initiates an election by broadcasting an election message with a ballot.
void beginElection(data package, int num_procs, MPI_Request requests[],
                   MPI_Datatype mpi_data_type) {
    enum msgTag ctag = mSetLeader;
    printf("Process %d sending election broadcast with ballot %d\n",
           package.pid, package.ballot);
    for (int other_rank = 0; other_rank < num_procs; other_rank++) {
        SAFE_MPI_CALL(MPI_Isend(&package, 1, mpi_data_type, other_rank, ctag,
                                MPI_COMM_WORLD, &requests[other_rank]));
    }
}

mpi_counter_t *
    process_cnts;  // Shared counter to track participation in an election round

// Function responsible for initiating and managing the leader election process.
void leaderElection(int my_rank, int num_procs, int *leader,
                    MPI_Datatype mpi_data_type) {
    process_cnts = create_shared_var(MPI_COMM_WORLD, 0);
    int cnt = 0;  // Local counter for the current election attempt
    int run_loop = 1;
    double duration =
        2.0;  // Example timeout duration for detecting leader failure
    double beg = MPI_Wtime();
    double end = beg;
    int msg_flag;
    MPI_Status status;
    int is_leader_dead =
        (*leader == MPI_PROC_NULL);  // Initially true if no leader is known

    MPI_Request requests[num_procs];

    while (run_loop) {
        is_leader_dead = isLeaderFailed((*leader));
        if (is_leader_dead) {
            beg = MPI_Wtime();
            while ((end - beg) <= duration) {
                end = MPI_Wtime();
                MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD,
                           &msg_flag, &status);
                if (msg_flag)
                    break;  // Break the timeout loop if any message is received
            }

            // Check again if another process has already started an election
            // and potentially updated the leader.
            is_leader_dead = isLeaderFailed((*leader));
            if (is_leader_dead) {
                printf(
                    "Process %d: A leader process (pid=%d) has failed or no "
                    "initial leader\n",
                    my_rank, *leader);
                data package;
                memset(&package, 0, sizeof(package));
                package.ballot =
                    (int)time(NULL);  // Generate a unique ballot based on time
                package.pid = my_rank;

                printf(
                    "Process %d: An election has been triggered with ballot "
                    "%d\n",
                    my_rank, package.ballot);
                beginElection(package, num_procs, requests, mpi_data_type);
            }
        }
        // Increment the shared counter to track the number of processes
        // that have reached this point in the election cycle.
        cnt = increment_counter(process_cnts, 1);
        // Determine if a quorum of processes has participated in this round.
        if (cnt == (int)MAX(((num_procs + 1) / 2.0), 1)) {
            run_loop = 0;  // Exit the election loop once a quorum is reached.
        }
        // MPI_Barrier(MPI_COMM_WORLD); // Removed the barrier inside the loop
    }
    if (process_cnts != NULL && process_cnts->win != MPI_WIN_NULL) {
        SAFE_MPI_CALL(MPI_Win_free(&process_cnts->win));
        free(process_cnts);
    }
}

mpi_counter_t *
    process_max_ballot;  // Shared variable to track the highest ballot received
mpi_counter_t
    *proc_cnts;  // Shared counter for processes responding to an election

// Function for a process to check incoming election messages and potentially
// update the current leader based on the received ballot.
void checkLeader(data recv, int my_rank, int num_procs, int *leader,
                 MPI_Request requests[], MPI_Datatype mpi_data_type) {
    static int max_ballot =
        -10000;  // Static local variable to track the highest ballot seen
    static int cnt =
        0;  // Static local counter for the number of promises received
    enum msgTag ctag;
    data accept_value;
    memset(&accept_value, 0, sizeof(accept_value));

    // If the received ballot is higher than the current maximum, update it.
    if (recv.ballot > max_ballot) {
        max_ballot = recv.ballot;
        accept_value =
            recv;  // Store the data associated with the highest ballot.
    }

    // Increment the shared counter for the number of processes that have
    // responded.
    cnt = increment_counter(proc_cnts, 1);
    int promise_cnt = (int)MAX(((num_procs + 1) / 2.0), 1);  // Quorum size.

    // If a quorum of processes has responded and the current leader is not
    // the process with the highest ballot, then broadcast the new leader.
    if (cnt == promise_cnt && (*leader != accept_value.pid)) {
        ctag = mSetLeader;
        printf(
            "Process %d: Sending broadcast to set leader to %d with ballot "
            "%d\n",
            my_rank, accept_value.pid, accept_value.ballot);
        *leader = accept_value.pid;  // Update the local leader.
        for (int other_rank = 0; other_rank < num_procs; other_rank++) {
            SAFE_MPI_CALL(MPI_Isend(&accept_value, 1, mpi_data_type, other_rank,
                                    ctag, MPI_COMM_WORLD,
                                    &requests[other_rank]));
        }
    }
}

// Function to reset the shared counters and free the MPI Window objects.
void resetElectionCounters() {
    if (process_cnts != NULL && process_cnts->win != MPI_WIN_NULL) {
        SAFE_MPI_CALL(MPI_Win_free(&process_cnts->win));
        free(process_cnts);
        process_cnts = NULL;
    }
    if (proc_cnts != NULL && proc_cnts->win != MPI_WIN_NULL) {
        SAFE_MPI_CALL(MPI_Win_free(&proc_cnts->win));
        free(proc_cnts);
        proc_cnts = NULL;
    }
    if (process_max_ballot != NULL && process_max_ballot->win != MPI_WIN_NULL) {
        SAFE_MPI_CALL(MPI_Win_free(&process_max_ballot->win));
        free(process_max_ballot);
        process_max_ballot = NULL;
    }
}

// Function to initialize the shared counters using MPI Windows.
void initializeElectionCounters(MPI_Comm comm) {
    process_cnts = create_shared_var(comm, 0);
    proc_cnts = create_shared_var(comm, 0);
    process_max_ballot = create_shared_var(comm, -10000);
}

int main(int argc, char **argv) {
    SAFE_MPI_CALL(MPI_Init(&argc, &argv));

    int my_rank;
    int num_procs;
    SAFE_MPI_CALL(MPI_Comm_rank(MPI_COMM_WORLD, &my_rank));
    SAFE_MPI_CALL(MPI_Comm_size(MPI_COMM_WORLD, &num_procs));

    MPI_Datatype mpi_data_type;
    SAFE_MPI_CALL(MPI_Type_create_struct(
        2, (int[]){1, 1},
        (MPI_Aint[]){offsetof(data, ballot), offsetof(data, pid)},
        (MPI_Datatype[]){MPI_INT, MPI_INT}, &mpi_data_type));
    SAFE_MPI_CALL(MPI_Type_commit(&mpi_data_type));

    int leader = MPI_PROC_NULL;  // Initialize leader to null, indicating no
                                 // leader initially.

    initializeElectionCounters(MPI_COMM_WORLD);

    leaderElection(my_rank, num_procs, &leader, mpi_data_type);

    MPI_Barrier(MPI_COMM_WORLD);
    printf("Process %d: Leader after first phase: %d\n", my_rank, leader);

    MPI_Request requests[num_procs];
    MPI_Status status;
    data received_data;

    process_max_ballot = create_shared_var(MPI_COMM_WORLD, -10000);
    proc_cnts = create_shared_var(MPI_COMM_WORLD, 0);

    for (int i = 0; i < num_procs; ++i) {
        if (i != my_rank) {
            SAFE_MPI_CALL(MPI_Irecv(&received_data, 1, mpi_data_type, i,
                                    MPI_ANY_TAG, MPI_COMM_WORLD, &requests[i]));
        }
    }

    for (int i = 0; i < num_procs; ++i) {
        if (i != my_rank) {
            SAFE_MPI_CALL(MPI_Wait(&requests[i], &status));
            if (status.MPI_TAG == mSetLeader) {
                checkLeader(received_data, my_rank, num_procs, &leader,
                            requests, mpi_data_type);
            }
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    printf("Process %d: Final Leader: %d\n", my_rank, leader);

    resetElectionCounters();
    SAFE_MPI_CALL(MPI_Type_free(&mpi_data_type));
    SAFE_MPI_CALL(MPI_Finalize());
    return 0;
}
