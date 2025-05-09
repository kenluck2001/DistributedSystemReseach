/*
Name: Kenneth Emeka Odoh
Basic Heartbeat Failure Detector in Open MPI


How to run the source code
===========================
mpicc failure-detector.c && mpiexec -n 4 ./a.out
mpicc failure-detector.c && mpiexec -n 8 ./a.out
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>  // For sleep

#define HEARTBEAT_TAG 10
#define SUSPECT_TAG 20
#define ALIVE_TAG 30

#define HEARTBEAT_INTERVAL 1  // Send heartbeat every 1 second
#define TIMEOUT_MULTIPLIER \
    3  // Suspect if no heartbeat for TIMEOUT_MULTIPLIER * HEARTBEAT_INTERVAL
#define CHECK_INTERVAL 0.5  // Check for timeouts every 0.5 seconds

typedef struct {
    int rank;
    double last_heartbeat;
    int suspected;
} ProcessState;

int main(int argc, char **argv) {
    int my_rank, num_procs;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    ProcessState *process_states =
        (ProcessState *)malloc(num_procs * sizeof(ProcessState));
    for (int i = 0; i < num_procs; ++i) {
        process_states[i].rank = i;
        process_states[i].last_heartbeat =
            MPI_Wtime();  // Initialize with current time
        process_states[i].suspected = 0;
    }

    MPI_Request send_request;
    MPI_Status status;
    int heartbeat_msg = 1;
    double last_send_time = MPI_Wtime();

    while (1) {
        // Send heartbeat
        double current_time = MPI_Wtime();
        if (current_time - last_send_time >= HEARTBEAT_INTERVAL) {
            for (int i = 0; i < num_procs; ++i) {
                if (i != my_rank) {
                    MPI_Isend(&heartbeat_msg, 1, MPI_INT, i, HEARTBEAT_TAG,
                              MPI_COMM_WORLD, &send_request);
                }
            }
            last_send_time = current_time;
        }

        // Receive messages (heartbeats, suspicions, alives)
        int flag = 0;
        int recv_msg;
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
        if (flag) {
            MPI_Recv(&recv_msg, 1, MPI_INT, status.MPI_SOURCE, status.MPI_TAG,
                     MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (status.MPI_TAG == HEARTBEAT_TAG) {
                process_states[status.MPI_SOURCE].last_heartbeat = MPI_Wtime();
                process_states[status.MPI_SOURCE].suspected =
                    0;  // Process is alive
                if (my_rank == 0) {
                    printf("Rank %d received heartbeat from %d\n", my_rank,
                           status.MPI_SOURCE);
                }
            } else if (status.MPI_TAG == SUSPECT_TAG) {
                process_states[status.MPI_SOURCE].suspected = 1;
                if (my_rank == 0) {
                    printf("Rank %d received suspicion about %d\n", my_rank,
                           status.MPI_SOURCE);
                }
            } else if (status.MPI_TAG == ALIVE_TAG) {
                process_states[status.MPI_SOURCE].suspected = 0;
                if (my_rank == 0) {
                    printf("Rank %d received alive message from %d\n", my_rank,
                           status.MPI_SOURCE);
                }
            }
        }

        // Check for timeouts
        for (int i = 0; i < num_procs; ++i) {
            if (i != my_rank && !process_states[i].suspected) {
                if (current_time - process_states[i].last_heartbeat >
                    HEARTBEAT_INTERVAL * TIMEOUT_MULTIPLIER) {
                    process_states[i].suspected = 1;
                    if (my_rank == 0) {
                        printf("Rank %d suspects Rank %d (timeout)\n", my_rank,
                               i);
                    }
                    // Optionally broadcast suspicion (simple all-to-all for
                    // now)
                    for (int j = 0; j < num_procs; ++j) {
                        if (j != my_rank) {
                            MPI_Send(&i, 1, MPI_INT, j, SUSPECT_TAG,
                                     MPI_COMM_WORLD);
                        }
                    }
                }
            }
        }

        // Introduce a small delay to avoid busy-waiting
        usleep((int)(CHECK_INTERVAL * 1000000));
    }

    free(process_states);
    MPI_Finalize();
    return 0;
}
