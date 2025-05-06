/*****************************************************************
Name: Kenneth Emeka Odoh
This is a demonstrations of how to use a lamport clock to catch happen-before
event in messages sents over the channel.

How to run the source code
===========================
mpicc lamport1.c && mpiexec -n 4 ./a.out

*****************************************************************/

#include <assert.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

void printArray(int rank, int *array, int size) {
    printf("rank: %d) ", rank);
    printf("[");
    for (int loop = 0; loop < size; loop++) printf("%d ", array[loop]);
    printf("]");
    printf("\n");
}

int main(int argc, char **argv) {
    int my_rank, num_procs;
    /* Initialize the infrastructure necessary for communication */
    MPI_Init(&argc, &argv);

    /* Identify this process */
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    /* Find out how many total processes are active */
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    /* Until this point, all programs have been doing exactly the same.
       Here, we check the rank to distinguish the roles of the programs */

    int lamport_vec[num_procs];

    if (my_rank == 0) {
        int other_rank;
        int rsource, rdestination;
        printf("We have %i processes.\n", num_procs);

        memset(lamport_vec, 0, num_procs * sizeof(int));
        lamport_vec[my_rank] = lamport_vec[my_rank] + 1;
        for (other_rank = 0; other_rank < num_procs; other_rank++) {
            MPI_Send(lamport_vec, num_procs, MPI_INT, other_rank, my_rank,
                     MPI_COMM_WORLD);
        }

        /* Receive messages from all other process */
        for (other_rank = 0; other_rank < num_procs; other_rank++) {
            MPI_Recv(lamport_vec, num_procs, MPI_INT, other_rank, 0,
                     MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            printArray(other_rank, lamport_vec, num_procs);
        }

    } else {
        MPI_Status status;
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        int tag = status.MPI_TAG;
        int source = status.MPI_SOURCE;

        /* Receive message from any process */
        MPI_Recv(lamport_vec, num_procs, MPI_INT, source, tag, MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);

        /* Send message to process #0 */
        lamport_vec[my_rank] =
            MAX(lamport_vec[my_rank], lamport_vec[source]) + 1;  // time

        /* Receive message from any process for telemetry */
        MPI_Send(lamport_vec, num_procs, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }

    /* Tear down the communication infrastructure */
    MPI_Finalize();
    return 0;
}
