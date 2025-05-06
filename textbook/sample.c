/*****************************************************************
Name: Kenneth Emeka Odoh
This is a demonstrations of how to scale a process. 

Note: The user is expected to write the custom logic to use the processes that are spawned from the openmpi runtime or pthreads as the case may be.

This will be run by our controller logic in autoscaling.py

*****************************************************************/
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


int main(int argc, char **argv) {
    int my_rank, num_procs;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    if (my_rank == 0){
        printf("Total number of processes: %d\n", num_procs);
    }

    MPI_Finalize();
    return 0;
}
