/*
  "Hello World" MPI Test Program
*/
//https://en.wikipedia.org/wiki/Message_Passing_Interface#Example_program
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

void printArray(int rank, int *array, int size){
    printf("rank: %d) ", rank);
    printf("[");
    for(int loop = 0; loop < size; loop++)
        printf("%d ", array[loop]);
    printf("]");
    printf("\n");
}



int main(int argc, char **argv)
{
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

    MPI_Request requests;
    MPI_Status status;


    int flag = 0;

    //printf("You are in rank: %i processes.\n", my_rank );
    if (my_rank == 0) {

        //MPI_Request request = requests[my_rank];
        //MPI_Status  status = status[my_rank];
        int num_received;
        int other_rank;
        int rsource, rdestination;
        printf("We have %i processes.\n", num_procs);

        memset( lamport_vec, 0, num_procs * sizeof(int) );
        lamport_vec[my_rank] = lamport_vec[my_rank] + 1; 
        for (other_rank = 0; other_rank < num_procs; other_rank++)
        {
            MPI_Isend(lamport_vec, num_procs, MPI_INT, other_rank, my_rank, MPI_COMM_WORLD, &requests);
        }


        for (other_rank = 0; other_rank < num_procs; other_rank++)
        {
            //MPI_Iprobe(other_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status[my_rank]);
            MPI_Probe(other_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            int tag = status.MPI_TAG;
            int source = status.MPI_SOURCE;
                
            /* Receive message from any process */
            MPI_Irecv(lamport_vec, num_procs, MPI_INT, source, tag, MPI_COMM_WORLD, &requests);
            MPI_Get_count(&status, MPI_INT, &num_received);
            MPI_Wait(&requests, &status);
            printArray(other_rank, lamport_vec, num_procs);
        }

    } else {
        int num_received;

        //MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &flag, &status);
        MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

        int tag = status.MPI_TAG;
        int source = status.MPI_SOURCE;        

       //if(flag)
       //{
        /* Receive message from any process */
        MPI_Irecv(lamport_vec, num_procs, MPI_INT, source, tag, MPI_COMM_WORLD, &requests);

        MPI_Get_count(&status, MPI_INT, &num_received);
        MPI_Wait(&requests, &status);


        lamport_vec[my_rank] = MAX(lamport_vec[my_rank], lamport_vec[source]) + 1;         //time
        //printArray(my_rank, lamport_vec, num_procs);
        //}
        /* Receive message from any process for telemetry */
        for (int other_rank = 0; other_rank < num_procs; other_rank++)
        {            
            MPI_Isend(lamport_vec, num_procs, MPI_INT, other_rank, 0, MPI_COMM_WORLD, &requests);
        }

        //}
    }
    //int ierr = MPI_Waitall(num_procs, requests, status); 




    /* Tear down the communication infrastructure */
    MPI_Finalize();


    return 0;
}


/**
MPI_Isend and MPI_Irecv MUST be followed at some point by MPI_Test and MPI_Wait. The process sending should never write in the buffer until the request has been completed. On the other hand, the process receiving should never read in the buffer before the request has been completed. And the only way to know if a request is completed, is to call MPI_Wait and MPI_Test.
*/
