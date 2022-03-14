/*
  "Hello World" MPI Test Program
*/
//https://en.wikipedia.org/wiki/Message_Passing_Interface#Example_program
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
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

//int rounds[100] = {1};
//memset( rounds, 1, 100 * sizeof(int) );

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
    int rounds[num_procs];
    memset( rounds, 1, num_procs * sizeof(int) );
               
    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];
    int request_complete = 0;

    //rounds[num_procs - 1] = 0;
    int flag = 0;

    printf("You are in rank: %i processes.\n", my_rank );
    if (my_rank == 0) {

        int num_received;
        int other_rank;
        int rsource, rdestination;
        printf("We have %i processes.\n", num_procs);
        int rounds[num_procs];

        int cnt =0;
        int flag = -1;
        int ret;

        //memset( rounds, 2, num_procs * sizeof(int) );

        //MPI_Ibcast(rounds, num_procs, MPI_INT, 0, MPI_COMM_WORLD,  &requests[num_procs]);            

        memset( lamport_vec, 0, num_procs * sizeof(int) );

        lamport_vec[my_rank] = lamport_vec[my_rank] + 1; 
        

        for (other_rank = 0; other_rank < num_procs; other_rank++)
        {
            MPI_Isend(lamport_vec, num_procs, MPI_INT, other_rank, my_rank, MPI_COMM_WORLD, &requests[other_rank]);
        }

        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(lamport_vec, num_procs, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    int source = status[my_rank].MPI_SOURCE;
                    printf("source: %d\n", source);
                    lamport_vec[my_rank] = MAX(lamport_vec[my_rank], lamport_vec[source]) + 1;             
                    printArray(my_rank, lamport_vec, num_procs);
                    if (source != my_rank)
                    MPI_Isend(lamport_vec, num_procs, MPI_INT, source, 0, MPI_COMM_WORLD, &requests[source]);
                        
                }
                cnt += 1;
                flag = -1;                
            }
              
            if (cnt== num_procs)
                break;
                
        }
    } else {
        int cnt =0;
        int flag = -1;
        int ret;
        /* Receive message from any process for telemetry */
        memset( lamport_vec, 0, num_procs * sizeof(int) );
        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(lamport_vec, num_procs, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    int source = status[my_rank].MPI_SOURCE;
                    lamport_vec[my_rank] = MAX(lamport_vec[my_rank], lamport_vec[source]) + 1;             
                    //printArray(my_rank, lamport_vec, num_procs);
                    MPI_Isend(lamport_vec, num_procs, MPI_INT, source, 0, MPI_COMM_WORLD, &requests[source]);
                }
                cnt += 1;
                flag = -1;
            }
            //printf("cnt: %d\n", cnt);
            if (cnt== 1)
                break;
                
        }            
    }
    //MPI_Waitall(num_procs, requests, status);
    
    /* Tear down the communication infrastructure */
    MPI_Finalize();
    return 0;
}


/**
MPI_Isend and MPI_Irecv MUST be followed at some point by MPI_Test and MPI_Wait. The process sending should never write in the buffer until the request has been completed. On the other hand, the process receiving should never read in the buffer before the request has been completed. And the only way to know if a request is completed, is to call MPI_Wait and MPI_Test.
*/
