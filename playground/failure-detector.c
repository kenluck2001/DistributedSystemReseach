/*
  "Hello World" MPI Test Program
*/
// Works for equal number of roles (CLIENT, PROPOSER, ACCEPTOR, LEARNER)
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <stddef.h> // used for offsetof
#include <time.h>
#include <math.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define EMPTY -999
#define LIST_SIZE 100
#define NUM_OF_ELEM 6


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
    const int nRole = 4;
    int my_rank, num_procs;
    /* Initialize the infrastructure necessary for communication */
    MPI_Init(&argc, &argv);

    /* Identify this process */
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    /* Find out how many total processes are active */
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    if (num_procs != 4) {
    //if ((num_procs % nRole) != 0) {
        fprintf(stderr, "Must use multiple of 4 processes for this example\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];
    enum msgTag {mHEATBEAT};
    double start, end, gamma, delta, duration;

    int cnt, flag, ret, threshold;

    for (int cur_rank = 0; cur_rank < num_procs; cur_rank++)
    {
        //if (my_rank == 0) 
        if (my_rank == cur_rank) 
        {
            enum msgTag ctag;
            ctag = mHEATBEAT;
            int msg[num_procs];
            gamma = 10; // in secs
            delta = 0.5; // in secs;
            duration = 0;

            while (1)
            { 
                memset( msg, 0, num_procs * sizeof(int) );

                sleep(10); 
                for (int other_rank = 0; other_rank < num_procs; other_rank++)
                {
                    if (my_rank != other_rank) 
                    {
                        MPI_Isend(msg, num_procs, MPI_INT, other_rank, ctag, MPI_COMM_WORLD, &requests[other_rank]);
                    }
                }
                //printf("broadcast started \n");
                printArray(my_rank, msg, num_procs);

                cnt =0;
                flag = -1;
                ret;
                threshold = num_procs;
                

                int recv[num_procs];
                memset( recv, 0, num_procs * sizeof(int) );

                start = MPI_Wtime();
                while (1)
                { 
                    /* Receive message from any process */
                    if(flag != 0)
                    {
                        ret = MPI_Irecv(&recv, num_procs, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &requests[my_rank]);

                        flag = 0;
                    }
                    MPI_Test(&requests[my_rank], &flag, &status[my_rank]);
                    if (flag != 0)
                    {
                        //end = MPI_Wtime();
                        if (ret == MPI_SUCCESS )
                        {
                            end = MPI_Wtime();
                            duration = end - start;
                            printf("duration: %f\n", duration);

                            enum msgTag tag = status[my_rank].MPI_TAG;
                            int source = status[my_rank].MPI_SOURCE;

                            // detect a delayed process as suspected failure
                            if (duration > delta)
                            {
                                recv[source] = 1;
                            }
                            else
                            {
                                recv[source] = 0;
                            }


                            for (int other_rank = 0; other_rank < num_procs; other_rank++)
                            {
                                if (my_rank != other_rank) 
                                {
                                    MPI_Isend(recv, num_procs, MPI_INT, other_rank, ctag, MPI_COMM_WORLD, &requests[other_rank]);
                                }
                            }

                            //printf("another broadcast started \n");
                            cnt += 1;

                        }
                        flag = -1;
                    }

                    if (cnt== threshold)
                    {
                        break;
                    }                    
                } 
            } 
        }           
    } 
    MPI_Finalize();
    return 0;
}


