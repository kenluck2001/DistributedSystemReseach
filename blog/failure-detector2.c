/****************************************************************
Name: Kenneth Emeka Odoh
This is a failure detector optimised for completeness properites, instead of accuracy property.
The status indicator is set to zeros at the start to denote healthy processes. When failure is suspected, we set the value of the index to 1.

A better version of failure detector that can catch more errors is shown in leader-election2.c

How to run the source code
===========================
mpicc failure-detector2.c && mpiexec -n 4 ./a.out

****************************************************************/

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <stddef.h> // used for offsetof
#include <time.h>
#include <math.h>


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
                for (int other_rank = 0; other_rank < num_procs; other_rank++)
                {
                    if (my_rank != other_rank) 
                    {
                        MPI_Isend(msg, num_procs, MPI_INT, other_rank, ctag, MPI_COMM_WORLD, &requests[other_rank]);
                    }
                }

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

                            while ((end - start) < gamma)
                            {
                                end = MPI_Wtime();
                            }

                            if ((end - start) >= gamma)
                            {
                                // reset start
                                start = MPI_Wtime();
                            }

                            for (int other_rank = 0; other_rank < num_procs; other_rank++)
                            {
                                if (my_rank != other_rank) 
                                {
                                    MPI_Isend(recv, num_procs, MPI_INT, other_rank, ctag, MPI_COMM_WORLD, &requests[other_rank]);
                                }
                            }

                            cnt += 1;

                        }
                        flag = -1;
                    }

                    if (cnt== threshold)
                    {
                        // reset start
                        start = MPI_Wtime();
                        break;
                    }                    
                } 
            } 
        }           
    } 
    MPI_Finalize();
    return 0;
}


