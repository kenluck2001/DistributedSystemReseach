/*
  "Hello World" MPI Test Program
*/
//https://en.wikipedia.org/wiki/Message_Passing_Interface#Example_program
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <stddef.h> // used for offsetof
#include <time.h>

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

    typedef struct data_stamp
    {
        int value;
        int lamport_vec[num_procs];
    } data;


    /* create a type for struct data */
    const int nitems=2;
    //int          blocklengths[nitems] = {1,num_procs};
    int          blocklengths[nitems];

    blocklengths[0] = 1;
    blocklengths[1] = num_procs;

    //MPI_Datatype types[nitems] = {MPI_INT, MPI_INT};
    MPI_Datatype types[nitems];
    types[0] = MPI_INT;
    types[1] = MPI_INT;

    MPI_Datatype mpi_data_type;
    MPI_Aint     offsets[nitems];

    offsets[0] = offsetof(data, value);
    offsets[1] = offsetof(data, lamport_vec);

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &mpi_data_type);
    MPI_Type_commit(&mpi_data_type);

    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];

    printf("You are in rank: %i processes.\n", my_rank );

    if (my_rank == 0) {
        printf("We have %i processes.\n", num_procs);
        srand((my_rank + 1) * time(0));
        int largest_timestamp = -99999; 
        float probability = 0.5;
        int cnt =0; // number of messages
        int flag = -1;
        int ret;        

        data package;
        memset(&package, 0, sizeof(package));
        // create your data here

        package.value = (rand() > probability) ? 40 : 1;
        package.lamport_vec[my_rank] = package.lamport_vec[my_rank] + 1; 
        

        for (int other_rank = 0; other_rank < num_procs; other_rank++)
        {
            MPI_Isend(&package, 1, mpi_data_type, other_rank, my_rank, MPI_COMM_WORLD, &requests[other_rank]);
        }

        data agreed_value; // get result after consensus
        memset(&agreed_value, 0, sizeof(agreed_value));

        data recv;
        memset(&recv, 0, sizeof(recv));
        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    int source = status[my_rank].MPI_SOURCE;
                    //printf("source: %d\n", source);
                    recv.lamport_vec[my_rank] = MAX(recv.lamport_vec[my_rank], recv.lamport_vec[source]) + 1;             
                    //printArray(my_rank, lamport_vec, num_procs);
                    if (largest_timestamp < recv.lamport_vec[source] ){
                        largest_timestamp = recv.lamport_vec[source] ;
                        agreed_value = recv; // copy struct for consensus value
                    }
                    if (source != my_rank){
                        MPI_Isend(&recv, 1, mpi_data_type, source, 0, MPI_COMM_WORLD, &requests[source]);
                    }
                        
                }
                cnt += 1;
                flag = -1;                
            }
              
            if (cnt== num_procs)
                break;
                
        }
        printf("Agreed value: %d at rank: %d\n", agreed_value.value, my_rank);
    } else {
        srand((my_rank + 1) * time(0));
        int largest_timestamp = -99999; 
        float probability = 0.1;

        int cnt =0;
        int flag = -1;
        int ret;

        data package;
        memset(&package, 0, sizeof(package));

        // create your data here

        package.value = (rand() > probability) ? 4 : 1;
        package.lamport_vec[my_rank] = package.lamport_vec[my_rank] + 1; 



        for (int other_rank = 0; other_rank < num_procs; other_rank++)
        {
            MPI_Isend(&package, 1, mpi_data_type, other_rank, my_rank, MPI_COMM_WORLD, &requests[other_rank]);
        }

        data agreed_value; // get result after consensus
        memset(&agreed_value, 0, sizeof(agreed_value));

        data recv;
        memset(&recv, 0, sizeof(recv));

        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    int source = status[my_rank].MPI_SOURCE;
                    recv.lamport_vec[my_rank] = MAX(recv.lamport_vec[my_rank], recv.lamport_vec[source]) + 1;             
                    //printArray(source, recv.lamport_vec, num_procs);

                    if (largest_timestamp < recv.lamport_vec[source] ){
                        largest_timestamp = recv.lamport_vec[source] ;
                        agreed_value = recv; // copy struct for consensus value
                    }

                    if (source != my_rank) {
                        MPI_Isend(&recv, 1, mpi_data_type, source, 0, MPI_COMM_WORLD, &requests[source]);
                    }
                }
                cnt += 1;
                flag = -1;
            }
            if (cnt== num_procs)
                break;
                
        }
        printf("Agreed value: %d at rank: %d\n", agreed_value.value, my_rank);                  
    }
    //MPI_Waitall(num_procs, requests, status);
    
    MPI_Type_free(&mpi_data_type);
    /* Tear down the communication infrastructure */
    MPI_Finalize();
    return 0;
}


/**
MPI_Isend and MPI_Irecv MUST be followed at some point by MPI_Test and MPI_Wait. The process sending should never write in the buffer until the request has been completed. On the other hand, the process receiving should never read in the buffer before the request has been completed. And the only way to know if a request is completed, is to call MPI_Wait and MPI_Test.
*/
