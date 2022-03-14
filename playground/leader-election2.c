/*
  "Hello World" MPI Test Program
*/

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
#define GAMMA 10
#define DELTA 0.5

enum msgTag {mHEATBEAT, mElection, mSetLeader};
typedef struct data_stamp
{
    int ballot; // ballot
    int pid; // pid
} data;


void printArray(int rank, int *array, int size){
    printf("rank: %d) ", rank);
    printf("[");
    for(int loop = 0; loop < size; loop++)
        printf("%d ", array[loop]);
    printf("]");
    printf("\n");
}


MPI_Datatype serialize_data_stamp()
{
    // create a type for struct data 
    const int nitems=2;

    int          blocklengths[nitems];

    blocklengths[0] = 1;
    blocklengths[1] = 1;

    MPI_Datatype types[nitems];
    types[0] = MPI_INT;
    types[1] = MPI_INT;

    MPI_Datatype mpi_data_type;
    MPI_Aint     offsets[nitems];

    offsets[0] = offsetof(data, ballot);
    offsets[1] = offsetof(data, pid);

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &mpi_data_type);
    MPI_Type_commit(&mpi_data_type);
    return mpi_data_type;
}


int isLeaderFailed(int leader, int alive_status[])
{
    printf("alive_status[leader]: %d, leader: %d", alive_status[leader], leader);
    if(alive_status[leader] == 1)
    {
        return 1; // failed
    }
    return 0; // alive
}


void checkLeader (data recv, int num_procs, int leader, MPI_Request requests [], MPI_Datatype mpi_data_type)
{
    int promise_cnt;
    enum msgTag ctag;

    if (leader != recv.pid)
    {
        ctag = mSetLeader;
        printf ("send broadcast\n");
        for (int other_rank = 0; other_rank < num_procs; other_rank++)
        {
            MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, MPI_COMM_WORLD, &requests[other_rank]);
        }
    }
}

void beginElection (data recv, int num_procs, MPI_Request requests [], MPI_Datatype mpi_data_type)
{
    enum msgTag ctag;
    data payload;
    memset(&payload, 0, sizeof(payload));

    payload.ballot = recv.ballot; 
    payload.pid = recv.pid;

    ctag = mElection;
    for (int other_rank = 0; other_rank < num_procs; other_rank++)
    {
        MPI_Isend(&payload, 1, mpi_data_type, other_rank, ctag, MPI_COMM_WORLD, &requests[other_rank]);
    }
}

void leaderElection (MPI_Request requests [], MPI_Status status[], int msg[], int num_procs, int my_rank, MPI_Datatype mpi_data_type, int * leader)
{
    int cnt, flag, ret, threshold;
    cnt =0;
    flag = -1;
    threshold = num_procs;
    int run_loop = 1;

    int is_leader_dead = 0;

    double beg, end, duration;
    beg = MPI_Wtime();
    end = MPI_Wtime();
    duration = num_procs * (GAMMA + DELTA);

    printf("leader duration: %f\n", duration);

    int forced_quit = 0;

    // wait before triggering election
    while (run_loop)
    {
        is_leader_dead = isLeaderFailed((*leader), msg);
        if (is_leader_dead)
        {

            while ((end - beg) <= duration)
            {
                end = MPI_Wtime();
            }

            // see if anyone started election as they would have updated the leader
            is_leader_dead = isLeaderFailed((*leader), msg);
            if (is_leader_dead)
            {
                printf ("A leader process has failed\n");
                data package;
                memset(&package, 0, sizeof(package));
                // create your data here
                package.ballot = (int)time(NULL);  
                package.pid = my_rank;

                printf ("An election has been triggered\n");
                beginElection (package, num_procs, requests, mpi_data_type);
            }
        }
        run_loop = 0;
    }

    data recv;
    memset(&recv, 0, sizeof(recv));

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
            if (ret == MPI_SUCCESS )
            {
                enum msgTag tag = status[my_rank].MPI_TAG;
                int source = status[my_rank].MPI_SOURCE;

                if (tag == mElection)
                {
                    checkLeader(recv, num_procs, (*leader), requests, mpi_data_type);
                
                }
                if (tag == mSetLeader)
                {
                    (*leader) = recv.pid;
                    forced_quit = 1;
                    printf ("A leader: %d, has been choosen\n", (*leader));
                }                
                cnt += 1;
            }
            flag = -1;
        }

        if ((cnt== threshold) || (forced_quit==1))
        {
            if (!flag)
                MPI_Cancel( &requests[my_rank] );
            cnt = 0;
            flag = -1;
            forced_quit = 0;
            break;
        }                    
    }
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

    if (num_procs != 4) {
        fprintf(stderr, "Must use multiple of 4 processes for this example\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];
    double start, end, gamma, delta, duration;
    int cnt, flag, ret, threshold;
    MPI_Datatype mpi_data_type = serialize_data_stamp();
    for (int cur_rank = 0; cur_rank < num_procs; cur_rank++)
    {

        if (my_rank == cur_rank) 
        {
            enum msgTag ctag;
            ctag = mHEATBEAT;
            int leader = 0; //start with setting leader to rank 0
            int msg[num_procs];
            gamma = GAMMA; // in 10 secs
            delta = DELTA; // in 0.5 secs;
            duration = 0;

            double start_timeout, end_timeout, duration_timeout;
            duration_timeout = num_procs * (GAMMA + DELTA);

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
                //printf("broadcast started \n");
                printArray(my_rank, msg, num_procs);

                cnt =0;
                flag = -1;
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
                                recv[source] = 1; //failed
                            }
                            else
                            {
                                recv[source] = 0; //succeed
                            }

                            //force an failure to trigger election
                            //recv[0] = 1;

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

                            //printf("another broadcast started \n");
                            cnt += 1;
                            if (cnt == 1)
                            {
                                start_timeout = MPI_Wtime();
                            }
                        
                        }
                        else
                        {
                            int source = status[my_rank].MPI_SOURCE;
                            recv[source] = 1; //failed
                            end_timeout = MPI_Wtime();
                        }
                        flag = -1;
                    }

                    if ((cnt== threshold) || (end_timeout - start_timeout)>=duration_timeout)
                    {
                        // reset start
                        start = MPI_Wtime();
                        start_timeout = MPI_Wtime();
                        cnt = 0;

                        // Add logic for leader election here
                        leaderElection (requests, status, recv, num_procs, my_rank, mpi_data_type, &leader);
                        printf("leader: %d", leader);
                        break;
                    }                    
                } 
            } 
        }           
    }
    MPI_Type_free(&mpi_data_type); 
    MPI_Finalize();
    return 0;
}


