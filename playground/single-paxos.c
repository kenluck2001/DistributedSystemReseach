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

/**
This is not working as we may need a barrier
**/


int getRole (int rank, int nRole, int nproc)
{
    int bin =  nproc / nRole;
    int index = 0;
    int start = 0;
    int end = 0;
    while ((index < nRole) || (end < nproc))
    {
        start = index * bin;
        end = (start + bin) - 1;
        if ((rank >= start) && (rank <= end))
        {
            return index;
        }
        index = index + 1;
    }

    return index;
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

    if ((num_procs % nRole) != 0) {
        fprintf(stderr, "Must use multiple of 4 processes for this example\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* Until this point, all programs have been doing exactly the same.
       Here, we check the rank to distinguish the roles of the programs */

    typedef struct data_stamp
    {
        int value; // current value
        int round_number; // current rounder number
    } data;

    /* create a type for struct data */
    const int nitems=2;

    int          blocklengths[nitems];

    blocklengths[0] = 1;
    blocklengths[1] = 1;

    MPI_Datatype types[nitems];
    types[0] = MPI_INT;
    types[1] = MPI_INT;

    MPI_Datatype mpi_data_type;
    MPI_Aint     offsets[nitems];

    offsets[0] = offsetof(data, value);
    offsets[1] = offsetof(data, round_number);

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &mpi_data_type);
    MPI_Type_commit(&mpi_data_type);

    enum role {CLIENT, PROPOSER, ACCEPTOR, LEARNER};
    enum msgTag {mPROPOSE, mPROMISE, mACCEPTED, mNACK, mPREPARE, mACCEPT, mDECIDE};

    // bin the requests into manageable block
    int NUM_REQUESTS = num_procs / nRole;
    // CLIENT
    MPI_Request client_requests[NUM_REQUESTS];
    MPI_Status client_status[NUM_REQUESTS];

    // PROPOSER
    MPI_Request proposer_requests[NUM_REQUESTS];
    MPI_Status proposer_status[NUM_REQUESTS];

    // ACCEPTOR
    MPI_Request acceptor_requests[NUM_REQUESTS];
    MPI_Status acceptor_status[NUM_REQUESTS];

    // LEARNER
    MPI_Request learner_requests[NUM_REQUESTS];
    MPI_Status learner_status[NUM_REQUESTS];

    printf("You are in rank: %i.\n", my_rank );

    enum role cur_role = getRole (my_rank, nRole, num_procs);

    if (cur_role == CLIENT) {
        printf("We have %i processes.\n", num_procs);
        float probability = 0.9;
        int ret;        
        printf("1 code is here\n");
        data package;
        memset(&package, 0, sizeof(package));
        // create your data here
        printf("2 code is here\n");
        package.value = (rand() > probability) ? 40 : 1;   
        package.round_number = (int)time(NULL);
        //package.round_number = 100;
        printf("3 code is here: %d\n");
        printf ("Kenneth package.value: %d, package.round_number: %d", package.value, package.round_number);
        enum msgTag tag = mPROPOSE;
        for (int other_rank = 0; other_rank < NUM_REQUESTS; other_rank++)
        {
            printf("4 code is here\n");
            MPI_Isend(&package, 1, mpi_data_type, other_rank, tag, MPI_COMM_WORLD, &proposer_requests[other_rank]);
        }

    } else if (cur_role == PROPOSER) {
        int cnt =0;
        int flag = -1;
        int ret;
        int nRole = 4;
        //int threshold = num_procs / nRole;
        int threshold = 1;
        int NUM_REQUESTS = num_procs / nRole;


        int round_number = EMPTY;       //proposer current round number
        int current_value = EMPTY;  //proposer current value

        int max_promise_round_number = -10000000;

        int promise_cnt = 0;
        int acks = 0;


        data accept_value; // get result after consensus
        memset(&accept_value, 0, sizeof(accept_value));
        
        data recv;
        memset(&recv, 0, sizeof(recv));

        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &proposer_requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&proposer_requests[my_rank], &flag, &proposer_status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    enum msgTag tag = proposer_status[my_rank].MPI_TAG;
                    int source = proposer_status[my_rank].MPI_SOURCE;

                    if (tag == mPROPOSE)
                    {
                        // store the value reserved for proposer
                        round_number = recv.round_number;
                        current_value = recv.value;

                        enum msgTag ctag = mPREPARE;
                       
                        for (int other_rank = 0; other_rank < NUM_REQUESTS; other_rank++)
                        {
                            MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, MPI_COMM_WORLD, &acceptor_requests[other_rank]);
                        }
                    }

                    if (tag == mPROMISE)
                    {
                        if (round_number == recv.round_number)
                        {
                            enum msgTag ctag = mACCEPT;
                            promise_cnt = promise_cnt + 1;
                            if (max_promise_round_number < recv.round_number )
                            {
                                max_promise_round_number = recv.round_number;
                                accept_value = recv;
                            }

                            //if (promise_cnt == (int)((num_procs / 2.0) + 1))
                            if (promise_cnt == (int)((NUM_REQUESTS / 2.0)))
                            {                        
                                for (int other_rank = 0; other_rank < NUM_REQUESTS; other_rank++)
                                {
                                    MPI_Isend(&accept_value, 1, mpi_data_type, other_rank, ctag, MPI_COMM_WORLD, &acceptor_requests[other_rank]);
                                }
                            }
                        }
                    }

                    if (tag == mACCEPTED)
                    {
                        if (round_number == recv.round_number)
                        {
                            enum msgTag ctag = mDECIDE;
                            acks = acks + 1;

                            //if (acks == (int)((num_procs / 2.0) + 1))
                            if (acks == (int)((NUM_REQUESTS / 2.0)))
                            {
                                for (int other_rank = 0; other_rank < NUM_REQUESTS; other_rank++)
                                {
                                    MPI_Isend(&accept_value, 1, mpi_data_type, other_rank, ctag, MPI_COMM_WORLD, &learner_requests[other_rank]);
                                }
                            }
                        }
                    }

                    if (tag == mNACK)
                    {
                        if (round_number == recv.round_number)
                        {
                            round_number = 0;
                            break; 
                        }                       
                    }

                }
                cnt += 1;
                flag = -1;

                printf ("proposers cnt: %d", cnt);

            }
            if (cnt== threshold)
                break;
                
        }                  
    } else if (cur_role == ACCEPTOR) {
        int cnt =0;
        int flag = -1;
        int ret;
        int nRole = 4;
        //int threshold = num_procs / nRole;
        int threshold = 1;
        int NUM_REQUESTS = num_procs / nRole;


        int round_number_promise = 0;       //promise not to accept lower round
        int round_number_accepted = 0;       //round number value is accepted
        int current_value_accepted = EMPTY;  //proposer current value

        data recv;
        memset(&recv, 0, sizeof(recv));

        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &acceptor_requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&acceptor_requests[my_rank], &flag, &acceptor_status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    enum msgTag tag = acceptor_status[my_rank].MPI_TAG;
                    int source = acceptor_status[my_rank].MPI_SOURCE;
                    if (tag == mPREPARE)
                    {
                        enum msgTag ctag;
                        if (round_number_promise < recv.round_number)
                        {
                            round_number_promise = recv.round_number;
                            ctag = mPROMISE;
                            for (int other_rank = 0; other_rank < NUM_REQUESTS; other_rank++)
                            {
                                MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, MPI_COMM_WORLD, &proposer_requests[other_rank]);
                            }

                        } 
                        else
                        {
                            ctag = mNACK;
                            for (int other_rank = 0; other_rank < NUM_REQUESTS; other_rank++)
                            {
                                MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, MPI_COMM_WORLD, &proposer_requests[other_rank]);
                            }
                        }
                    }

                    if (tag == mACCEPT)
                    {
                        enum msgTag ctag;
                        if (round_number_promise < recv.round_number)
                        {
                            round_number_promise = recv.round_number;
                            round_number_accepted = recv.round_number;   
                            current_value_accepted = recv.value;  

                            ctag = mACCEPTED;
                            for (int other_rank = 0; other_rank < NUM_REQUESTS; other_rank++)
                            {
                                MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, MPI_COMM_WORLD, &proposer_requests[other_rank]);
                            }
                        }
                        else
                        {
                            ctag = mNACK;
                            for (int other_rank = 0; other_rank < NUM_REQUESTS; other_rank++)
                            {
                                MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, MPI_COMM_WORLD, &proposer_requests[other_rank]);
                            }
                        }

                    }
                }
                cnt += 1;
                flag = -1;

                printf ("acceptors cnt: %d", cnt);

            }
            if (cnt== threshold)
                break;
                
        }                  
    } else if (cur_role == LEARNER) {
        int cnt =0;
        int flag = -1;
        int ret;
        int nRole = 4;
        //int threshold = num_procs / nRole;
        int threshold = 1;
        int NUM_REQUESTS = num_procs / nRole;

        int current_value_decided = EMPTY;  //decided value

        data recv;
        memset(&recv, 0, sizeof(recv));

        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &learner_requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&learner_requests[my_rank], &flag, &learner_status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    enum msgTag tag = learner_status[my_rank].MPI_TAG;
                    int source = learner_status[my_rank].MPI_SOURCE;

                    if (tag == mDECIDE)
                    {
                        if (current_value_decided == EMPTY)
                        {
                            current_value_decided = recv.value;
                        }
                    }
                }
                cnt += 1;
                flag = -1;

                printf ("learners cnt: %d", cnt);
            }
            if (cnt== threshold)
                break;
                
        }
        printf ("Decided value is :%d", current_value_decided);                  
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
