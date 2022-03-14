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
struct mpi_counter_t {
    MPI_Win win;
    int  hostrank ;
    int  myval;
    int *data;
    int rank, size;
};

//MPI_COMM_WORLD
struct mpi_counter_t *create_shared_var(MPI_Comm comm, int hostrank) {
    struct mpi_counter_t *count;

    count = (struct mpi_counter_t *)malloc(sizeof(struct mpi_counter_t));
    count->hostrank = hostrank;
    MPI_Comm_rank(comm, &(count->rank));
    MPI_Comm_size(comm, &(count->size));

    if (count->rank == hostrank) {
        MPI_Alloc_mem(count->size * sizeof(int), MPI_INFO_NULL, &(count->data));
        for (int i=0; i<count->size; i++) count->data[i] = 0;
        MPI_Win_create(count->data, count->size * sizeof(int), sizeof(int),
                       MPI_INFO_NULL, comm, &(count->win));
    } else {
        count->data = NULL;
        MPI_Win_create(count->data, 0, 1,
                       MPI_INFO_NULL, comm, &(count->win));
    }
    count -> myval = 0;

    return count;
}

int near_atomic(struct mpi_counter_t *count, int increment, int vals[]) {
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, count->hostrank, 0, count->win);
    for (int i=0; i<count->size; i++) {
        if (i == count->rank) {
            MPI_Accumulate(&increment, 1, MPI_INT, 0, i, 1, MPI_INT, MPI_SUM,
                           count->win);
        } else {
            MPI_Get(&vals[i], 1, MPI_INT, 0, i, 1, MPI_INT, count->win);
        }
    }
    MPI_Win_unlock(0, count->win);
}


int modify_var(struct mpi_counter_t *count, int increment) {
    int *vals = (int *)malloc( count->size * sizeof(int) );
    int val;
    near_atomic(count, increment, vals);
    count->myval += increment;
    vals[count->rank] = count->myval;
    val = 0;
    for (int i=0; i<count->size; i++)
    {
        val = MAX(val, vals[i]);
    }
    free(vals);
    return val;
}

int increment_counter(struct mpi_counter_t *count, int increment) {
    int *vals = (int *)malloc( count->size * sizeof(int) );
    int val;
    near_atomic(count, increment, vals);
    count->myval += increment;
    vals[count->rank] = count->myval;
    val = 0;
    for (int i=0; i<count->size; i++)
        val += vals[i];
    free(vals);
    return val;
}

void delete_counter(struct mpi_counter_t **count) {
    if ((*count)->rank == (*count)->hostrank) {
        MPI_Free_mem((*count)->data);
    }
    MPI_Win_free(&((*count)->win));
    free((*count));
    *count = NULL;
    return;
}

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

    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];


    printf("You are in rank: %i.\n", my_rank );

    enum role cur_role = getRole (my_rank, nRole, num_procs);

    // Split the communicator based on the color and use the
    // original rank for ordering
    MPI_Comm row_comm[nRole];
 
    for (int ind=0; ind < nRole; ind++){

        MPI_Comm_split(MPI_COMM_WORLD, ind, my_rank, &row_comm[ind]);
        int row_rank, row_size;
        MPI_Comm_rank(row_comm[ind], &row_rank);
        MPI_Comm_size(row_comm[ind], &row_size);
    }

    int rank;

    // proposer
    struct mpi_counter_t *prop_round_number = create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_current_value = create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_max_promise_round_number = create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_promise_cnt = create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_acks = create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_msg_cnts = create_shared_var(row_comm[PROPOSER], 0);

    MPI_Comm_rank(row_comm[PROPOSER], &rank);

    // acceptors
    struct mpi_counter_t *accept_round_number_promise = create_shared_var(row_comm[ACCEPTOR], 0);
    struct mpi_counter_t *accept_round_number_accepted = create_shared_var(row_comm[ACCEPTOR], 0);
    struct mpi_counter_t *accept_current_value_accepted = create_shared_var(row_comm[ACCEPTOR], 0);
    struct mpi_counter_t *accept_msg_cnts = create_shared_var(row_comm[ACCEPTOR], 0);

    MPI_Comm_rank(row_comm[ACCEPTOR], &rank);

    // learners
    struct mpi_counter_t *learner_current_value_decided = create_shared_var(row_comm[LEARNER], 0);
    struct mpi_counter_t *learner_msg_cnts = create_shared_var(row_comm[LEARNER], 0);

    MPI_Comm_rank(row_comm[LEARNER], &rank);



    if (cur_role == CLIENT) {
        //printf("We have %i processes.\n", num_procs);
        float probability = 0.9;
        int ret;        
        data package;
        memset(&package, 0, sizeof(package));
        // create your data here
        package.value = (rand() > probability) ? 40 : 1;   
        package.round_number = (int)time(NULL);

        int nRole = 4;
        int NUM_REQUESTS = num_procs / nRole;
        int start = PROPOSER * NUM_REQUESTS;
        int end = start + NUM_REQUESTS;

        //printf("start: %d, end: %d\n", start, end);
        enum msgTag tag = mPROPOSE;
        for (int other_rank = start; other_rank < end; other_rank++)
        {
            // send to proposers
            MPI_Isend(&package, 1, mpi_data_type, other_rank, tag, row_comm[PROPOSER], &requests[other_rank]);
        }

    } else if (cur_role == PROPOSER) {
        int cnt =0;
        int flag = -1;
        int ret;
        int nRole = 4;
        int NUM_REQUESTS = num_procs / nRole;
        //int threshold = (2 * NUM_REQUESTS) + 1;
        int threshold = 3 * NUM_REQUESTS;

        int round_number = 0;       //proposer current round number
        int current_value = 0;  //proposer current value

        int max_promise_round_number = EMPTY;

        int promise_cnt = 0;
        int acks = 0;

        printf("proposer \n");
        data accept_value; // get result after consensus
        memset(&accept_value, 0, sizeof(accept_value));
        
        data recv;
        memset(&recv, 0, sizeof(recv));

        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, row_comm[PROPOSER], &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    enum msgTag tag = status[my_rank].MPI_TAG;
                    int source = status[my_rank].MPI_SOURCE;

                    //printf ("Proposer: Kenneth package.value: %d, package.round_number: %d, tag: %d\n", recv.value, recv.round_number, tag);

                    if (tag == mPROPOSE)
                    {
                        // store the value reserved for proposer
                        round_number = modify_var(prop_round_number, recv.round_number);
                        current_value = modify_var(prop_current_value, recv.value);
                        max_promise_round_number = modify_var(prop_max_promise_round_number, recv.round_number);

                        enum msgTag ctag = mPREPARE;
                        int nRole = 4;
                        int NUM_REQUESTS = num_procs / nRole;
                        int start = ACCEPTOR * NUM_REQUESTS;
                        int end = start + NUM_REQUESTS;
                        //printf("line 203 round_number: %d, max_promise_round_number: %d\n", round_number, max_promise_round_number );                       
                        for (int other_rank = start; other_rank < end; other_rank++)
                        {
                            MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, row_comm[ACCEPTOR], &requests[other_rank]);
                        }
                    }

                    else if (tag == mPROMISE)
                    {

                         //printf("line 213 round_number: %d, max_promise_round_number: %d, promise_cnt: %d, recv.round_number: %d\n", round_number, max_promise_round_number, promise_cnt, recv.round_number );
                        if (round_number == recv.round_number)
                        {
                            enum msgTag ctag = mACCEPT;
                            promise_cnt = increment_counter(prop_promise_cnt, 1);
                            if (max_promise_round_number <= recv.round_number )
                            {
                                max_promise_round_number = modify_var(prop_max_promise_round_number, recv.round_number);

                                accept_value = recv;
                            }
                            //printf("line 223 round_number: %d, max_promise_round_number: %d, promise_cnt: %d, recv.round_number: %d\n", round_number, max_promise_round_number, promise_cnt, recv.round_number );
                            //if (promise_cnt == (int)((num_procs / 2.0) + 1))
                            if (promise_cnt == (int) MAX((NUM_REQUESTS / 2.0), 1))
                            {                        
                                int nRole = 4;
                                int NUM_REQUESTS = num_procs / nRole;
                                int start = ACCEPTOR * NUM_REQUESTS;
                                int end = start + NUM_REQUESTS;
                               
                                for (int other_rank = start; other_rank < end; other_rank++)
                                {
                                    //printf("line 235 send to acceptors, accept_value.round_number: %d, accept_value.value: %d\n", accept_value.round_number, accept_value.value);
                                    MPI_Isend(&accept_value, 1, mpi_data_type, other_rank, ctag, row_comm[ACCEPTOR], &requests[other_rank]);
                                }
                            }
                        }
                    }

                    else if (tag == mACCEPTED)
                    {
                        //printf("line 243 will item be sent to learner\n");
                        if (round_number == recv.round_number)
                        {
                            enum msgTag ctag = mDECIDE;
                            //acks = acks + 1;
                            acks = increment_counter(prop_acks, 1);

                            //if (acks == (int)((num_procs / 2.0) + 1))
                            if (acks == (int) MAX((NUM_REQUESTS / 2.0), 1))
                            {

                                int nRole = 4;
                                int NUM_REQUESTS = num_procs / nRole;
                                int start = LEARNER * NUM_REQUESTS;
                                int end = start + NUM_REQUESTS;
                                //printf("line 253 round_number: %d, max_promise_round_number: %d\n", round_number, max_promise_round_number );
                                for (int other_rank = start; other_rank < end; other_rank++)
                                {
                                    MPI_Isend(&accept_value, 1, mpi_data_type, other_rank, ctag, row_comm[LEARNER], &requests[other_rank]);
                                }
                            }
                        }
                    }

                    else if (tag == mNACK)
                    {
                        printf("line 269: nack was received");
                        if (round_number == recv.round_number)
                        {
                            round_number = 0;
                            break; 
                        }                       
                    }
                    //cnt += 1;
                    cnt = increment_counter(prop_msg_cnts, 1);
                    printf ("proposer cnt: %d, threshold: %d\n", cnt, threshold);
                }
                flag = -1;
            }
            if (cnt== threshold)
            {
                if (!flag)
                MPI_Cancel( &requests[my_rank] );
                break;
            }
                
        }             
    } else if (cur_role == ACCEPTOR) {
        int cnt =0;
        int flag = -1;
        int ret;
        int nRole = 4;
        int NUM_REQUESTS = num_procs / nRole;
        int threshold = (2 * NUM_REQUESTS);

        int round_number_promise = 0;       //promise not to accept lower round
        int round_number_accepted = 0;       //round number value is accepted
        int current_value_accepted = EMPTY;  //proposer current value

        printf("acceptor \n");

        data recv;
        memset(&recv, 0, sizeof(recv));

        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, row_comm[ACCEPTOR], &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    enum msgTag tag = status[my_rank].MPI_TAG;
                    int source = status[my_rank].MPI_SOURCE;
                    if (tag == mPREPARE)
                    {

                        printf("line 328 round_number_promise: %d, round_number_accepted: %d, current_value_accepted: %d, recv.round_number: %d\n", round_number_promise, round_number_accepted, current_value_accepted, recv.round_number );

                        enum msgTag ctag;
                        if (round_number_promise < recv.round_number)
                        {
                            round_number_promise = modify_var(accept_round_number_promise, recv.round_number);
                            ctag = mPROMISE;
                            //printf ("line 335, propose is made, NUM_REQUESTS: %d, ctag: %d\n", NUM_REQUESTS, ctag);
                            int start = PROPOSER * NUM_REQUESTS;
                            int end = start + NUM_REQUESTS;
                            //printf ("line 338, start: %d, end: %d\n", start, end);
                            for (int other_rank = start; other_rank < end; other_rank++)
                            {
                                MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, row_comm[PROPOSER], &requests[other_rank]);
                                //printf("line 342 data sent\n");
                            }

                        } 
                        else
                        {
                            ctag = mNACK;
                            //printf ("line 348, nack is made\n");
                            int start = PROPOSER * NUM_REQUESTS;
                            int end = start + NUM_REQUESTS;
                            for (int other_rank = start; other_rank < end; other_rank++)
                            {
                                MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, row_comm[PROPOSER], &requests[other_rank]);
                            }
                        }
                    }

                    else if (tag == mACCEPT)
                    {
                        //printf("line 364 received from proposer is round_number_promise: %d, recv.round_number: %d\n", round_number_promise, recv.round_number);
                        enum msgTag ctag;
                        if (round_number_promise <= recv.round_number)
                        {
                            round_number_promise = modify_var(accept_round_number_promise, recv.round_number);
                            round_number_accepted = modify_var(accept_round_number_promise, recv.round_number);
                            current_value_accepted = modify_var(accept_current_value_accepted, recv.value);

                            //printf("line 372\n");

                            ctag = mACCEPTED;
                            int start = PROPOSER * NUM_REQUESTS;
                            int end = start + NUM_REQUESTS;
                            for (int other_rank = start; other_rank < end; other_rank++)
                            {
                                MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, row_comm[PROPOSER], &requests[other_rank]);
                            }
                        }
                        else
                        {

                            //printf("line 385\n");
                            ctag = mNACK;
                            int start = PROPOSER * NUM_REQUESTS;
                            int end = start + NUM_REQUESTS;
                            for (int other_rank = start; other_rank < end; other_rank++)
                            {
                                MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, row_comm[PROPOSER], &requests[other_rank]);
                            }
                        }

                    }
                    //cnt += 1;
                    cnt = increment_counter(accept_msg_cnts, 1);

                    printf ("acceptors cnt: %d, threshold: %d\n", cnt, threshold);
                }

                flag = -1;
            }
            if (cnt== threshold)
            {
                if (!flag)
                MPI_Cancel( &requests[my_rank] );
                break;
            }
                
        }                
    } else if (cur_role == LEARNER) {
        int cnt =0;
        int flag = -1;
        int ret;
        int nRole = 4;
        //int threshold = num_procs / nRole;
        int NUM_REQUESTS = num_procs / nRole;
        int threshold = (1 * NUM_REQUESTS); // number of message x number of bins
        int current_value_decided = EMPTY;  //decided value

        printf("learner \n");

        data recv;
        memset(&recv, 0, sizeof(recv));

        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, row_comm[LEARNER], &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    enum msgTag tag = status[my_rank].MPI_TAG;
                    int source = status[my_rank].MPI_SOURCE;

                    printf("line 436 recv.value: %d, recv.round_number: %d\n", recv.value, recv.round_number );
                    if (tag == mDECIDE)
                    {
                        if (current_value_decided == EMPTY)
                        {
                            current_value_decided = modify_var(learner_current_value_decided, recv.value);
                        }
                    }
                    //cnt += 1;
                    cnt = increment_counter(learner_msg_cnts, 1);

                    printf ("learner cnt: %d, threshold: %d\n", cnt, threshold);
                }

                flag = -1;
            }
            if (cnt== threshold)
            {
                if (!flag)
                MPI_Cancel( &requests[my_rank] );
                break;
            }
                
        }
        printf ("Decided value is :%d\n", current_value_decided);  
                  
    }
 
    MPI_Barrier(row_comm[PROPOSER]); // proposer_requests
    MPI_Barrier(row_comm[ACCEPTOR]);
    MPI_Barrier(row_comm[LEARNER]);

    //MPI_Waitall(num_procs, requests, status);
    //printf("nRole = %d\n", nRole);
    for (int ind=0; ind < nRole; ind++){
    
        MPI_Comm_free(&row_comm[ind]);
    }


    //MPI_Barrier(row_comm[LEARNER]);
    //delete_counter(&c);
    delete_counter(&prop_round_number);
    delete_counter(&prop_current_value);
    delete_counter(&prop_max_promise_round_number);
    delete_counter(&prop_promise_cnt);
    delete_counter(&prop_acks);
    delete_counter(&prop_msg_cnts);

    delete_counter(&accept_round_number_promise);
    delete_counter(&accept_round_number_accepted);
    delete_counter(&accept_current_value_accepted);
    delete_counter(&accept_msg_cnts);

    delete_counter(&learner_current_value_decided);
    delete_counter(&learner_msg_cnts);

    MPI_Type_free(&mpi_data_type);
    /* Tear down the communication infrastructure */
    MPI_Finalize();
    return 0;
}

