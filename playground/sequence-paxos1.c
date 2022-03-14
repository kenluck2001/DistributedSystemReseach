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


typedef struct data_stamp
{
    int value; // current value
    int round_number; // current rounder number
    int custom_round_number;
} data;


typedef struct sequence_stamp
{
    int array[LIST_SIZE]; 
    int last_pos;
} list;


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


int near_atomic_shared(struct mpi_counter_t *count, int increment, int vals[]) {
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, count->hostrank, 0, count->win);
    for (int i=0; i<count->size; i++) {
        if (i == count->rank) {
            MPI_Accumulate(&increment, 1, MPI_INT, 0, i, 1, MPI_INT, MPI_MAX,
                           count->win);
        } else {
            MPI_Get(&vals[i], 1, MPI_INT, 0, i, 1, MPI_INT, count->win);
        }
    }
    MPI_Win_unlock(0, count->win);
}


int modify_var(struct mpi_counter_t *count, int valuein) {
    int *vals = (int *)malloc( count->size * sizeof(int) );
    int val;
    near_atomic_shared(count, valuein, vals);
    count->myval = MAX(count->myval, valuein);
    vals[count->rank] = count->myval;
    val = 0;
    for (int i=0; i<count->size; i++)
    {
        val = MAX(val, vals[i]);
    }
    free(vals);
    return val;
}


int reset_var(struct mpi_counter_t *count, int valuein) {
    int *vals = (int *)malloc( count->size * sizeof(int) );
    int val;
    near_atomic_shared(count, valuein, vals);
    count->myval = valuein;
    vals[count->rank] = count->myval;
    val = count->myval;
    free(vals);
    return val;
}


int get_var(struct mpi_counter_t *count) {
    return count->myval;
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


MPI_Datatype serialize_data_stamp()
{
    // create a type for struct data 
    const int nitems=3;

    int          blocklengths[nitems];

    blocklengths[0] = 1;
    blocklengths[1] = 1;
    blocklengths[2] = 1;

    MPI_Datatype types[nitems];
    types[0] = MPI_INT;
    types[1] = MPI_INT;
    types[2] = MPI_INT;

    MPI_Datatype mpi_data_type;
    MPI_Aint     offsets[nitems];

    offsets[0] = offsetof(data, value);
    offsets[1] = offsetof(data, round_number);
    offsets[2] = offsetof(data, custom_round_number);

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &mpi_data_type);
    MPI_Type_commit(&mpi_data_type);
    return mpi_data_type;
}


MPI_Datatype serialize_sequence_stamp()
{
    // create a type for struct data 
    const int nitems=2;

    int          blocklengths[nitems];

    blocklengths[0] = LIST_SIZE;
    blocklengths[1] = 1;

    MPI_Datatype types[nitems];
    types[0] = MPI_INT;
    types[1] = MPI_INT;

    MPI_Datatype mpi_data_type;
    MPI_Aint     offsets[nitems];

    offsets[0] = offsetof(list, array);
    offsets[1] = offsetof(list, last_pos);

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &mpi_data_type);
    MPI_Type_commit(&mpi_data_type);
    return mpi_data_type;
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

    MPI_Datatype mpi_data_type = serialize_data_stamp();

    MPI_Datatype mpi_list_type = serialize_sequence_stamp();

    enum role {CLIENT, PROPOSER, ACCEPTOR, LEARNER};
    enum msgTag {mPROPOSE, mPROMISE, mACCEPTED, mNACK, mPREPARE, mACCEPT, mDECIDE, mDECIDE_SEQ, mPINGS};

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

    struct mpi_counter_t *prop_list_last_pos = create_shared_var(row_comm[PROPOSER], 0);

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

    struct mpi_counter_t *learner_list_last_pos = create_shared_var(row_comm[LEARNER], 0);
    MPI_Comm_rank(row_comm[LEARNER], &rank);

    if (cur_role == CLIENT) {
        //printf("We have %i processes.\n", num_procs);
        int nRole = 4;
        int NUM_REQUESTS = num_procs / nRole;
        int threshold = (1 * NUM_REQUESTS); // number of message x number of bins

        int recv_last_pos = 0;
        enum msgTag tag;

        int start = PROPOSER * NUM_REQUESTS;
        int end = start + NUM_REQUESTS;

        int sequence[5] = {10, 20, 30, 40, 50};

        data package;
        memset(&package, 0, sizeof(package));

        package.value = sequence[recv_last_pos];   
        package.round_number = (int)time(NULL);
        package.custom_round_number = EMPTY;

        for (int other_rank = start; other_rank < end; other_rank++)
        {
            // send to proposers
            tag = mPROPOSE;
            MPI_Isend(&package, 1, mpi_data_type, other_rank, tag, row_comm[PROPOSER], &requests[other_rank]);
        }

        //===
        int cnt =0;
        int flag = -1;
        int ret;
        while (1)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                tag = mDECIDE_SEQ;
                ret = MPI_Irecv(&recv_last_pos, 1, MPI_INT, MPI_ANY_SOURCE, tag, row_comm[CLIENT], &requests[my_rank]);
                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);
            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    cnt += 1;
                }
                flag = -1;
            }
            if (cnt== threshold)
            {
                //if (!flag)
                    //MPI_Cancel( &requests[my_rank] );
                cnt =0;
                flag = -1;
                break;
            }                
        }
        //===


        printf("recv_last_pos: %d\n", recv_last_pos);
        memset(&package, 0, sizeof(package));
        package.value = sequence[recv_last_pos];   
        package.round_number = (int)time(NULL);
        package.custom_round_number = EMPTY;

        for (int other_rank = start; other_rank < end; other_rank++)
        {
            // send to proposers
            printf("start next sequence: %d, rank: %d\n", package.value, other_rank);
            tag = mPROPOSE;
            MPI_Isend(&package, 1, mpi_data_type, other_rank, tag, row_comm[PROPOSER], &requests[other_rank]);
            printf("end next sequence\n");
        }


    } else if (cur_role == PROPOSER) {
        int cnt =0;
        int flag = -1;
        int ret;
        int nRole = 4;
        int NUM_REQUESTS = num_procs / nRole;
        //int threshold = (2 * NUM_REQUESTS) + 1;
        int threshold = 3 * NUM_REQUESTS;

        int round_number = EMPTY;       //proposer current round number
        int current_value = EMPTY;  //proposer current value

        int max_promise_round_number = EMPTY;

        int promise_cnt = 0;
        int acks = 0;
        int last_pos = 0;

        list list_recv;
        memset(&list_recv, 0, sizeof(list_recv));

        printf("proposer \n");
        data accept_value; // get result after consensus
        memset(&accept_value, 0, sizeof(accept_value));
        
        data recv;
        memset(&recv, 0, sizeof(recv));

        int ready = 1;
        RESTART_PROPSER: ready = 1;

        while (ready)
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

                    printf ("Proposer: Kenneth package.value: %d, package.round_number: %d, tag: %d\n", recv.value, recv.round_number, tag);

                    if (tag == mPROPOSE)
                    {
                        // store the value reserved for proposer
                        round_number = reset_var(prop_round_number, recv.round_number);
                        current_value = reset_var(prop_current_value, recv.value);
                        //max_promise_round_number = modify_var(prop_max_promise_round_number, recv.round_number);
                        max_promise_round_number = reset_var(prop_max_promise_round_number, recv.round_number);

                        enum msgTag ctag = mPREPARE;
                        int nRole = 4;
                        int NUM_REQUESTS = num_procs / nRole;
                        int start = ACCEPTOR * NUM_REQUESTS;
                        int end = start + NUM_REQUESTS;
                        //printf("line 203 round_number: %d, max_promise_round_number: %d\n", round_number, max_promise_round_number );  
                        //recv.round_number = round_number;                     
                        for (int other_rank = start; other_rank < end; other_rank++)
                        {
                            MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag, row_comm[ACCEPTOR], &requests[other_rank]);
                        }
                    }

                    else if (tag == mPROMISE)
                    {

                         //printf("line 213 round_number: %d, max_promise_round_number: %d, promise_cnt: %d, recv.round_number: %d\n", round_number, max_promise_round_number, promise_cnt, recv.round_number );
                        if (recv.custom_round_number == round_number)
                        {
                            enum msgTag ctag = mACCEPT;
                            promise_cnt = increment_counter(prop_promise_cnt, 1);
                            if (max_promise_round_number <= recv.round_number )
                            {
                                //max_promise_round_number = modify_var(prop_max_promise_round_number, recv.round_number);
                                max_promise_round_number = reset_var(prop_max_promise_round_number, recv.round_number);

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
                                if (accept_value.value == EMPTY)
                                {
                                    accept_value.value = current_value;
                                }
                               
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
                        if ( recv.custom_round_number == round_number)
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

                                // save to the list

                                list_recv.array[last_pos] = accept_value.value; 
                                last_pos = increment_counter(prop_list_last_pos, 1);
                                list_recv.last_pos = last_pos; 

                                for (int other_rank = start; other_rank < end; other_rank++)
                                {
                                    MPI_Isend(&list_recv, 1, mpi_list_type, other_rank, ctag, row_comm[LEARNER], &requests[other_rank]);
                                }


                            }
                        }
                    }

                    else if (tag == mNACK)
                    {
                        printf("line 269: nack was received\n");
                        printf("recv.custom_round_number: %d, recv.round_number: %d, recv.value: %d, round_number: %d\n", recv.custom_round_number, recv.round_number, recv.value, round_number);
                        if ( recv.custom_round_number == round_number)
                        {
                            printf("inside line 269: nack was received\n");
                            round_number = reset_var(prop_round_number, 0);
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
                //if (!flag)
                //MPI_Cancel( &requests[my_rank] );
                round_number = reset_var(prop_round_number, EMPTY);
                current_value = reset_var(prop_current_value, EMPTY);
                max_promise_round_number = reset_var(prop_max_promise_round_number, EMPTY);
                promise_cnt = reset_var(prop_promise_cnt, 0);
                acks = reset_var(prop_acks, 0);
                cnt = reset_var(prop_msg_cnts, 0);
                flag = -1;

                printf ("[Proposer cnt: %d, round_number: %d, current_value: %d, max_promise_round_number: %d, promise_cnt: %d, acks: %d]\n", cnt, round_number, current_value, max_promise_round_number, promise_cnt, acks); 
                //break;
                ready = 0;
                goto RESTART_PROPSER;
            }
                
        }             
    } else if (cur_role == ACCEPTOR) {
        int cnt =0;
        int flag = -1;
        int ret;
        int nRole = 4;
        int NUM_REQUESTS = num_procs / nRole;
        int threshold = (2 * NUM_REQUESTS);

        int round_number_promise = EMPTY;       //promise not to accept lower round
        int round_number_accepted = EMPTY;       //round number value is accepted
        int current_value_accepted = EMPTY;  //proposer current value

        printf("acceptor \n");

        data recv;
        memset(&recv, 0, sizeof(recv));

        int ready = 1;
        RESTART_ACCEPTOR: ready = 1;

        while (ready)
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
                        data payload;
                        memset(&payload, 0, sizeof(payload));

                        enum msgTag ctag;
                        if (round_number_promise <= recv.round_number)
                        {
                            round_number_promise = reset_var(accept_round_number_promise, recv.round_number);
                            ctag = mPROMISE;
                            //printf ("line 335, propose is made, NUM_REQUESTS: %d, ctag: %d\n", NUM_REQUESTS, ctag);
                            int start = PROPOSER * NUM_REQUESTS;
                            int end = start + NUM_REQUESTS;
                            //printf ("line 338, start: %d, end: %d\n", start, end);

                            //payload = recv;
                            //if (current_value_accepted == EMPTY)
                            //{
                            current_value_accepted = reset_var(accept_current_value_accepted, recv.value);
                            //}
                            payload.value = current_value_accepted; // current value

                            //if (round_number_accepted == EMPTY)
                            //{
                            round_number_accepted = reset_var(accept_round_number_accepted, recv.round_number);

                            //}

                            printf("line 328 round_number_promise: %d, round_number_accepted: %d, current_value_accepted: %d, recv.round_number: %d\n", round_number_promise, round_number_accepted, current_value_accepted, recv.round_number );


                            payload.round_number = round_number_accepted; // current rounder number
                            payload.value = current_value_accepted;
                            payload.custom_round_number = recv.round_number;
                            for (int other_rank = start; other_rank < end; other_rank++)
                            {
                                MPI_Isend(&payload, 1, mpi_data_type, other_rank, ctag, row_comm[PROPOSER], &requests[other_rank]);
                                //printf("line 342 data sent\n");
                            }

                            
                        } 
                        else
                        {
                            ctag = mNACK;
                            //printf ("line 348, nack is made\n");
                            int start = PROPOSER * NUM_REQUESTS;
                            int end = start + NUM_REQUESTS;

                            payload = recv;
                            payload.custom_round_number = recv.round_number;
                            for (int other_rank = start; other_rank < end; other_rank++)
                            {
                                MPI_Isend(&payload, 1, mpi_data_type, other_rank, ctag, row_comm[PROPOSER], &requests[other_rank]);
                            }
                        }
                    }

                    else if (tag == mACCEPT)
                    {
                        //printf("line 364 received from proposer is round_number_promise: %d, recv.round_number: %d\n", round_number_promise, recv.round_number);
                        enum msgTag ctag;

                        data payload;
                        memset(&payload, 0, sizeof(payload));

                        if (round_number_promise <= recv.round_number)
                        {
                            round_number_promise = reset_var(accept_round_number_promise, recv.round_number);

                            round_number_accepted = reset_var(accept_round_number_accepted, recv.round_number);
                            current_value_accepted = reset_var(accept_current_value_accepted, recv.value);

                            //printf("line 372\n");

                            payload.value = current_value_accepted; // current value
                            payload.round_number = round_number_accepted; // current rounder number
                            payload.custom_round_number = recv.round_number;


                            ctag = mACCEPTED;
                            int start = PROPOSER * NUM_REQUESTS;
                            int end = start + NUM_REQUESTS;
                            for (int other_rank = start; other_rank < end; other_rank++)
                            {
                                MPI_Isend(&payload, 1, mpi_data_type, other_rank, ctag, row_comm[PROPOSER], &requests[other_rank]);
                            }
                        }
                        else
                        {

                            //printf("line 385\n");
                            ctag = mNACK;
                            int start = PROPOSER * NUM_REQUESTS;
                            int end = start + NUM_REQUESTS;

                            payload = recv;
                            payload.custom_round_number = recv.round_number;

                            for (int other_rank = start; other_rank < end; other_rank++)
                            {
                                MPI_Isend(&payload, 1, mpi_data_type, other_rank, ctag, row_comm[PROPOSER], &requests[other_rank]);
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
                //if (!flag)
                //MPI_Cancel( &requests[my_rank] );
                round_number_promise = reset_var(accept_round_number_promise, EMPTY);
                round_number_accepted = reset_var(accept_round_number_accepted, EMPTY);
                current_value_accepted = reset_var(accept_current_value_accepted, EMPTY);
                cnt = reset_var(accept_msg_cnts, 0); 
                flag = -1;

                printf ("[Acceptor cnt: %d, round_number_promise: %d, round_number_accepted: %d, current_value_accepted: %d]\n", cnt, round_number_promise, round_number_accepted, current_value_accepted);               
                //break;
                ready = 0;
                goto RESTART_ACCEPTOR;
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
        //int current_value_decided = EMPTY;  //decided value

        printf("learner \n");

        list saved_recv;
        memset(&saved_recv, 0, sizeof(saved_recv));

        list recv;
        memset(&recv, 0, sizeof(recv));

        int ready = 1;
        RESTART_LEARNER: ready = 1;

        printf("RESTART_LEARNER is repeating");
        while (ready)
        { 
            /* Receive message from any process */
            if(flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_list_type, MPI_ANY_SOURCE, MPI_ANY_TAG, row_comm[LEARNER], &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);


            if (flag != 0)
            {
                if (ret == MPI_SUCCESS )
                {
                    enum msgTag tag = status[my_rank].MPI_TAG;
                    int source = status[my_rank].MPI_SOURCE;

                    //printf("line 436 recv.value: %d, recv.round_number: %d\n", recv.value, recv.round_number );
                    if (tag == mDECIDE)
                    {
                        //current_value_decided = reset_var(learner_current_value_decided, recv.value);


                        printf("saved_recv.last_pos: %d, recv.last_pos %d\n", saved_recv.last_pos, recv.last_pos );



                        if (saved_recv.last_pos < recv.last_pos)
                        {
                            saved_recv = recv;
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
                //if (!flag)
                //MPI_Cancel( &requests[my_rank] );
                cnt = reset_var(learner_msg_cnts, 0);
                flag = -1;
                printf ("[Learner cnt: %d]\n", cnt);
                ready = 0;
                //goto RESTART_LEARNER;
                //break;

            }
                
        }

        // send last pos to client for book-keeping
        int start = CLIENT * NUM_REQUESTS;
        int end = start + NUM_REQUESTS;

        enum msgTag tag = mDECIDE_SEQ;

        for (int other_rank = start; other_rank < end; other_rank++)
        {
            // send to clients
            MPI_Isend(&saved_recv.last_pos, 1, MPI_INT, other_rank, tag, row_comm[CLIENT], &requests[other_rank]);
        }

        printf ("====================================\n"); 
        printf ("====================================\n"); 
        //printf ("Decided value is :%d\n", current_value_decided); 

        for (int i=0; i<=saved_recv.last_pos; i++)
        {
            printf ("index: %d, value: %d\n", i, saved_recv.array[i]);
        }

        printf ("====================================\n"); 
        printf ("====================================\n"); 

        /**
        if (num_procs > 4)
        {
            MPI_Abort(MPI_COMM_WORLD, 1); // Abort once decided 
        }  
        */   

        goto RESTART_LEARNER;            
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

    delete_counter(&prop_list_last_pos);
    delete_counter(&learner_list_last_pos);

    MPI_Type_free(&mpi_data_type);
    MPI_Type_free(&mpi_list_type);
    /* Tear down the communication infrastructure */
    MPI_Finalize();
    return 0;
}

