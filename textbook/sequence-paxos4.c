/*****************************************************************
Name: Kenneth Emeka Odoh
This is a demonstrations of sequence paxos as a repeated run of single paxos on a list of value while maintain the order with a series of states in each role. 

How to run the source code
===========================
mpicc sequence-paxos4.c && mpiexec -n 4 ./a.out

*****************************************************************/
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


enum role {CLIENT, PROPOSER, ACCEPTOR, LEARNER};
enum msgTag {mPROPOSE, mPROMISE, mACCEPTED, mNACK, mPREPARE, mACCEPT, mDECIDE, mDECIDE_SEQ, mPINGS};

/////////

typedef struct data_stamp
{
    int value; // current value
    int round_number; // current rounder number
    int custom_round_number;
    int total_size;
} data;

typedef struct sequence_stamp
{
    int array[LIST_SIZE]; 
    int last_pos;
} list;

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
    const int nitems=4;

    int          blocklengths[nitems];

    blocklengths[0] = 1;
    blocklengths[1] = 1;
    blocklengths[2] = 1;
    blocklengths[3] = 1;

    MPI_Datatype types[nitems];
    types[0] = MPI_INT;
    types[1] = MPI_INT;
    types[2] = MPI_INT;
    types[3] = MPI_INT;

    MPI_Datatype mpi_data_type;
    MPI_Aint     offsets[nitems];

    offsets[0] = offsetof(data, value);
    offsets[1] = offsetof(data, round_number);
    offsets[2] = offsetof(data, custom_round_number);
    offsets[3] = offsetof(data, total_size);

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


void clientLogic(MPI_Comm row_comm[], MPI_Request requests[], MPI_Status status[],
                 MPI_Datatype mpi_data_type, int my_rank, int num_procs, int nRole)
{
    int NUM_REQUESTS = num_procs / nRole;
    int threshold = NUM_REQUESTS; // Number of expected decision messages before potentially moving to the next sequence element

    int recv_last_pos = 0;
    enum msgTag tag;

    int start = PROPOSER * NUM_REQUESTS;
    int end = start + NUM_REQUESTS;

    int sequence[NUM_OF_ELEM] = {10, 20, 30, 40, 50, 60};
    int data_size = sizeof(sequence) / sizeof(sequence[0]);

    data package;
    memset(&package, 0, sizeof(package));
    package.custom_round_number = EMPTY;
    package.total_size = data_size;

    int cnt = 0;
    int flag = -1;
    int ret;
    int processing = 1; // Flag to control the main loop

    while (processing)
    {
        if (recv_last_pos <= data_size - 1)
        {
            // Send proposal to all proposers for the current sequence value
            package.value = sequence[recv_last_pos];
            package.round_number = (int)time(NULL);
            for (int other_rank = start; other_rank < end; other_rank++)
            {
                tag = mPROPOSE;
                MPI_Isend(&package, 1, mpi_data_type, other_rank, tag, row_comm[PROPOSER], &requests[other_rank]);
                printf("Client %d: Sent propose for sequence[%d] = %d to proposer %d\n", my_rank, recv_last_pos, package.value, other_rank);
            }

            // Reset receive flag and counter
            flag = -1;
            cnt = 0;

            // Wait to receive the decided sequence position (threshold number of times)
            while (cnt < threshold)
            {
                if (flag != 0)
                {
                    tag = mDECIDE_SEQ;
                    ret = MPI_Irecv(&recv_last_pos, 1, MPI_INT, MPI_ANY_SOURCE, tag, row_comm[CLIENT], &requests[my_rank]);
                    flag = 0;
                }
                MPI_Test(&requests[my_rank], &flag, &status[my_rank]);
                if (flag != 0 && ret == MPI_SUCCESS)
                {
                    printf("Client %d: Received recv_last_pos: %d (count: %d)\n", my_rank, recv_last_pos, cnt + 1);
                    cnt++;
                    flag = -1; // Reset flag to continue receiving
                }
            }

            // After receiving enough decision messages, continue to the next sequence element
        }
        else
        {
            processing = 0; // Exit the main loop when all sequence elements are processed
        }
    }
    printf("Client %d: Finished processing the sequence.\n", my_rank);
}


void proposerLogic(MPI_Comm row_comm[], MPI_Request requests[],
                   MPI_Status status[], MPI_Datatype mpi_data_type, MPI_Datatype mpi_list_type, int my_rank,
                   int num_procs, int nRole)
{
    int NUM_REQUESTS = num_procs / nRole;
    int threshold = 3 * NUM_REQUESTS;

    int round_number = EMPTY; // proposer current round number
    int current_value = EMPTY; // proposer current value
    int max_promise_round_number = EMPTY;
    int promise_cnt = 0;
    int acks = 0;
    int last_pos = 0;
    int cnt = 0;
    int flag = -1;
    int ret;
    int ready = 1;
    data recv;
    memset(&recv, 0, sizeof(recv));
    list list_recv;
    memset(&list_recv, 0, sizeof(list_recv));
    data accept_value; // get result after consensus
    memset(&accept_value, 0, sizeof(accept_value));

    printf("proposer %d\n", my_rank);

    while (ready)
    {
        if (recv.total_size == 0)
        {
            ready = 1;
        }
        else if ((last_pos <= recv.total_size - 1) && (last_pos >= 0))
        {
            ready = 1;
        }
        else
        {
            printf("Proposer %d ENDED\n", my_rank);
            ready = 0;
            continue; // Exit the loop
        }

        // Reset variables for a new proposal round
        round_number = EMPTY;
        current_value = EMPTY;
        max_promise_round_number = EMPTY;
        promise_cnt = 0;
        acks = 0;
        cnt = 0;
        flag = -1;

        while (cnt < threshold && ready)
        {
            if (flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, row_comm[PROPOSER], &requests[my_rank]);
                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);
            if (flag != 0)
            {
                if (ret == MPI_SUCCESS)
                {
                    enum msgTag tag = status[my_rank].MPI_TAG;
                    int source = status[my_rank].MPI_SOURCE;

                    printf("Proposer %d: Received value: %d, round: %d, tag: %d, last_pos: %d\n", my_rank, recv.value, recv.round_number, tag, last_pos);

                    if (tag == mPROPOSE)
                    {
                        round_number = recv.round_number;
                        current_value = recv.value;
                        max_promise_round_number = recv.round_number;

                        enum msgTag prepare_tag = mPREPARE;
                        int start_acceptor = ACCEPTOR * NUM_REQUESTS;
                        int end_acceptor = start_acceptor + NUM_REQUESTS;

                        for (int other_rank = start_acceptor; other_rank < end_acceptor; other_rank++)
                        {
                            MPI_Isend(&recv, 1, mpi_data_type, other_rank, prepare_tag, row_comm[ACCEPTOR], &requests[other_rank]);
                        }
                    }
                    else if (tag == mPROMISE)
                    {
                        if (recv.custom_round_number == round_number)
                        {
                            enum msgTag accept_tag = mACCEPT;
                            promise_cnt += 1;
                            if (max_promise_round_number <= recv.round_number)
                            {
                                max_promise_round_number = recv.round_number;
                                accept_value = recv;
                            }

                            if (promise_cnt == (int)MAX((NUM_REQUESTS / 2.0), 1))
                            {
                                int start_acceptor = ACCEPTOR * NUM_REQUESTS;
                                int end_acceptor = start_acceptor + NUM_REQUESTS;
                                if (accept_value.value == EMPTY)
                                {
                                    accept_value.value = current_value;
                                }
                                accept_value.custom_round_number = round_number; // Ensure round number is set for accept
                                for (int other_rank = start_acceptor; other_rank < end_acceptor; other_rank++)
                                {
                                    MPI_Isend(&accept_value, 1, mpi_data_type, other_rank, accept_tag, row_comm[ACCEPTOR], &requests[other_rank]);
                                }
                            }
                        }
                    }
                    else if (tag == mACCEPTED)
                    {
                        if (recv.custom_round_number == round_number)
                        {
                            acks += 1;
                            if (acks == (int)MAX((NUM_REQUESTS / 2.0), 1))
                            {
                                enum msgTag decide_tag = mDECIDE;
                                int start_learner = LEARNER * NUM_REQUESTS;
                                int end_learner = start_learner + NUM_REQUESTS;

                                list_recv.array[last_pos] = accept_value.value;
                                last_pos += 1;
                                list_recv.last_pos = last_pos;

                                for (int other_rank = start_learner; other_rank < end_learner; other_rank++)
                                {
                                    MPI_Isend(&list_recv, 1, mpi_list_type, other_rank, decide_tag, row_comm[LEARNER], &requests[other_rank]);
                                }
                            }
                        }
                    }
                    else if (tag == mNACK)
                    {
                        printf("Proposer %d: Received NACK for round %d, current round %d\n", my_rank, recv.custom_round_number, round_number);
                        if (recv.custom_round_number == round_number)
                        {
                            break; // Break inner loop to restart proposal with a new round number
                        }
                    }
                    cnt += 1;
                    printf("Proposer %d: cnt: %d, threshold: %d\n", my_rank, cnt, threshold);
                }
                flag = -1;
            }
        }

        printf("Proposer %d: End of inner loop, cnt: %d, threshold: %d\n", my_rank, cnt, threshold);

        // The outer loop condition will determine if we continue to the next proposal
        // based on the received recv.total_size and last_pos.
    }
    printf("Proposer %d: Exiting main loop.\n", my_rank);
}


void acceptorLogic(MPI_Comm row_comm[], MPI_Request requests[],
                   MPI_Status status[], MPI_Datatype mpi_data_type, int my_rank,
                   int num_procs, int nRole)
{
    int NUM_REQUESTS = num_procs / nRole;
    int threshold = (2 * NUM_REQUESTS);
    int cnt = 0;
    int round_number_promise = EMPTY;     // promise not to accept lower round
    int round_number_accepted = EMPTY;    // round number value is accepted
    int current_value_accepted = EMPTY; // proposer current value
    enum msgTag ctag;
    int flag = -1;
    int ret;
    int ready = 1;
    int accept_epoch_cnts = 0;
    data recv;
    memset(&recv, 0, sizeof(recv));

    printf("acceptor %d\n", my_rank);

    while (ready)
    {
        printf("accept_epoch_cnts: %d, recv.total_size: %d\n", accept_epoch_cnts, recv.total_size);
        accept_epoch_cnts += 1;
        if (recv.total_size == 0)
        {
            ready = 1;
        }
        else if ((accept_epoch_cnts <= recv.total_size) && (accept_epoch_cnts >= 0))
        {
            ready = 1;
        }
        else
        {
            printf("ACCEPTOR %d ENDED\n", my_rank);
            ready = 0;
            continue; // Exit the loop
        }

        // Reset variables for a new epoch
        round_number_promise = EMPTY;
        round_number_accepted = EMPTY;
        current_value_accepted = EMPTY;
        cnt = 0;
        flag = -1;

        while (cnt < threshold && ready)
        {
            if (flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG, row_comm[ACCEPTOR], &requests[my_rank]);
                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);

            if (flag != 0)
            {
                if (ret == MPI_SUCCESS)
                {
                    enum msgTag tag = status[my_rank].MPI_TAG;
                    int source = status[my_rank].MPI_SOURCE;

                    if (tag == mPREPARE)
                    {
                        data payload;
                        memset(&payload, 0, sizeof(payload));

                        if (round_number_promise <= recv.round_number)
                        {
                            round_number_promise = recv.round_number;
                            ctag = mPROMISE;
                            int start = PROPOSER * NUM_REQUESTS;
                            int end = start + NUM_REQUESTS;
                            current_value_accepted = recv.value;
                            payload.value = current_value_accepted; // current value
                            round_number_accepted = recv.round_number;
                            payload.round_number = round_number_accepted; // current rounder number
                            payload.value = current_value_accepted;
                            payload.custom_round_number = recv.round_number;
                            payload.total_size = recv.total_size;
                            for (int other_rank = start; other_rank < end; other_rank++)
                            {
                                MPI_Isend(&payload, 1, mpi_data_type, other_rank, ctag, row_comm[PROPOSER], &requests[other_rank]);
                            }
                        }
                        else
                        {
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
                    else if (tag == mACCEPT)
                    {
                        data payload;
                        memset(&payload, 0, sizeof(payload));

                        if (round_number_promise <= recv.round_number)
                        {
                            round_number_promise = recv.round_number;
                            round_number_accepted = recv.round_number;
                            current_value_accepted = recv.value;

                            payload.value = current_value_accepted; // current value
                            payload.round_number = round_number_accepted; // current rounder number
                            payload.custom_round_number = recv.round_number;
                            payload.total_size = recv.total_size;

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
                    cnt += 1;
                    printf("Acceptor %d: cnt: %d, threshold: %d\n", my_rank, cnt, threshold);
                }
                flag = -1;
            }
        }
        printf("Acceptor %d: End of inner loop, cnt: %d, threshold: %d\n", my_rank, cnt, threshold);
        // The outer loop condition will determine if we continue to the next epoch
    }
    printf("Acceptor %d: Exiting main loop.\n", my_rank);
}


void learnerLogic(MPI_Comm row_comm[], MPI_Request requests[],
                  MPI_Status status[], MPI_Datatype mpi_data_type, MPI_Datatype mpi_list_type, int my_rank,
                  int num_procs, int nRole)
{
    int NUM_REQUESTS = num_procs / nRole;
    int threshold = NUM_REQUESTS; // Number of expected decision messages
    int cnt = 0;
    printf("learner %d\n", my_rank);

    list saved_recv;
    memset(&saved_recv, 0, sizeof(saved_recv));

    list recv;
    memset(&recv, 0, sizeof(recv));

    int processing = 1;
    int flag = -1;
    int ret;

    while (processing)
    {
        if (saved_recv.last_pos >= NUM_OF_ELEM)
        {
            MPI_Abort(row_comm[LEARNER], 1);
            processing = 0; // Should not reach here if abort is successful
            continue;
        }

        printf("Learner %d: Waiting for decide messages (current last_pos: %d)\n", my_rank, saved_recv.last_pos);
        cnt = 0;
        flag = -1;

        while (cnt < threshold)
        {
            if (flag != 0)
            {
                ret = MPI_Irecv(&recv, 1, mpi_list_type, MPI_ANY_SOURCE, mDECIDE, row_comm[LEARNER], &requests[my_rank]);
                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);

            if (flag != 0)
            {
                if (ret == MPI_SUCCESS)
                {
                    printf("Learner %d: Received decide message from %d, recv.last_pos: %d\n", my_rank, status[my_rank].MPI_SOURCE, recv.last_pos);
                    if (saved_recv.last_pos < recv.last_pos)
                    {
                        saved_recv = recv;
                        printf("Learner %d: Updated saved_recv.last_pos to %d\n", my_rank, saved_recv.last_pos);
                    }
                    cnt++;
                }
                flag = -1;
            }
        }

        printf("Learner %d: Received %d decide messages (threshold: %d), current last_pos: %d\n", my_rank, cnt, threshold, saved_recv.last_pos);

        // Send the updated last_pos to the clients
        int start_client = CLIENT * NUM_REQUESTS;
        int end_client = start_client + NUM_REQUESTS;
        enum msgTag tag_client = mDECIDE_SEQ;

        for (int other_rank = start_client; other_rank < end_client; other_rank++)
        {
            MPI_Isend(&saved_recv.last_pos, 1, MPI_INT, other_rank, tag_client, row_comm[CLIENT], &requests[other_rank]);
            printf("Learner %d: Sent saved_recv.last_pos (%d) to client %d\n", my_rank, saved_recv.last_pos, other_rank);
        }

        printf("Learner %d: Decided values up to index %d:\n", my_rank, saved_recv.last_pos - 1);
        printf("====================================\n");
        printf("====================================\n");
        for (int i = 0; i < saved_recv.last_pos; i++)
        {
            printf("index: %d, value: %d\n", i, saved_recv.array[i]);
        }
        printf("====================================\n");
        printf("====================================\n");

        // Continue processing as long as not all elements are decided
        if (saved_recv.last_pos < NUM_OF_ELEM)
        {
            continue; // Go back to the beginning of the while loop to receive more decisions
        }
        else
        {
            processing = 0; // All elements decided, exit the loop
        }
    }
    printf("Learner %d: Finished learning all elements.\n", my_rank);
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

    if (num_procs != nRole) {
        fprintf(stderr, "Must be %d processes for this example\n", nRole);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* Until this point, all programs have been doing exactly the same.
       Here, we check the rank to distinguish the roles of the programs */

    MPI_Datatype mpi_data_type = serialize_data_stamp();

    MPI_Datatype mpi_list_type = serialize_sequence_stamp();

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

    // client
    MPI_Comm_rank(row_comm[CLIENT], &rank);
    // proposer
    MPI_Comm_rank(row_comm[PROPOSER], &rank);
    // acceptors
    MPI_Comm_rank(row_comm[ACCEPTOR], &rank);
    // learners
    MPI_Comm_rank(row_comm[LEARNER], &rank);

    if (cur_role == CLIENT) {
        //int nRole = 4;
        clientLogic(row_comm, requests, status, mpi_data_type, my_rank, num_procs, nRole);
    } else if (cur_role == PROPOSER) {
        //int nRole = 4;
        proposerLogic(row_comm, requests, status, mpi_data_type, mpi_list_type, my_rank, num_procs, nRole);
    } else if (cur_role == ACCEPTOR) {
        //int nRole = 4;
        acceptorLogic(row_comm, requests, status, mpi_data_type, my_rank, num_procs, nRole);
    } else if (cur_role == LEARNER) {
        //int nRole = 4;
        learnerLogic(row_comm, requests, status, mpi_data_type, mpi_list_type, my_rank, num_procs, nRole);
    }

    MPI_Barrier(row_comm[CLIENT]); 
    MPI_Barrier(row_comm[PROPOSER]);  // proposer_requests
    MPI_Barrier(row_comm[ACCEPTOR]);
    MPI_Barrier(row_comm[LEARNER]);

    for (int ind=0; ind < nRole; ind++){
        MPI_Comm_free(&row_comm[ind]);
    }

    MPI_Type_free(&mpi_data_type);
    MPI_Type_free(&mpi_list_type);
    /* Tear down the communication infrastructure */
    MPI_Finalize();
    return 0;
}

