/*****************************************************************
Name: Kenneth Emeka Odoh
This is a demonstrations of single paxos works, we allow the users to group the
processes into 4 roles  (CLIENT, PROPOSER, ACCEPTOR, LEARNER), where each role
has the same number of processes. We allow multiple of 4 for the total number of
processes and each role has a set of shared state handled with our custom
distributed shared primitives.

How to run the source code
===========================
mpicc single-paxos3-snapshot.c && mpiexec -n 5 ./a.out
mpicc single-paxos3-snapshot.c && mpiexec -n 10 ./a.out
mpicc single-paxos3-snapshot.c && mpiexec -n 15 ./a.out

*****************************************************************/
// Works for equal number of roles (CLIENT, PROPOSER, ACCEPTOR, LEARNER,
// TELEMETRY)
#include <assert.h>
#include <math.h>
#include <mpi.h>
#include <stddef.h>  // used for offsetof
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define EMPTY -999
#define TIMEOUT_SEC 5.0  // Timeout in seconds

enum manageTag { mSNAPSHOT, mREVERT };
enum msgTag {
    mPROPOSE,
    mPROMISE,
    mACCEPTED,
    mNACK,
    mPREPARE,
    mACCEPT,
    mDECIDE
};
enum role { CLIENT, PROPOSER, ACCEPTOR, LEARNER, TELEMETRY };

struct mpi_counter_t {
    MPI_Win win;
    int hostrank;
    int myval;
    int *data;
    int rank, size;
};

typedef struct data_stamp {
    int value;         // current value
    int round_number;  // current rounder number
    int custom_round_number;
} data;

struct mpi_counter_t *create_shared_var(MPI_Comm comm, int hostrank) {
    struct mpi_counter_t *count;

    count = (struct mpi_counter_t *)malloc(sizeof(struct mpi_counter_t));
    count->hostrank = hostrank;
    MPI_Comm_rank(comm, &(count->rank));
    MPI_Comm_size(comm, &(count->size));

    if (count->rank == hostrank) {
        MPI_Alloc_mem(count->size * sizeof(int), MPI_INFO_NULL, &(count->data));
        for (int i = 0; i < count->size; i++) count->data[i] = 0;
        MPI_Win_create(count->data, count->size * sizeof(int), sizeof(int),
                       MPI_INFO_NULL, comm, &(count->win));
    } else {
        count->data = NULL;
        MPI_Win_create(count->data, 0, 1, MPI_INFO_NULL, comm, &(count->win));
    }
    count->myval = 0;

    return count;
}

int near_atomic(struct mpi_counter_t *count, int increment, int vals[]) {
    MPI_Win_lock(MPI_LOCK_EXCLUSIVE, count->hostrank, 0, count->win);
    for (int i = 0; i < count->size; i++) {
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
    for (int i = 0; i < count->size; i++) {
        if (i == count->rank) {
            MPI_Accumulate(&increment, 1, MPI_INT, 0, i, 1, MPI_INT, MPI_MAX,
                           count->win);
        } else {
            MPI_Get(&vals[i], 1, MPI_INT, 0, i, 1, MPI_INT, count->win);
        }
    }
    MPI_Win_unlock(0, count->win);
}

int modify_var(struct mpi_counter_t *count, int increment) {
    int *vals = (int *)malloc(count->size * sizeof(int));
    int val;
    near_atomic_shared(count, increment, vals);
    count->myval = MAX(count->myval, increment);
    vals[count->rank] = count->myval;
    val = 0;
    for (int i = 0; i < count->size; i++) {
        val = MAX(val, vals[i]);
    }
    free(vals);
    return val;
}

int reset_var(struct mpi_counter_t *count, int valuein) {
    int *vals = (int *)malloc(count->size * sizeof(int));
    int val;
    near_atomic_shared(count, valuein, vals);
    count->myval = valuein;
    vals[count->rank] = count->myval;
    val = count->myval;
    free(vals);
    return val;
}

int increment_counter(struct mpi_counter_t *count, int increment) {
    int *vals = (int *)malloc(count->size * sizeof(int));
    int val;
    near_atomic(count, increment, vals);
    count->myval += increment;
    vals[count->rank] = count->myval;
    val = 0;
    for (int i = 0; i < count->size; i++) val += vals[i];
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

 
MPI_Datatype serialize_data_stamp()
{
   /* create a type for struct data */
    const int nitems = 3;

    int blocklengths[nitems];

    blocklengths[0] = 1;
    blocklengths[1] = 1;
    blocklengths[2] = 1;

    MPI_Datatype types[nitems];
    types[0] = MPI_INT;
    types[1] = MPI_INT;
    types[2] = MPI_INT;

    MPI_Datatype mpi_data_type;
    MPI_Aint offsets[nitems];

    offsets[0] = offsetof(data, value);
    offsets[1] = offsetof(data, round_number);
    offsets[2] = offsetof(data, custom_round_number);

    MPI_Type_create_struct(nitems, blocklengths, offsets, types,
                           &mpi_data_type);
    MPI_Type_commit(&mpi_data_type);
    return mpi_data_type;
}

int getRole(int rank, int nRole, int nproc) {
    int bin = nproc / nRole;
    int index = 0;
    int start = 0;
    int end = 0;
    while ((index < nRole) || (end < nproc)) {
        start = index * bin;
        end = (start + bin) - 1;
        if ((rank >= start) && (rank <= end)) {
            return index;
        }
        index = index + 1;
    }
    return index;
}

void trigger_snapshot(MPI_Comm comm, data payload_backup, int num_procs,
                      MPI_Datatype mpi_data_type, MPI_Request requests[]) {
    enum manageTag ctag = mSNAPSHOT;
    for (int other_rank = 0; other_rank < num_procs; other_rank++) {
        MPI_Isend(&payload_backup, 1, mpi_data_type, other_rank, ctag, comm,
                  &requests[other_rank]);
    }
}

void reset_snapshot(MPI_Comm comm, int num_procs, MPI_Request requests[]) {
    enum manageTag ctag = mREVERT;
    for (int other_rank = 0; other_rank < num_procs; other_rank++) {
        MPI_Isend(NULL, 0, MPI_INT, other_rank, ctag, comm,
                  &requests[other_rank]);
    }
}

void handle_snapshot_messages(MPI_Comm comm, int my_rank,
                              MPI_Request requests[], MPI_Status status[],
                              MPI_Datatype mpi_data_type,
                              struct mpi_counter_t *telemetry_msg_cnts) {
    int flag = -1;
    int cnt = 0;
    int ret;
    data recv;
    memset(&recv, 0, sizeof(recv));

    while (1) {
        /* Receive message from any process */
        if (flag != 0) {
            ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE,
                            MPI_ANY_TAG, comm, &requests[my_rank]);

            flag = 0;
        }
        MPI_Test(&requests[my_rank], &flag, &status[my_rank]);

        if (flag != 0) {
            if (ret != MPI_SUCCESS) {
                fprintf(stderr, "Rank %d: Error in MPI_Test: %d\n", my_rank,
                        ret);
                break;  // Exit loop on error
            }

            if (ret == MPI_SUCCESS) {
                enum manageTag tag = status[my_rank].MPI_TAG;
                int source = status[my_rank].MPI_SOURCE;

                if (tag == mSNAPSHOT) {
                    printf(
                        "#################### SAVING SNAPSHOT "
                        "###################");
                    printf(
                        "recv.custom_round_number: %d, recv.round_number: %d, "
                        "recv.value: %d\n",
                        recv.custom_round_number, recv.round_number,
                        recv.value);
                    printf(
                        "##################### END SNAPSHOT "
                        "####################");

                } else if (tag == mREVERT) {
                    printf(
                        "#################### DELETE SNAPSHOT "
                        "###################");
                    printf("PERFORM CUSTOM LOGIC FOR MANAGING SNAPSHOT");
                    printf(
                        "##################### END SNAPSHOT "
                        "####################");
                }

                cnt = increment_counter(telemetry_msg_cnts, 1);
            }
            flag = -1;
        }

        if (cnt > 0) {
            if (!flag) MPI_Cancel(&requests[my_rank]);
            break;
        }
    }
}

void clientLogic(MPI_Comm row_comm[], MPI_Request requests[],
                 MPI_Datatype mpi_data_type, int num_procs, int nRole) {
    // printf("We have %i processes.\n", num_procs);
    float probability = 0.9;
    data package;
    memset(&package, 0, sizeof(package));
    // create your data here
    package.value = (rand() > probability) ? 40 : 1;
    package.round_number = (int)time(NULL);
    package.custom_round_number = EMPTY;
    int NUM_REQUESTS = num_procs / nRole;
    int start = PROPOSER * NUM_REQUESTS;
    int end = start + NUM_REQUESTS;

    enum msgTag tag = mPROPOSE;
    for (int other_rank = start; other_rank < end; other_rank++) {
        // send to proposers
        MPI_Isend(&package, 1, mpi_data_type, other_rank, tag,
                  row_comm[PROPOSER], &requests[other_rank]);
    }
}

void proposerLogic(MPI_Comm row_comm[], MPI_Request requests[],
                   MPI_Status status[], MPI_Datatype mpi_data_type, int my_rank,
                   int num_procs, int nRole,
                   struct mpi_counter_t *prop_round_number,
                   struct mpi_counter_t *prop_current_value,
                   struct mpi_counter_t *prop_max_promise_round_number,
                   struct mpi_counter_t *prop_promise_cnt,
                   struct mpi_counter_t *prop_acks,
                   struct mpi_counter_t *prop_msg_cnts) {
    int cnt = 0;
    int flag = -1;
    int ret;
    int NUM_REQUESTS = num_procs / nRole;
    int threshold = 3 * NUM_REQUESTS;

    int round_number = EMPTY;   // proposer current round number
    int current_value = EMPTY;  // proposer current value

    int max_promise_round_number = EMPTY;

    int promise_cnt = 0;
    int acks = 0;

    int start = 0;
    int end = 0;

    printf("proposer \n");
    data accept_value;  // get result after consensus
    memset(&accept_value, 0, sizeof(accept_value));

    data recv;
    memset(&recv, 0, sizeof(recv));

    double start_time;
    double current_time;
    double timeout = TIMEOUT_SEC;

    while (1) {
        /* Receive message from any process */
        if (flag != 0) {
            ret =
                MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG,
                          row_comm[PROPOSER], &requests[my_rank]);

            flag = 0;
            start_time = MPI_Wtime();  // Start timing after posting the receive
        }
        MPI_Test(&requests[my_rank], &flag, &status[my_rank]);

        if (flag != 0) {
            if (ret != MPI_SUCCESS) {
                fprintf(stderr, "Rank %d: Error in MPI_Test: %d\n", my_rank,
                        ret);
                break;  // Exit loop on error
            }

            if (ret == MPI_SUCCESS) {
                enum msgTag tag = status[my_rank].MPI_TAG;
                int source = status[my_rank].MPI_SOURCE;

                if (tag == mPROPOSE) {
                    // store the value reserved for proposer
                    round_number =
                        reset_var(prop_round_number, recv.round_number);
                    current_value = reset_var(prop_current_value, recv.value);
                    max_promise_round_number = reset_var(
                        prop_max_promise_round_number, recv.round_number);

                    enum msgTag ctag = mPREPARE;
                    start = ACCEPTOR * NUM_REQUESTS;
                    end = start + NUM_REQUESTS;

                    for (int other_rank = start; other_rank < end;
                         other_rank++) {
                        MPI_Isend(&recv, 1, mpi_data_type, other_rank, ctag,
                                  row_comm[ACCEPTOR], &requests[other_rank]);
                    }
                }

                else if (tag == mPROMISE) {
                    if (recv.custom_round_number == round_number) {
                        enum msgTag ctag = mACCEPT;
                        promise_cnt = increment_counter(prop_promise_cnt, 1);
                        if (max_promise_round_number <= recv.round_number) {
                            max_promise_round_number =
                                reset_var(prop_max_promise_round_number,
                                          recv.round_number);

                            accept_value = recv;
                        }

                        if (promise_cnt == (int)MAX((NUM_REQUESTS / 2.0), 1)) {
                            start = ACCEPTOR * NUM_REQUESTS;
                            end = start + NUM_REQUESTS;
                            if (accept_value.value == EMPTY) {
                                accept_value.value = current_value;
                            }

                            for (int other_rank = start; other_rank < end;
                                 other_rank++) {
                                MPI_Isend(&accept_value, 1, mpi_data_type,
                                          other_rank, ctag, row_comm[ACCEPTOR],
                                          &requests[other_rank]);
                            }
                        }
                    }
                }

                else if (tag == mACCEPTED) {
                    if (recv.custom_round_number == round_number) {
                        enum msgTag ctag = mDECIDE;
                        acks = increment_counter(prop_acks, 1);

                        if (acks == (int)MAX((NUM_REQUESTS / 2.0), 1)) {
                            start = LEARNER * NUM_REQUESTS;
                            end = start + NUM_REQUESTS;

                            for (int other_rank = start; other_rank < end;
                                 other_rank++) {
                                MPI_Isend(&accept_value, 1, mpi_data_type,
                                          other_rank, ctag, row_comm[LEARNER],
                                          &requests[other_rank]);
                            }
                        }
                    }
                }

                else if (tag == mNACK) {
                    printf(
                        "recv.custom_round_number: %d, recv.round_number: %d, "
                        "recv.value: %d, round_number: %d\n",
                        recv.custom_round_number, recv.round_number, recv.value,
                        round_number);
                    if (recv.custom_round_number == round_number) {
                        printf("inside line 269: nack was received\n");
                        round_number = 0;
                        break;
                    }
                }
                cnt = increment_counter(prop_msg_cnts, 1);
                printf("proposer cnt: %d, threshold: %d\n", cnt, threshold);
            }
            flag = -1;
        } else {
            current_time = MPI_Wtime();
            if (current_time - start_time > timeout && flag == 0 &&
                ret != MPI_ERR_REQUEST) {
                printf("Rank %d: Timeout waiting for vote.\n", my_rank);
                break;  // Exit loop on timeout
            }
            //usleep(10000);  // Optional delay
        }
        if (cnt == threshold) {
            if (!flag) MPI_Cancel(&requests[my_rank]);
            break;
        }
    }
}

void acceptorLogic(MPI_Comm row_comm[], MPI_Request requests[],
                   MPI_Status status[], MPI_Datatype mpi_data_type, int my_rank,
                   int num_procs, int nRole,
                   struct mpi_counter_t *accept_round_number_promise,
                   struct mpi_counter_t *accept_current_value_accepted,
                   struct mpi_counter_t *accept_round_number_accepted,
                   struct mpi_counter_t *accept_msg_cnts) {
    int cnt = 0;
    int flag = -1;
    int ret;
    int NUM_REQUESTS = num_procs / nRole;
    int threshold = (2 * NUM_REQUESTS);
    int start = 0;
    int end = 0;

    int round_number_promise = EMPTY;    // promise not to accept lower round
    int round_number_accepted = EMPTY;   // round number value is accepted
    int current_value_accepted = EMPTY;  // proposer current value

    printf("acceptor \n");

    data recv;
    memset(&recv, 0, sizeof(recv));

    double start_time;
    double current_time;
    double timeout = TIMEOUT_SEC;

    while (1) {
        /* Receive message from any process */
        if (flag != 0) {
            ret =
                MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG,
                          row_comm[ACCEPTOR], &requests[my_rank]);

            flag = 0;
            start_time = MPI_Wtime();  // Start timing after posting the receive
        }
        MPI_Test(&requests[my_rank], &flag, &status[my_rank]);

        if (flag != 0) {
            if (ret != MPI_SUCCESS) {
                fprintf(stderr, "Rank %d: Error in MPI_Test: %d\n", my_rank,
                        ret);
                break;  // Exit loop on error
            }

            if (ret == MPI_SUCCESS) {
                enum msgTag tag = status[my_rank].MPI_TAG;
                int source = status[my_rank].MPI_SOURCE;
                if (tag == mPREPARE) {
                    data payload;
                    memset(&payload, 0, sizeof(payload));

                    enum msgTag ctag;
                    if (round_number_promise <= recv.round_number) {
                        round_number_promise = reset_var(
                            accept_round_number_promise, recv.round_number);
                        ctag = mPROMISE;

                        start = PROPOSER * NUM_REQUESTS;
                        end = start + NUM_REQUESTS;

                        current_value_accepted = reset_var(
                            accept_current_value_accepted, recv.value);
                        //}
                        payload.value =
                            current_value_accepted;  // current value

                        round_number_accepted = reset_var(
                            accept_round_number_accepted, recv.round_number);

                        payload.round_number =
                            round_number_accepted;  // current rounder number
                        payload.custom_round_number = recv.round_number;
                        for (int other_rank = start; other_rank < end;
                             other_rank++) {
                            MPI_Isend(&payload, 1, mpi_data_type, other_rank,
                                      ctag, row_comm[PROPOSER],
                                      &requests[other_rank]);
                        }

                    } else {
                        ctag = mNACK;
                        start = PROPOSER * NUM_REQUESTS;
                        end = start + NUM_REQUESTS;

                        payload = recv;
                        payload.custom_round_number = recv.round_number;
                        for (int other_rank = start; other_rank < end;
                             other_rank++) {
                            MPI_Isend(&payload, 1, mpi_data_type, other_rank,
                                      ctag, row_comm[PROPOSER],
                                      &requests[other_rank]);
                        }
                    }
                }

                else if (tag == mACCEPT) {
                    enum msgTag ctag;

                    data payload;
                    memset(&payload, 0, sizeof(payload));

                    if (round_number_promise <= recv.round_number) {
                        round_number_promise = reset_var(
                            accept_round_number_promise, recv.round_number);

                        round_number_accepted = reset_var(
                            accept_round_number_accepted, recv.round_number);
                        current_value_accepted = reset_var(
                            accept_current_value_accepted, recv.value);

                        payload.value =
                            current_value_accepted;  // current value
                        payload.round_number =
                            round_number_accepted;  // current rounder number
                        payload.custom_round_number = recv.round_number;

                        ctag = mACCEPTED;
                        start = PROPOSER * NUM_REQUESTS;
                        end = start + NUM_REQUESTS;
                        for (int other_rank = start; other_rank < end;
                             other_rank++) {
                            MPI_Isend(&payload, 1, mpi_data_type, other_rank,
                                      ctag, row_comm[PROPOSER],
                                      &requests[other_rank]);
                        }
                    } else {
                        ctag = mNACK;
                        start = PROPOSER * NUM_REQUESTS;
                        end = start + NUM_REQUESTS;

                        payload = recv;
                        payload.custom_round_number = recv.round_number;

                        for (int other_rank = start; other_rank < end;
                             other_rank++) {
                            MPI_Isend(&payload, 1, mpi_data_type, other_rank,
                                      ctag, row_comm[PROPOSER],
                                      &requests[other_rank]);
                        }
                    }
                }

                cnt = increment_counter(accept_msg_cnts, 1);

                printf("acceptors cnt: %d, threshold: %d\n", cnt, threshold);
            }

            flag = -1;
        } else {
            current_time = MPI_Wtime();
            if (current_time - start_time > timeout && flag == 0 &&
                ret != MPI_ERR_REQUEST) {
                printf("Rank %d: Timeout waiting for vote.\n", my_rank);
                break;  // Exit loop on timeout
            }
            //usleep(10000);  // Optional delay
        }
        if (cnt == threshold) {
            if (!flag) MPI_Cancel(&requests[my_rank]);
            break;
        }
    }
}

void learnerLogic(MPI_Comm row_comm[], MPI_Request requests[],
                  MPI_Status status[], MPI_Datatype mpi_data_type, int my_rank,
                  int num_procs, int nRole,
                  struct mpi_counter_t *learner_current_value_decided,
                  struct mpi_counter_t *learner_msg_cnts) {
    int cnt = 0;
    int flag = -1;
    int ret;
    int NUM_REQUESTS = num_procs / nRole;
    int threshold = (1 * NUM_REQUESTS);  // number of message x number of bins
    int current_value_decided = EMPTY;   // decided value

    double start_time;
    double current_time;
    double timeout = TIMEOUT_SEC;

    printf("learner \n");

    data recv;
    memset(&recv, 0, sizeof(recv));

    while (1) {
        /* Receive message from any process */
        if (flag != 0) {
            ret = MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE,
                            MPI_ANY_TAG, row_comm[LEARNER], &requests[my_rank]);

            flag = 0;
            start_time = MPI_Wtime();  // Start timing after posting the receive
        }
        MPI_Test(&requests[my_rank], &flag, &status[my_rank]);

        if (flag != 0) {
            if (ret != MPI_SUCCESS) {
                fprintf(stderr, "Rank %d: Error in MPI_Test: %d\n", my_rank,
                        ret);
                break;  // Exit loop on error
            }

            if (ret == MPI_SUCCESS) {
                enum msgTag tag = status[my_rank].MPI_TAG;
                int source = status[my_rank].MPI_SOURCE;

                printf("line 436 recv.value: %d, recv.round_number: %d\n",
                       recv.value, recv.round_number);
                if (tag == mDECIDE) {
                    if (current_value_decided == EMPTY) {
                        current_value_decided = reset_var(
                            learner_current_value_decided, recv.value);
                    }
                }

                cnt = increment_counter(learner_msg_cnts, 1);

                printf("learner cnt: %d, threshold: %d\n", cnt, threshold);
            }

            flag = -1;
        } else {
            current_time = MPI_Wtime();
            if (current_time - start_time > timeout && flag == 0 &&
                ret != MPI_ERR_REQUEST) {
                printf("Rank %d: Timeout waiting for vote.\n", my_rank);
                break;  // Exit loop on timeout
            }
            //usleep(10000);  // Optional delay
        }
        if (cnt == threshold) {
            if (!flag) MPI_Cancel(&requests[my_rank]);
            break;
        }
    }

    printf("====================================\n");
    printf("====================================\n");
    printf("Decided value is :%d\n", current_value_decided);
    printf("====================================\n");
    printf("====================================\n");

    trigger_snapshot(row_comm[TELEMETRY], recv, num_procs, mpi_data_type,
                     requests);

    // shutfown after deciding and saving snapshot
    // MPI_Abort(MPI_COMM_WORLD, 1); // Abort once decided
}

int main(int argc, char **argv) {
    // const int nRole = 4;
    const int nRole = 5;

    int my_rank, num_procs;
    /* Initialize the infrastructure necessary for communication */
    MPI_Init(&argc, &argv);

    /* Identify this process */
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    /* Find out how many total processes are active */
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    if ((num_procs % nRole) != 0) {
        fprintf(stderr, "Must use multiple of 5 processes for this example\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* Until this point, all programs have been doing exactly the same.
       Here, we check the rank to distinguish the roles of the programs */

    /* create a type for struct data */
    MPI_Datatype mpi_data_type = serialize_data_stamp();

    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];

    printf("You are in rank: %i.\n", my_rank);

    enum role cur_role = getRole(my_rank, nRole, num_procs);

    // Split the communicator based on the color and use the
    // original rank for ordering
    MPI_Comm row_comm[nRole];

    for (int ind = 0; ind < nRole; ind++) {
        MPI_Comm_split(MPI_COMM_WORLD, ind, my_rank, &row_comm[ind]);
        int row_rank, row_size;
        MPI_Comm_rank(row_comm[ind], &row_rank);
        MPI_Comm_size(row_comm[ind], &row_size);
    }

    int rank;

    // proposer
    struct mpi_counter_t *prop_round_number =
        create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_current_value =
        create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_max_promise_round_number =
        create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_promise_cnt =
        create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_acks = create_shared_var(row_comm[PROPOSER], 0);
    struct mpi_counter_t *prop_msg_cnts =
        create_shared_var(row_comm[PROPOSER], 0);

    MPI_Comm_rank(row_comm[PROPOSER], &rank);

    // acceptors
    struct mpi_counter_t *accept_round_number_promise =
        create_shared_var(row_comm[ACCEPTOR], 0);
    struct mpi_counter_t *accept_round_number_accepted =
        create_shared_var(row_comm[ACCEPTOR], 0);
    struct mpi_counter_t *accept_current_value_accepted =
        create_shared_var(row_comm[ACCEPTOR], 0);
    struct mpi_counter_t *accept_msg_cnts =
        create_shared_var(row_comm[ACCEPTOR], 0);

    MPI_Comm_rank(row_comm[ACCEPTOR], &rank);

    // learners
    struct mpi_counter_t *learner_current_value_decided =
        create_shared_var(row_comm[LEARNER], 0);
    struct mpi_counter_t *learner_msg_cnts =
        create_shared_var(row_comm[LEARNER], 0);

    MPI_Comm_rank(row_comm[LEARNER], &rank);

    // telemetry
    struct mpi_counter_t *telemetry_msg_cnts =
        create_shared_var(row_comm[TELEMETRY], 0);

    MPI_Comm_rank(row_comm[TELEMETRY], &rank);

    if (cur_role == CLIENT) {
        clientLogic(row_comm, requests, mpi_data_type, num_procs, nRole);

    } else if (cur_role == PROPOSER) {
        proposerLogic(row_comm, requests, status, mpi_data_type, my_rank,
                      num_procs, nRole, prop_round_number, prop_current_value,
                      prop_max_promise_round_number, prop_promise_cnt,
                      prop_acks, prop_msg_cnts);
    } else if (cur_role == ACCEPTOR) {
        acceptorLogic(row_comm, requests, status, mpi_data_type, my_rank,
                      num_procs, nRole, accept_round_number_promise,
                      accept_current_value_accepted,
                      accept_round_number_accepted, accept_msg_cnts);
    } else if (cur_role == LEARNER) {
        learnerLogic(row_comm, requests, status, mpi_data_type, my_rank,
                     num_procs, nRole, learner_current_value_decided,
                     learner_msg_cnts);
    }

    // retrieve snapshot
    handle_snapshot_messages(row_comm[TELEMETRY], my_rank, requests, status,
                             mpi_data_type, telemetry_msg_cnts);

    MPI_Barrier(row_comm[CLIENT]);
    MPI_Barrier(row_comm[PROPOSER]);  // proposer_requests
    MPI_Barrier(row_comm[ACCEPTOR]);
    MPI_Barrier(row_comm[LEARNER]);
    MPI_Barrier(row_comm[TELEMETRY]);

    for (int ind = 0; ind < nRole; ind++) {
        MPI_Comm_free(&row_comm[ind]);
    }

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

