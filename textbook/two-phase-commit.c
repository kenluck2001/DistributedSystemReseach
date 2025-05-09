/*****************************************************************

Name: Kenneth Emeka Odoh

This is a demonstrations of two-phase commit with message-passing.

How to run the source code
===========================
mpicc two-phase-commit.c && mpiexec -n 4 ./a.out
mpicc two-phase-commit.c && mpiexec -n 8 ./a.out

/*****************************************************************/

#include <assert.h>
#include <math.h>
#include <mpi.h>
#include <stddef.h>  // used for offsetof
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))
#define EMPTY -999
#define NUM_OF_OPERATION 2
#define TIMEOUT_SEC 5.0  // Timeout in seconds

// Define voting
#define NO 0
#define YES 1
#define NEUTRAL -1

// Define function types

typedef char *(*create_type)(char *);
typedef int (*save_type)(char *);

struct mpi_counter_t {
    MPI_Win win;
    int hostrank;
    int myval;
    int *data;
    int rank, size;
};

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

// Define a structure to hold function pointers and their types
typedef struct {
    union {
        create_type create_func;
        save_type save_func;
    } func;
} transaction_t;

char *create_string(char *str) {
    printf("%s\n", str);
    return str;
}

int save_string(char *str) {
    // save using any logic of yours
    printf("%s%s\n", str, str);
    return 0;
}

enum msgTag { CANCOMMIT, COMMIT, PRECOMMIT, ABORT, VOTE, NOVOTE };
enum role { COORDINATOR, PARTICIPANT };

void canCommit(MPI_Comm comm, int num_procs, MPI_Request requests[]) {
    enum msgTag ctag = CANCOMMIT;
    int vote = NEUTRAL;
    for (int other_rank = 0; other_rank < num_procs; other_rank++) {
        MPI_Isend(&vote, 1, MPI_INT, other_rank, ctag, comm,
                  &requests[other_rank]);
    }
}

void handleCordinatorCommittededMsg(MPI_Comm recv_comm, MPI_Comm send_comm,
                                    int my_rank, int num_procs,
                                    MPI_Request requests[], MPI_Status status[],
                                    transaction_t functions[]) {
    /**
     * Avoid using shared variable as we assume it only run in rank 0 thereby
     * keeping every context
     */
    int flag = -1;
    int ret;
    int recv = 0;
    int threshold = num_procs - 1;

    int cnt = 0;
    int vote = 0;
    int isAborted = 1;

    double start_time;
    double current_time;
    double timeout = TIMEOUT_SEC;

    while (1) {
        /* Receive message from any process */
        if (flag != 0) {
            ret = MPI_Irecv(&recv, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG,
                            recv_comm, &requests[my_rank]);
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

                if (tag == VOTE) {
                    vote += 1;
                } else if (tag == NOVOTE) {
                    isAborted = 1;
                    break;
                }
                cnt += 1;
            }
            flag = -1;
        } else {
            current_time = MPI_Wtime();
            if (current_time - start_time > timeout && flag == 0 &&
                ret != MPI_ERR_REQUEST) {
                printf("Rank %d: Timeout waiting for vote.\n", my_rank);
                isAborted = 1;
                break;  // Exit loop on timeout
            }
            usleep(10000);  // Optional delay
        }

        printf("vote: %d, cnt: %d\n", vote, cnt);

        if (vote == threshold) {
            enum msgTag ctag = COMMIT;
            int commit_val = YES;
            // exclude coordinator, send to participant
            for (int other_rank = 1; other_rank < num_procs; other_rank++) {
                MPI_Isend(&commit_val, 1, MPI_INT, other_rank, ctag, send_comm,
                          &requests[other_rank]);
            }

            // Execute the transaction here
            printf("================================\n");
            printf("CORDINATOR COMMIT SUCCEEDED, rank: %d\n", my_rank);
            char *result_str = functions[0].func.create_func("hello kenneth\n");
            printf("Result str: %s\n", result_str);

            int result_int = functions[1].func.save_func("hello kenneth\n");
            printf("Result int: %d\n", result_int);
            printf("================================\n");

            // do not abort
            isAborted = 0;
            break;  // Exit after successful commit decision and sending
        }

        if (cnt == threshold) {
            if (!flag) MPI_Cancel(&requests[my_rank]);
            break;
        }
    }

    if (isAborted) {
        printf("CORDINATOR COMMIT FAILED\n");
        enum msgTag ctag = ABORT;
        int abort_val = NO;
        // exclude coordinator
        for (int other_rank = 1; other_rank < num_procs; other_rank++) {
            MPI_Isend(&abort_val, 1, MPI_INT, other_rank, ctag, send_comm,
                      &requests[other_rank]);
        }

        // Run custom logic for reconciliation and deletion
        // abort the current transaction on this node
        MPI_Abort(recv_comm, 1);  // Abort once decided
        // break; // Exit the while loop after aborting
    }
}

void handleParticipantPreparedMsg(MPI_Comm recv_comm, MPI_Comm send_comm,
                                  int my_rank, int num_procs,
                                  MPI_Request requests[], MPI_Status status[],
                                  struct mpi_counter_t *msg_cnts) {
    int flag = -1;
    int cnt = 0;
    int ret;
    int recv = 0;
    int threshold = num_procs - 1;
    cnt = reset_var(msg_cnts, 0);

    double start_time;
    double current_time;
    double timeout = TIMEOUT_SEC;

    while (1) {
        /* Receive message from any process */
        if (flag != 0) {
            ret = MPI_Irecv(&recv, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG,
                            recv_comm, &requests[my_rank]);
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

                if (tag == CANCOMMIT) {
                    enum msgTag ctag = VOTE;
                    int vote = YES;
                    // Send to coordinator
                    MPI_Isend(&vote, 1, MPI_INT, 0, ctag, send_comm,
                              &requests[0]);
                }
                cnt = increment_counter(msg_cnts, 1);
            }
            flag = -1;
        } else {
            current_time = MPI_Wtime();
            if (current_time - start_time > timeout && flag == 0 &&
                ret != MPI_ERR_REQUEST) {
                printf("Rank %d: Timeout waiting for CANCOMMIT.\n", my_rank);
                // Decide to abort locally and send NOVOTE
                enum msgTag ctag = NOVOTE;
                int novote = NO;
                MPI_Isend(&novote, 1, MPI_INT, 0, ctag, send_comm,
                          &requests[0]);
                break;  // Exit loop on timeout
            }
            usleep(10000);  // Optional delay
        }

        if (cnt == threshold) {
            cnt = reset_var(msg_cnts, 0);
            if (!flag) MPI_Cancel(&requests[my_rank]);
            break;
        }
    }
}

void handleParticipantCommittededMsg(
    MPI_Comm recv_comm, int my_rank, int num_procs,
    struct mpi_counter_t *is_aborted_participant,
    struct mpi_counter_t *msg_cnts, MPI_Request requests[], MPI_Status status[],
    transaction_t functions[]) {
    int flag = -1;
    int cnt = 0;
    int ret;
    int recv = 0;
    int threshold = num_procs - 1;
    cnt = reset_var(msg_cnts, 0);
    int isAborted = reset_var(is_aborted_participant, 1);

    double start_time;
    double current_time;
    double timeout = TIMEOUT_SEC;

    while (1) {
        /* Receive message from any process */
        if (flag != 0) {
            ret = MPI_Irecv(&recv, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG,
                            recv_comm, &requests[my_rank]);
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

                if (tag == COMMIT) {
                    // Execute the transaction here
                    printf("================================\n");
                    printf("PARTICIPANT COMMIT SUCCEEDED, rank: %d\n", my_rank);
                    char *result_str =
                        functions[0].func.create_func("hello kenneth\n");
                    printf("Result str: %s\n", result_str);

                    int result_int =
                        functions[1].func.save_func("hello kenneth\n");
                    printf("Result int: %d\n", result_int);
                    printf("================================\n");
                    isAborted = reset_var(is_aborted_participant, 0);
                    break;  // Exit after processing COMMIT
                } else if (tag == ABORT) {
                    isAborted = reset_var(is_aborted_participant, 1);
                    break;  // Exit after processing ABORT
                }
                cnt = increment_counter(msg_cnts, 1);
            }
            flag = -1;
        } else {
            current_time = MPI_Wtime();
            if (current_time - start_time > timeout && flag == 0 &&
                ret != MPI_ERR_REQUEST) {
                printf("Rank %d: Timeout waiting for COMMIT or ABORT.\n",
                       my_rank);
                isAborted = reset_var(is_aborted_participant, 1);
                break;  // Exit loop on timeout
            }
            usleep(10000);  // Optional delay
        }

        if (cnt == 1)  // Only one message expected from coordinator
        {
            cnt = reset_var(msg_cnts, 0);
            if (!flag) MPI_Cancel(&requests[my_rank]);
            break;
        }
    }

    if (isAborted) {
        printf("PARTICIPANT COMMIT ABORTED\n");
        // Run custom logic for reconciliation and deletion
        // abort the current transaction on this node

        MPI_Abort(recv_comm, 1);  // Abort once decided
        // break; // Exit the while loop after aborting
    }
}

int main(int argc, char **argv) {
    int my_rank, num_procs, nRole = 2;
    MPI_Init(&argc, &argv);

    /* Identify this process */
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    /* Find out how many total processes are active */
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];

    if (num_procs <= 1) {
        fprintf(stderr, "Must more than one process for this example\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Split the communicator based on the color and use the
    // original rank for ordering
    MPI_Comm row_comm[nRole];  //(coordinator and participant)

    int rank;

    for (int ind = 0; ind < nRole; ind++) {
        MPI_Comm_split(MPI_COMM_WORLD, ind, my_rank, &row_comm[ind]);
        int row_rank, row_size;
        MPI_Comm_rank(row_comm[ind], &row_rank);
        MPI_Comm_size(row_comm[ind], &row_size);
    }

    // coordinator
    MPI_Comm_rank(row_comm[COORDINATOR], &rank);

    // participant
    struct mpi_counter_t *commit_msg_cnts =
        create_shared_var(row_comm[PARTICIPANT], 0);
    struct mpi_counter_t *prep_msg_cnts =
        create_shared_var(row_comm[PARTICIPANT], 0);
    struct mpi_counter_t *is_aborted_participant =
        create_shared_var(row_comm[PARTICIPANT], 0);

    MPI_Comm_rank(row_comm[PARTICIPANT], &rank);

    printf("Total number of processes: %d, rank: %d\n", num_procs, my_rank);

    // Create an array of function wrappers
    transaction_t functions[NUM_OF_OPERATION];
    functions[0].func.create_func = create_string;
    functions[1].func.save_func = save_string;

    if (my_rank == 0) {
        // Coordinator
        printf("Entering Coordinator\n");
        canCommit(row_comm[PARTICIPANT], num_procs, requests);  // 1 -> 2
        handleCordinatorCommittededMsg(
            row_comm[COORDINATOR], row_comm[PARTICIPANT], my_rank, num_procs,
            requests, status, functions);  // 3 -> 4
        printf("Leaving Coordinator\n");
    } else {
        // Participant
        printf("Entering Participant\n");
        handleParticipantPreparedMsg(
            row_comm[PARTICIPANT], row_comm[COORDINATOR], my_rank, num_procs,
            requests, status, prep_msg_cnts);  // 2 -> 3
        handleParticipantCommittededMsg(
            row_comm[PARTICIPANT], my_rank, num_procs, is_aborted_participant,
            commit_msg_cnts, requests, status,
            functions);  // 4 -> 5 WE IGNORED TRANSITING TO DONE ON THE
                         // OCRDINATOR
        printf("Leaving Participant\n");
    }

    MPI_Barrier(row_comm[COORDINATOR]);
    MPI_Barrier(row_comm[PARTICIPANT]);

    for (int ind = 0; ind < nRole; ind++) {
        MPI_Comm_free(&row_comm[ind]);
    }

    delete_counter(&is_aborted_participant);
    delete_counter(&commit_msg_cnts);
    delete_counter(&prep_msg_cnts);

    MPI_Finalize();
    return 0;
}

