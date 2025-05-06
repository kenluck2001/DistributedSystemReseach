/*****************************************************************
Name: Kenneth Emeka Odoh
This is a demonstrations of how to use a lamport clock to use majority voting to
build a distributed key value hash map.

How to run the source code
===========================
mpicc lamport1-majority-voting8.c && mpiexec -n 4 ./a.out
mpicc lamport1-majority-voting8.c && mpiexec -n 8 ./a.out
mpicc lamport1-majority-voting8.c && mpiexec -n 12 ./a.out

*****************************************************************/

#include <assert.h>
#include <mpi.h>
#include <stddef.h>  // used for offsetof
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

#define TABLESIZE 150
#define EMPTY -999
#define KEYLENGTH 20

/**
 * Routine for map data structure
 */
//////////////////////////////////////////////////////////
void createMapArray(int **array, int length, int value) {
    *array = malloc(length * sizeof(int));
    if (*array == NULL) return;
    for (int i = 0; i < length; i++) (*array)[i] = value;
}

int hash3(const char *key) {
    int HashVal = 0;
    while (*key != '\0') HashVal = (HashVal << 5) + *key++;
    return HashVal % TABLESIZE;
}

void setKeyValueToMap(int **map_array, const char *key, int value) {
    int map_index = hash3(key);
    (*map_array)[map_index] = value;
}

int getKeyValueToMap(int **map_array, const char *key) {
    int map_index = hash3(key);
    return (*map_array)[map_index];
}
//////////////////////////////////////////////////////////

int main(int argc, char **argv) {
    int my_rank, num_procs;
    /* Initialize the infrastructure necessary for communication */
    MPI_Init(&argc, &argv);

    /* Identify this process */
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    /* Find out how many total processes are active */
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    typedef struct data_stamp {
        char key[KEYLENGTH];
        int value;
        int lamport_vec[num_procs];
    } data;

    MPI_Datatype serialize_data_stamp() {
        /* create a type for struct data */
        const int nitems = 3;
        // int          blocklengths[nitems] = {1,num_procs};
        int blocklengths[nitems];

        blocklengths[0] = KEYLENGTH;
        blocklengths[1] = 1;
        blocklengths[2] = num_procs;

        // MPI_Datatype types[nitems] = {MPI_INT, MPI_INT};
        MPI_Datatype types[nitems];
        types[0] = MPI_CHAR;
        types[1] = MPI_INT;
        types[2] = MPI_INT;

        MPI_Datatype mpi_data_type;
        MPI_Aint offsets[nitems];

        offsets[0] = offsetof(data, key);
        offsets[1] = offsetof(data, value);
        offsets[2] = offsetof(data, lamport_vec);

        MPI_Type_create_struct(nitems, blocklengths, offsets, types,
                               &mpi_data_type);
        MPI_Type_commit(&mpi_data_type);
        return mpi_data_type;
    }

    /* Until this point, all programs have been doing exactly the same.
       Here, we check the rank to distinguish the roles of the programs */

    MPI_Datatype mpi_data_type = serialize_data_stamp();

    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];

    printf("You are in rank: %i processes.\n", my_rank);

    if (my_rank == 0) {
        printf("We have %i processes.\n", num_procs);
        srand((my_rank + 1) * time(0));
        int largest_timestamp = -99999;
        float probability = 0.5;
        int threshold = num_procs;  // set the level of resilience
        int cnt = 0;                // number of messages
        int flag = -1;
        int ret;
        char *key;
        int *map_array;  // store map here

        createMapArray(&map_array, TABLESIZE,
                       EMPTY);  // initialize the map here

        data package;
        memset(&package, 0, sizeof(package));
        // create your data here
        strncpy(package.key, "KEN", KEYLENGTH);
        package.value = (rand() > probability) ? 40 : 1;
        package.lamport_vec[my_rank] = package.lamport_vec[my_rank] + 1;

        setKeyValueToMap(&map_array, package.key,
                         package.value);  // add key, value pair to the map

        for (int other_rank = 0; other_rank < num_procs; other_rank++) {
            MPI_Isend(&package, 1, mpi_data_type, other_rank, my_rank,
                      MPI_COMM_WORLD, &requests[other_rank]);
        }

        data agreed_value;  // get result after consensus
        memset(&agreed_value, 0, sizeof(agreed_value));

        data recv;
        memset(&recv, 0, sizeof(recv));
        while (1) {
            /* Receive message from any process */
            if (flag != 0) {
                ret =
                    MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE,
                              MPI_ANY_TAG, MPI_COMM_WORLD, &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);

            if (flag != 0) {
                if (ret == MPI_SUCCESS) {
                    int source = status[my_rank].MPI_SOURCE;
                    recv.lamport_vec[my_rank] = MAX(recv.lamport_vec[my_rank],
                                                    recv.lamport_vec[source]) +
                                                1;

                    if (largest_timestamp < recv.lamport_vec[source]) {
                        largest_timestamp = recv.lamport_vec[source];
                        agreed_value = recv;  // copy struct for consensus value

                        setKeyValueToMap(
                            &map_array, recv.key,
                            recv.value);  // add key, value pair to the map
                    }
                    if (source != my_rank) {
                        MPI_Isend(&recv, 1, mpi_data_type, source, 0,
                                  MPI_COMM_WORLD, &requests[source]);
                    }
                }
                cnt += 1;
                flag = -1;
            }

            if (cnt == threshold) break;
        }
        printf("latest Agreed key: %s, value: %d at rank: %d\n",
               agreed_value.key, agreed_value.value, my_rank);
        key = agreed_value.key;
        int store_Value = getKeyValueToMap(&map_array, key);

        if (store_Value != EMPTY) {
            printf(
                "For key: %s, retrieved value: %i, available in map at rank: "
                "%d\n",
                key, store_Value, my_rank);
        } else {
            printf("For key: %s, has no value available in map at rank: %d\n",
                   key, my_rank);
        }

        key = "dist";
        store_Value = getKeyValueToMap(&map_array, key);

        if (store_Value != EMPTY) {
            printf(
                "For key: %s, retrieved value: %i, available in map at rank: "
                "%d\n",
                key, store_Value, my_rank);
        } else {
            printf("For key: %s, has no value available in map at rank: %d\n",
                   key, my_rank);
        }

        free(map_array);  // free memory
    } else {
        srand((my_rank + 1) * time(0));
        int largest_timestamp = -99999;
        float probability = 0.1;
        int threshold = num_procs;  // set the level of resilience
        int cnt = 0;
        int flag = -1;
        int ret;
        char *key;
        int *map_array;  // store map here

        createMapArray(&map_array, TABLESIZE,
                       EMPTY);  // initialize the map here

        data package;
        memset(&package, 0, sizeof(package));
        // create your data here
        strncpy(package.key, "KEN", KEYLENGTH);

        package.value = (rand() > probability) ? 4 : 1;
        package.lamport_vec[my_rank] = package.lamport_vec[my_rank] + 1;

        for (int other_rank = 0; other_rank < num_procs; other_rank++) {
            MPI_Isend(&package, 1, mpi_data_type, other_rank, my_rank,
                      MPI_COMM_WORLD, &requests[other_rank]);
        }

        data agreed_value;  // get result after consensus
        memset(&agreed_value, 0, sizeof(agreed_value));

        data recv;
        memset(&recv, 0, sizeof(recv));

        while (1) {
            /* Receive message from any process */
            if (flag != 0) {
                ret =
                    MPI_Irecv(&recv, 1, mpi_data_type, MPI_ANY_SOURCE,
                              MPI_ANY_TAG, MPI_COMM_WORLD, &requests[my_rank]);

                flag = 0;
            }
            MPI_Test(&requests[my_rank], &flag, &status[my_rank]);

            if (flag != 0) {
                if (ret == MPI_SUCCESS) {
                    int source = status[my_rank].MPI_SOURCE;
                    recv.lamport_vec[my_rank] = MAX(recv.lamport_vec[my_rank],
                                                    recv.lamport_vec[source]) +
                                                1;

                    if (largest_timestamp < recv.lamport_vec[source]) {
                        largest_timestamp = recv.lamport_vec[source];
                        agreed_value = recv;  // copy struct for consensus value
                        setKeyValueToMap(
                            &map_array, recv.key,
                            recv.value);  // add key, value pair to the map
                    }

                    if (source != my_rank) {
                        MPI_Isend(&recv, 1, mpi_data_type, source, 0,
                                  MPI_COMM_WORLD, &requests[source]);
                    }
                }
                cnt += 1;
                flag = -1;
            }
            if (cnt == threshold) break;
        }

        printf("latest Agreed key: %s, value: %d at rank: %d\n",
               agreed_value.key, agreed_value.value, my_rank);
        key = agreed_value.key;
        int store_Value = getKeyValueToMap(&map_array, key);

        if (store_Value != EMPTY) {
            printf(
                "For key: %s, retrieved value: %i, available in map at rank: "
                "%d\n",
                key, store_Value, my_rank);
        } else {
            printf("For key: %s, has no value available in map at rank: %d\n",
                   key, my_rank);
        }

        key = "dist";
        store_Value = getKeyValueToMap(&map_array, key);

        if (store_Value != EMPTY) {
            printf(
                "For key: %s, retrieved value: %i, available in map at rank: "
                "%d\n",
                key, store_Value, my_rank);
        } else {
            printf("For key: %s, has no value available in map at rank: %d\n",
                   key, my_rank);
        }

        free(map_array);  // free memory
    }

    MPI_Type_free(&mpi_data_type);
    /* Tear down the communication infrastructure */
    MPI_Finalize();
    return 0;
}
