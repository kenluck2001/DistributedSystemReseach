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
#define MAXNUMOFTHREADS 25

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

typedef struct data_stamp {
    char key[KEYLENGTH];
    int value;
    int lamport_vec[MAXNUMOFTHREADS];
} data;



MPI_Datatype serialize_data_stamp(int num_procs) {
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

void initialize_process_data(int my_rank, int num_procs,
                                 data *package) {
    float probability = (my_rank == 0) ? 0.5 : 0.1;
    strncpy(package->key, "KEN", KEYLENGTH);
    package->value = (rand() > probability) ? 40 : 1;
    for (int i = 0; i < num_procs; i++) {
        package->lamport_vec[i] = 0;
    }
    package->lamport_vec[my_rank] = 1;
}

/**
 * @brief Sends the initial data package to all other processes.
 * @param package The data package to send.
 * @param num_procs The total number of processes.
 * @param mpi_data_type The MPI datatype for the data struct.
 * @param requests Array to store MPI requests.
 */
void send_initial_data(data *package, int num_procs,
                       MPI_Datatype mpi_data_type, MPI_Request *requests) {
    for (int other_rank = 0; other_rank < num_procs; other_rank++) {
        MPI_Isend(package, 1, mpi_data_type, other_rank, 0, MPI_COMM_WORLD,
                  &requests[other_rank]);
    }
}

/**
 * @brief Receives and processes data from other processes to reach agreement.
 * @param my_rank The rank of the current process.
 * @param num_procs The total number of processes.
 * @param mpi_data_type The MPI datatype for the data struct.
 * @param map_array The local key-value map.
 * @param agreed_value Pointer to the struct to store the agreed value.
 * @param requests Array of MPI requests.
 * @param status Array of MPI status.
 */
void receive_and_process_data(int my_rank, int num_procs,
                             MPI_Datatype mpi_data_type, int *map_array,
                             data *agreed_value,
                             MPI_Request *requests, MPI_Status *status) {
    int largest_timestamp = -99999;
    int threshold = num_procs;
    int received_count = 0;
    int flag = 0;
    MPI_Request recv_request;
    data recv_data;
    memset(&recv_data, 0, sizeof(recv_data));

    MPI_Irecv(&recv_data, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG,
              MPI_COMM_WORLD, &recv_request);

    while (received_count < threshold) {
        MPI_Test(&recv_request, &flag, &status[my_rank]);
        if (flag) {
            int source = status[my_rank].MPI_SOURCE;
            for (int i = 0; i < num_procs; i++) {
                recv_data.lamport_vec[my_rank] =
                    MAX(recv_data.lamport_vec[my_rank],
                        recv_data.lamport_vec[source]) +
                    1;
            }

            if (largest_timestamp < recv_data.lamport_vec[source]) {
                largest_timestamp = recv_data.lamport_vec[source];
                *agreed_value = recv_data;
                setKeyValueToMap(&map_array, recv_data.key, recv_data.value);
            }

            if (source != my_rank) {
                MPI_Isend(&recv_data, 1, mpi_data_type, source, 0,
                          MPI_COMM_WORLD, &requests[source]);
            }
            received_count++;
            flag = 0; // Reset flag for the next receive
            MPI_Irecv(&recv_data, 1, mpi_data_type, MPI_ANY_SOURCE, MPI_ANY_TAG,
                      MPI_COMM_WORLD, &recv_request);
        }
        //usleep(10); // Add a small delay to avoid excessive CPU usage
    }
    MPI_Cancel(&recv_request);
    MPI_Wait(&recv_request, MPI_STATUS_IGNORE);
}



/**
 * @brief Prints the agreed key-value pair and retrieves and prints values from the
 * map.
 * @param my_rank The rank of the current process.
 * @param agreed_value The agreed data value.
 * @param map_array The map array.
 */
void print_and_retrieve_map_data(int my_rank, data *agreed_value,
                                 int *map_array) {
    printf("latest Agreed key: %s, value: %d at rank: %d\n",
           agreed_value->key, agreed_value->value, my_rank);
    char *key = agreed_value->key;
    int store_Value = getKeyValueToMap(&map_array, key);

    if (store_Value != EMPTY) {
        printf(
            "For key: %s, retrieved value: %i, available in map at rank: %d\n",
            key, store_Value, my_rank);
    } else {
        printf("For key: %s, has no value available in map at rank: %d\n", key,
               my_rank);
    }

    key = "dist";
    store_Value = getKeyValueToMap(&map_array, key);

    if (store_Value != EMPTY) {
        printf(
            "For key: %s, retrieved value: %i, available in map at rank: %d\n",
            key, store_Value, my_rank);
    } else {
        printf("For key: %s, has no value available in map at rank: %d\n", key,
               my_rank);
    }
}



int main(int argc, char **argv) {
    int my_rank, num_procs;
    /* Initialize the infrastructure necessary for communication */
    MPI_Init(&argc, &argv);

    /* Identify this process */
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    /* Find out how many total processes are active */
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    if (num_procs > MAXNUMOFTHREADS) {
        fprintf(stderr, "Must more than %d process for this example\n", num_procs);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* Until this point, all programs have been doing exactly the same.
       Here, we check the rank to distinguish the roles of the programs */

    MPI_Datatype mpi_data_type = serialize_data_stamp(num_procs);
    printf("You are in rank: %i processes.\n", my_rank);

    int *map_array;
    createMapArray(&map_array, TABLESIZE, EMPTY);

    data package;
    memset(&package, 0, sizeof(package));

    data agreed_value;
    memset(&agreed_value, 0, sizeof(agreed_value));

    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];

    initialize_process_data(my_rank, num_procs, &package);
    send_initial_data(&package, num_procs, mpi_data_type, requests);
    receive_and_process_data(my_rank, num_procs, mpi_data_type, map_array,
                             &agreed_value, requests, status);
    print_and_retrieve_map_data(my_rank, &agreed_value, map_array);

    free(map_array);


    MPI_Type_free(&mpi_data_type);
    /* Tear down the communication infrastructure */
    MPI_Finalize();
    return 0;
}
