/*
  "Hello World" MPI Test Program
*/
//https://en.wikipedia.org/wiki/Message_Passing_Interface#Example_program
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <mpi.h>

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

    int lamport_vec[num_procs];

    MPI_Request requests[num_procs];
    MPI_Status status[num_procs];

    //printf("You are in rank: %i processes.\n", my_rank );
    if (my_rank == 0) {

        //MPI_Request request = requests[my_rank];
        //MPI_Status  status = status[my_rank];

        int other_rank;
        int rsource, rdestination;
        printf("We have %i processes.\n", num_procs);

        memset( lamport_vec, 0, num_procs * sizeof(int) );
        lamport_vec[my_rank] = lamport_vec[my_rank] + 1; 
        for (other_rank = 0; other_rank < num_procs; other_rank++)
        {
            MPI_Isend(lamport_vec, num_procs, MPI_INT, other_rank, my_rank, MPI_COMM_WORLD, &requests[my_rank]);
        }

        //MPI_Wait(&requests[my_rank], &status[my_rank]);
        //int ierr = MPI_Waitall(num_procs, requests, status); 
        //if (ierr == MPI_SUCCESS) {
            /* Receive messages from all other process */

            /**
            for (other_rank = 0; other_rank < num_procs; other_rank++)
            {
                MPI_Irecv(lamport_vec, num_procs, MPI_INT, other_rank, 0, MPI_COMM_WORLD, &requests[other_rank]);
                MPI_Wait(&requests[other_rank], &status[other_rank]);
                printArray(other_rank, lamport_vec, num_procs);
            }
            */

        //}

            for (other_rank = 0; other_rank < num_procs; other_rank++)
            {
                MPI_Probe(other_rank, MPI_ANY_TAG, MPI_COMM_WORLD, &status[my_rank]);
                int tag = status[my_rank].MPI_TAG;
                int source = status[my_rank].MPI_SOURCE;
                
                /* Receive message from any process */
                //MPI_Recv(lamport_vec, num_procs, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                MPI_Irecv(lamport_vec, num_procs, MPI_INT, source, tag, MPI_COMM_WORLD, &requests[source]);
                MPI_Wait(&requests[source], &status[source]);
                printArray(other_rank, lamport_vec, num_procs);

            }

    } else {

           /**
            MPI_Status c_status;
            MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &c_status);
            int tag = c_status.MPI_TAG;
            int source = c_status.MPI_SOURCE;
            */

            //MPI_Status c_status;
            MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status[my_rank]);
            int tag = status[my_rank].MPI_TAG;
            int source = status[my_rank].MPI_SOURCE;
            
            /* Receive message from any process */
            //MPI_Recv(lamport_vec, num_procs, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            MPI_Irecv(lamport_vec, num_procs, MPI_INT, source, tag, MPI_COMM_WORLD, &requests[source]);
            MPI_Wait(&requests[source], &status[source]);

           /**
            for (int other_rank = 1; other_rank < num_procs; other_rank++)
            {
                MPI_Irecv(lamport_vec, num_procs, MPI_INT, other_rank, 0, MPI_COMM_WORLD, &requests[other_rank-1]);
                //MPI_Wait(&requests[other_rank-1], &status[other_rank-1]);                
            }
            */
            lamport_vec[my_rank] = MAX(lamport_vec[my_rank], lamport_vec[source]) + 1;         //time


            /* Receive message from any process for telemetry */
            for (int other_rank = 0; other_rank < num_procs; other_rank++)
            {            
                MPI_Isend(lamport_vec, num_procs, MPI_INT, other_rank, 0, MPI_COMM_WORLD, &requests[my_rank]);
            }

        //}
    }
    //int ierr = MPI_Waitall(num_procs, requests, status); 

    /* Tear down the communication infrastructure */
    MPI_Finalize();

    /**
        struct MPI_Struct {
          int MPI_SOURCE;
          int MPI_TAG;
          int MPI_ERROR;
          int _cancelled;
          size_t _ucount;
        };

        double values[5];
        MPI_Status status;
        MPI_Recv(&values, 5, MPI_DOUBLE, MPI_ANY_SOURCE, MPI_ANY_TAG, &status);
        std::cout << "Received from process " << status.MPI_SOURCE
                  << "; with tag " << status.MPI_TAG << std::endl;


        int MPI_Send(void *buf, int count , MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
        int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *status);


        if (rank == 0) {
            sleep(3);
            MPI_Send(buffer, buffer_count, MPI_INT, 1, 0, MPI_COMM_WORLD);
            sleep(6);
            MPI_Send(buffer, buffer_count, MPI_INT, 1, 1, MPI_COMM_WORLD);
          }
          else {
            sleep(5);
            MPI_Recv(buffer, buffer_count, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            sleep(3);
            MPI_Recv(buffer, buffer_count, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
          }

           Testing timing of proceses

            // This file is provided as an example of blocking scenario. Please don't modify it !
            void play_blocking_scenario() {
              // Initialising buffer :
              for (int i=0; i < buffer_count; ++i)
                buffer[i] = (rank == 0 ? i*2 : 0);

              MPI_Barrier(MPI_COMM_WORLD);
              // Starting the chronometer
              double time = -MPI_Wtime(); // This command helps us measuring time. We will see more about it later !
              // Simulate working
              if (rank == 0) {
                sleep(3);
                MPI_Send(buffer, buffer_count, MPI_INT, 1, 0, MPI_COMM_WORLD);
                sleep(6);
                
                // Modifying the buffer for second step
                for (int i=0; i < buffer_count; ++i)
                  buffer[i] = -i;
                
                MPI_Send(buffer, buffer_count, MPI_INT, 1, 1, MPI_COMM_WORLD);
              }
              else {
                sleep(5);
                MPI_Recv(buffer, buffer_count, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                sleep(3);
                MPI_Recv(buffer, buffer_count, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
              }

              time += MPI_Wtime();
              
              // This line gives us the maximum time elapsed on each process.
              // We will see about reduction later on !
              double final_time;
              MPI_Reduce(&time, &final_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

              if (rank == 0)
                std::cout << "Total time for blocking scenario : " << final_time << "s" << std::endl;
            }


            int MPI_Isend(void *buffer, int count, MPI_Datatype datatype, int dest, int tag, MPI_Communicator comm, MPI_Request *request);
             
            int MPI_Irecv(void *buffer, int count, MPI_Datatype datatype, int source, int tag, MPI_Communicator comm, MPI_Request *request);

            int MPI_Wait(MPI_Request *request, MPI_Status *status);
            int MPI_Waitany(int count, MPI_Request array_of_requests[], int *index, MPI_Status *status);

            int MPI_Test(MPI_Request *request, int *flag, MPI_Status *status);
            int MPI_Testany(int count, MPI_Request array_of_requests[], int *index, int *flag, MPI_Status *status);

            ########################################################3

            MPI_Request request;
            MPI_Status  status;
            int 	    request_complete = 0;

            // Rank 0 sends, rank 1 receives
            if (rank == 0) {
              MPI_Isend(buffer, buffer_count, MPI_INT, 1, 0, MPI_COMM_WORLD, &request);

              // Here we do some work while waiting for process 1 to be ready
              while (has_work) {
                do_work();

                // We only test if the request is not already fulfilled
                if (!request_complete)
                   MPI_Test(&request, &request_complete, &status);
              }

              // No more work, we wait for the request to be complete if it's not the case
              if (!request_complete)
                MPI_Wait(&request, &status);
            }
            else {
              MPI_Irecv(buffer, buffer_count, MPI_INT, 0, 0, MPI_COMM_WORLD, &request);

              // Here we just wait for the message to come
              MPI_Wait(&request, &status);
            }


            // Probing the reception of messages
            MPI_Status status;
            MPI_Probe(0, 0, MPI_COMM_WORLD, &status);

            // From the probed status we get the number of elements to receive
            int n_items;
            MPI_Get_count(&status, MPI_INT, &n_items);

            std::cout << "Process 1, probing tells us message will have " << n_items << " ints." << std::endl;

            void probing_process(int &int_sum, float &float_sum) {
              MPI_Status status;
              
              // 1- Probe the incoming message
                MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
              // 2- Get the tag and the source
              int tag = status.MPI_TAG;
              int source = status.MPI_SOURCE;
              

              // Printing the message
              std::cout << "Received a message from process " << source << " with tag " << tag << std::endl;

              // 3- Add to int_sum or float_sum depending on the tag of the message
                if (tag==0) {
                    int res;
                    MPI_Recv(&res, 1, MPI_INT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    int_sum += res;
                }
                else if (tag==1){
                    float res;
                    MPI_Recv(&res, 1, MPI_FLOAT, source, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    float_sum += res;
                }
            }


            void my_bcast(void* data, int count, MPI_Datatype datatype, int root,
                          MPI_Comm communicator) {
              int world_rank;
              MPI_Comm_rank(communicator, &world_rank);
              int world_size;
              MPI_Comm_size(communicator, &world_size);

              if (world_rank == root) {
                // If we are the root process, send our data to everyone
                int i;
                for (i = 0; i < world_size; i++) {
                  if (i != world_rank) {
                    MPI_Send(data, count, datatype, i, 0, communicator);
                  }
                }
              } else {
                // If we are a receiver process, receive the data from the root
                MPI_Recv(data, count, datatype, root, 0, communicator,
                         MPI_STATUS_IGNORE);
              }
            }

            ##############################################################


            // TODO : create a buffer called reception and call MPI_Reduce to sum all the variables
              // over all the processes and store the result on process 0.
              // In the end, you should have buffer_count variables.

              float reception[buffer_count];
              MPI_Barrier(MPI_COMM_WORLD);
              MPI_Reduce(buffer, reception, buffer_count, MPI_FLOAT, MPI_SUM, 0, MPI_COMM_WORLD); 


              // Now we print the results
              if (rank == 0) {
                for (int i=0; i < buffer_count; ++i)
                  std::cout << reception[i] << std::endl;
              }

            #####################################################

            int* send_buf;
            int recv_buf[1000];

            if (rank == 0) {
               send_buf = new int[100];
               init_send_buf(send_buf);
            }

            MPI_Scatter(send_buf, 5, MPI_INT, recv_buf, 5, MPI_INT, 0, MPI_COMM_WORLD);

            ##measuring time
            https://www.codingame.com/playgrounds/349/introduction-to-mpi/measuring-time


            #####################################################


            void play_non_blocking_scenario() {
              MPI_Request request;
              MPI_Status  status;
              int request_finished = 0;

              // Initialising buffer :
              for (int i=0; i < buffer_count; ++i)
                buffer[i] = (rank == 0 ? i*2 : 0);

              MPI_Barrier(MPI_COMM_WORLD);
              // Starting the chronometer
              double time = -MPI_Wtime(); // This command helps us measure time. We will see more about it later on !

              
              ////////// You should not modify anything BEFORE this point //////////
              
              if (rank == 0) {
                sleep(3);

                // 1- Initialise the non-blocking send to process 1
                // [...]
                MPI_Isend(buffer, buffer_count, MPI_INT, 1, 0, MPI_COMM_WORLD, &request);

                double time_left = 6000.0;
                while (time_left > 0.0) {
                  usleep(1000); // We work for 1ms

                  // 2- Test if the request is finished (only if not already finished)
                  // [...]

                  if (!request_complete)
                   MPI_Test(&request, &request_complete, &status);

                  
                  // 1ms left to work
                  time_left -= 1000.0;
                }

                // 3- If the request is not yet complete, wait here. 
                // [...]
                if (!request_complete)
                    MPI_Wait(&request, &status);

                // Modifying the buffer for second step
                for (int i=0; i < buffer_count; ++i)
                  buffer[i] = -i;

                // 4- Prepare another request for process 1 with a different tag
                // [...]
                MPI_Isend(buffer, buffer_count, MPI_INT, 1, 2, MPI_COMM_WORLD, &request);  
                
                time_left = 3000.0;
                while (time_left > 0.0) {
                  usleep(1000); // We work for 1ms

                  // 5- Test if the request is finished (only if not already finished)
                  // [...]
                  if (!request_complete)
                    MPI_Wait(&request, &status);

                  // 1ms left to work
                  time_left -= 1000.0;
                }
                // 6- Wait for it to finish
                // [...]
                MPI_Wait(&request, &status);
                
              }
              else {
                // Work for 5 seconds
                sleep(5);

                // 7- Initialise the non-blocking receive from process 0
                // [...]
                MPI_Irecv(buffer, buffer_count, MPI_INT, 0, 0, MPI_COMM_WORLD, &request);

                // 8- Wait here for the request to be completed
                // [...]
                MPI_Wait(&request, &status);

                print_buffer();
                
                // Work for 3 seconds
                sleep(3);

                // 9- Initialise another non-blocking receive
                // [...]
                MPI_Irecv(buffer, buffer_count, MPI_INT, 0, 0, MPI_COMM_WORLD, &request);

                
                // 10- Wait for it to be completed
                // [...]
                MPI_Wait(&request, &status);

                print_buffer();
              }
              ////////// should not modify anything AFTER this point //////////

              // Stopping the chronometer
              time += MPI_Wtime();

              // This line gives us the maximum time elapsed on each process.
              // We will see about reduction later on !
              double final_time;
              MPI_Reduce(&time, &final_time, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
              
              if (rank == 0)
                std::cout << "Total time for non-blocking scenario : " << final_time << "s" << std::endl;
            }
            ########################################################


            int ierr=MPI_Waitall(blocknum, requests, status); 
            if(ierr!=MPI_SUCCESS){fprintf(stderr,"MPI_Waitall() failed rank %d\n",rank);exit(1);}




    */


    return 0;
}


/**
MPI_Isend and MPI_Irecv MUST be followed at some point by MPI_Test and MPI_Wait. The process sending should never write in the buffer until the request has been completed. On the other hand, the process receiving should never read in the buffer before the request has been completed. And the only way to know if a request is completed, is to call MPI_Wait and MPI_Test.
*/
