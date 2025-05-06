#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Simple stabilization algorithm:
// Each process periodically checks if its local value is "stable"
// (e.g., within a threshold of the average). If not, it adjusts
// its value towards the average.

// Function to calculate the average of local values
float calculate_average(float local_value, MPI_Comm comm) {
    float total_value = 0.0f;
    int num_processes;
    MPI_Comm_size(comm, &num_processes);

    MPI_Allreduce(&local_value, &total_value, 1, MPI_FLOAT, MPI_SUM, comm);

    return total_value / num_processes;
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    const int MAX_ITERATIONS = 20;
    const float STABILITY_THRESHOLD = 5.0f; // Adjust as needed
    const int SLEEP_TIME = 1;              // Sleep time in seconds

    // Initialize local value with some random variation
    float local_value = (float)rank * 10.0f + (rand() % 10);
    printf("Process %d: Initial value = %.2f\n", rank, local_value);

    for (int iteration = 0; iteration < MAX_ITERATIONS; ++iteration) {
        // 1. Calculate the average value
        float average_value = calculate_average(local_value, MPI_COMM_WORLD);

        // 2. Check for stability
        float difference = fabs(local_value - average_value);
        if (difference > STABILITY_THRESHOLD) {
            // 3. Adjust local value towards the average
            if (local_value > average_value) {
                local_value -= 1.0f; // Simple adjustment
            } else {
                local_value += 1.0f;
            }
            printf("Process %d: Value adjusted to %.2f (avg=%.2f, diff=%.2f)\n",
                   rank, local_value, average_value, difference);
        } else {
            printf("Process %d: Value stable at %.2f (avg=%.2f, diff=%.2f)\n",
                   rank, local_value, average_value, difference);
        }

        MPI_Barrier(MPI_COMM_WORLD); // Synchronize all processes
        sleep(SLEEP_TIME);
    }

    printf("Process %d: Final value = %.2f\n", rank, local_value);

    MPI_Finalize();
    return 0;
}
