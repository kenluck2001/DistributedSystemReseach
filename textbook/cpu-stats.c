#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <limits.h>

// https://gist.github.com/AdjWang/c861081fcdbd31cba3356620785c3e60

size_t get_line_size() {
    // https://man7.org/linux/man-pages/man3/fgets.3p.html
    const size_t kLineMax = 16384u;
    size_t line_max;
    if (LINE_MAX >= kLineMax) {
        line_max = kLineMax;
    } else {
        long limit = sysconf(_SC_LINE_MAX);
        line_max =
            (limit < 0 || (size_t)limit > kLineMax) ? kLineMax : (size_t)limit;
    }
    return line_max + 1;
}


int run_cmd(const char* cmd, char* output, size_t n) {
    if (cmd == NULL) {
        return 0;
    }
    FILE* fp = popen(cmd, "r");
    if (fp == NULL) {
        fprintf(stderr, "failed. cmd=%s\n", cmd);
        perror("popen failed\n");
        return -1;
    }
    if (output == NULL || n == 0) {
        if (pclose(fp) == -1) {
            perror("pclose failed\n");
        }
        return -1;
    }
    size_t line_size = get_line_size();
    char* line = (char*)malloc(line_size);
    if (line == NULL) {
        fprintf(stderr, "malloc size=%ld failed\n", line_size);
        perror("malloc failed\n");
        if (pclose(fp) == -1) {
            perror("pclose failed\n");
        }
        return -1;
    }
    size_t size_remain = n;
    char* dest = output;
    while (size_remain > 0 && fgets(line, line_size, fp) != NULL) {
        size_t s = strlen(line);
        size_t size_copy = size_remain < s ? size_remain : s;
        memcpy(dest, line, size_copy);
        size_remain -= size_copy;
        dest += size_copy;
    }
    *dest = '\0';
    free(line);
    if (pclose(fp) == -1) {
        perror("pclose failed\n");
        return -1;
    }
    return 0;
}


double getCPUStat() {
    // execute the command and print the result
    double local_sum;
    char output[LINE_MAX];
    char command[LINE_MAX];
    // average % CPU usage
    snprintf(command, sizeof(command), "%s | %s | %s | %s", "top -bn1", "grep 'Cpu(s)'", "sed 's/.*, *\\([0-9.]*\\)%* id.*/\\1/'", "awk '{print 100 - $1}'");

    //printf ("##########################\n");
    run_cmd(command, output, LINE_MAX);
    //printf("%s", output);

    char *ptr;
    local_sum = strtod(output, &ptr);
    return local_sum;
}


int main(int argc, char **argv) {
    int my_rank, num_procs;
    double local_sum, global_sum = 0.0, global_avg = 0.0;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

    local_sum = getCPUStat();
    //printf("Local CPU value: %f, Rank: %d\n", local_sum, my_rank);

    // Use MPI_Ireduce to perform a global sum non-blocking
    MPI_Request request;
    MPI_Ireduce(&local_sum, &global_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD, &request);

    // Process 0 waits for the global sum to be computed
    if (my_rank == 0) {
        MPI_Wait(&request, MPI_STATUS_IGNORE);
        global_avg = global_sum/ num_procs;
        //printf("Process 0 received global sum: %f, average: %f\n", global_sum, global_avg);
        printf("%f", global_avg);
    }

    MPI_Finalize();
    return 0;
}

