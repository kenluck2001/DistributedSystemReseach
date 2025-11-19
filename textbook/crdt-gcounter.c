/********
Code Contribution from Archit Goyal.
********/
#include <mpi.h>
#include <stdio.h>
#include <string.h>

#define MAX_PROCS 16

typedef struct {
    int counts[MAX_PROCS];   // local CRDT state
} GCounter;

void gcounter_init(GCounter *g) {
    memset(g->counts, 0, sizeof(g->counts));
}

void gcounter_inc(GCounter *g, int rank, int delta) {
    g->counts[rank] += delta;   // grow-only
}

int gcounter_value(const GCounter *g, int world_size) {
    int sum = 0;
    for (int i = 0; i < world_size; i++) {
        sum += g->counts[i];
    }
    return sum;
}

int main(int argc, char **argv) {
    int rank, size;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size > MAX_PROCS) {
        if (rank == 0) {
            fprintf(stderr, "MAX_PROCS (%d) < world size (%d)\n", MAX_PROCS, size);
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    GCounter local;
    GCounter merged;

    gcounter_init(&local);

    // Each process increments its own slot rank+1 times.
    gcounter_inc(&local, rank, rank + 1);

    // Merge using Allreduce with MAX, which is equivalent to CRDT merge
    MPI_Allreduce(
        local.counts,
        merged.counts,
        MAX_PROCS,
        MPI_INT,
        MPI_MAX,
        MPI_COMM_WORLD
    );

    int value = gcounter_value(&merged, size);
    printf("Rank %d sees global counter value = %d\n", rank, value);

    MPI_Finalize();
    return 0;
}

