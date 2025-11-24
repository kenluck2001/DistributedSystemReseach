/********
Code Contribution from Archit Goyal.
********/
#include <mpi.h>
#include <stdio.h>
#include <string.h>

#define MAX_ENTRIES 128
#define MAX_KEY     64
#define MAX_VAL     64
#define MAX_TOTAL   (MAX_ENTRIES * 32)  // when np <= 32

typedef struct {
    char key[MAX_KEY];
    char value[MAX_VAL];
    long ts;       // logical timestamp
    int  node;     // tie-breaker
    int  tomb;     // 0=live, 1=deleted
    int  inuse;
} Entry;

typedef struct {
    Entry e[MAX_ENTRIES];
    int   count;
    long  clock;
    int   nodeId;
} LWWMap;

static void map_init(LWWMap* m, int node) {
    memset(m, 0, sizeof(*m));
    m->nodeId = node;
}

static int find_idx(LWWMap* m, const char* k) {
    for (int i = 0; i < m->count; i++) 
    {
        if (m->e[i].inuse && strncmp(m->e[i].key, k, MAX_KEY) == 0) 
        {
            return i;
        }
    }
    return -1;
}

static void put(LWWMap* m, const char* k, const char* v) {
    m->clock++;
    int i = find_idx(m, k);
    if (i < 0) {
        if (m->count >= MAX_ENTRIES) return;
        i = m->count++;
        m->e[i].inuse = 1;
        strncpy(m->e[i].key, k, MAX_KEY - 1);
        m->e[i].key[MAX_KEY - 1] = '\0';
    }

    strncpy(m->e[i].value, v, MAX_VAL - 1);
    m->e[i].value[MAX_VAL - 1] = '\0';
    m->e[i].ts   = m->clock;
    m->e[i].node = m->nodeId;
    m->e[i].tomb = 0;
}

static void del(LWWMap* m, const char* k) {
    m->clock++;
    int i = find_idx(m, k);
    if (i < 0) {
        if (m->count >= MAX_ENTRIES) return;
        i = m->count++;
        m->e[i].inuse = 1;
        strncpy(m->e[i].key, k, MAX_KEY - 1);
        m->e[i].key[MAX_KEY - 1] = '\0';
        m->e[i].value[0] = '\0';
    }

    m->e[i].ts   = m->clock;
    m->e[i].node = m->nodeId;
    m->e[i].tomb = 1;
}

static int newer(const Entry* a, const Entry* b) {
    if (b->ts != a->ts) return b->ts > a->ts;
    return b->node > a->node;
}

static void global_merge(LWWMap* local, LWWMap* out, MPI_Comm comm) {
    int r, np;
    MPI_Comm_rank(comm, &r);
    MPI_Comm_size(comm, &np);

    int lcount = local->count, counts[64] = {0};
    MPI_Gather(&lcount, 1, MPI_INT, counts, 1, MPI_INT, 0, comm);

    Entry buf[MAX_TOTAL];
    memset(buf, 0, sizeof(buf));

    if (r == 0) 
    {
        int disp = 0;
        for (int p = 0; p < np; p++) 
        {
            int c = counts[p];
            if (c > 0) 
            {
                if (p == 0) 
                {
                    memcpy(&buf[disp], local->e, sizeof(Entry) * c);
                } 
                else 
                {
                    MPI_Recv(&buf[disp], sizeof(Entry) * c, MPI_BYTE, p, 42, comm, MPI_STATUS_IGNORE);
                }
                disp += c;
            }
        }

        LWWMap merged;
        map_init(&merged, r);

        for (int i = 0; i < disp; i++) {
          if (!buf[i].inuse) continue;

          int j = -1;
          for (int k = 0; k < merged.count; k++) {
            if (strncmp(merged.e[k].key, buf[i].key, MAX_KEY) == 0) { j = k; break; }
          }

          if (j < 0) {
            merged.e[merged.count] = buf[i];
            merged.count++;
          } else if (newer(&merged.e[j], &buf[i])) {
            merged.e[j] = buf[i];
          }
        }

        int sz = merged.count;
        MPI_Bcast(&sz, 1, MPI_INT, 0, comm);
        MPI_Bcast(merged.e, sizeof(Entry) * sz, MPI_BYTE, 0, comm);
        *out = merged;

    } 
    else 
    {
        if (lcount > 0) {
            MPI_Send(local->e, sizeof(Entry) * lcount, MPI_BYTE, 0, 42, comm);
        }
        int sz = 0;
        MPI_Bcast(&sz, 1, MPI_INT, 0, comm);
        MPI_Bcast(out->e, sizeof(Entry) * sz, MPI_BYTE, 0, comm);
        out->count = sz;
        out->nodeId = r;
    }
}

static void print_visible(const LWWMap* m, int r) {
    printf("[rank %d] Visible state:\n", r);
    for (int i = 0; i < m->count; i++) 
    {
        if (!m->e[i].tomb) {
            printf("  %s => %s  (ts=%ld, node=%d)\n",
          m->e[i].key, m->e[i].value, m->e[i].ts, m->e[i].node);
        }
    }
}


int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int r, np;
    MPI_Comm_rank(MPI_COMM_WORLD, &r);
    MPI_Comm_size(MPI_COMM_WORLD, &np);

    LWWMap local;
    map_init(&local, r);

    if (r == 0) { put(&local, "a", "v1@r0"); put(&local, "b", "v1@r0"); }
    if (r == 1) { put(&local, "a", "v2@r1"); del(&local, "b"); }
    if (r == 2) { put(&local, "c", "v1@r2"); }
    if (r == 3) { put(&local, "c", "v2@r3"); }

    LWWMap merged;
    memset(&merged, 0, sizeof(merged));

    global_merge(&local, &merged, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    print_visible(&merged, r);

    MPI_Finalize();
    return 0;
}

