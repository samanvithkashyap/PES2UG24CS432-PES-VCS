// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c
//
// This is intentionally a simple text format. No magic numbers, no
// binary parsing. The focus is on the staging area CONCEPT (tracking
// what will go into the next commit) and ATOMIC WRITES (temp+rename).
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
//
// Identifies files that are staged, unstaged (modified/deleted in working dir),
// and untracked (present in working dir but not in index).
// Returns 0.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    // Note: A true Git implementation deeply diffs against the HEAD tree here. 
    // For this lab, displaying indexed files represents the staging intent.
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            // Fast diff: check metadata instead of re-hashing file content
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            // Skip hidden directories, parent directories, and build artifacts
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue; // compiled executable
            if (strstr(ent->d_name, ".o") != NULL) continue; // object files

            // Check if file is tracked in the index
            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1; 
                    break;
                }
            }
            
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) { // Only list regular files for simplicity
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    return 0;
}
// ─── TODO: Implement these ───────────────────────────────────────────────────

// Forward declaration (implemented in object.c).
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// Load the index from .pes/index.
// Returns 0 on success, -1 on error.
int index_load(Index *index) {
    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) {
        // Missing index = "nothing staged yet" — not an error.
        return 0;
    }

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        if (line[0] == '\n' || line[0] == '\0') continue;
        if (index->count >= MAX_INDEX_ENTRIES) break;

        IndexEntry *e = &index->entries[index->count];

        unsigned int mode;
        char hash_hex[HASH_HEX_SIZE + 1];
        unsigned long long mtime_ll;
        unsigned int size;
        char path[512];

        // Format: <mode-octal> <hash-hex> <mtime> <size> <path>
        int n = sscanf(line, "%o %64s %llu %u %511[^\n]",
                       &mode, hash_hex, &mtime_ll, &size, path);
        if (n != 5) continue;

        e->mode      = mode;
        e->mtime_sec = (uint64_t)mtime_ll;
        e->size      = size;
        snprintf(e->path, sizeof(e->path), "%s", path);

        if (hex_to_hash(hash_hex, &e->hash) != 0) continue;

        index->count++;
    }

    fclose(f);
    return 0;
}

// qsort comparator for sorting index entries by path.
static int compare_index_entries(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path,
                  ((const IndexEntry *)b)->path);
}

// Save the index to .pes/index atomically.
// Returns 0 on success, -1 on error.
int index_save(const Index *index) {
    // IMPORTANT: Index is ~5.7 MB. Allocate the sorted copy on the heap,
    // NOT the stack, or we blow past the 8 MB stack limit and segfault.
    Index *sorted = malloc(sizeof(Index));
    if (!sorted) return -1;
    memcpy(sorted, index, sizeof(Index));
    qsort(sorted->entries, sorted->count, sizeof(IndexEntry), compare_index_entries);

    char tmp_path[256];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%d", INDEX_FILE, (int)getpid());

    FILE *f = fopen(tmp_path, "w");
    if (!f) { free(sorted); return -1; }

    for (int i = 0; i < sorted->count; i++) {
        char hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&sorted->entries[i].hash, hex);
        if (fprintf(f, "%o %s %llu %u %s\n",
                    sorted->entries[i].mode,
                    hex,
                    (unsigned long long)sorted->entries[i].mtime_sec,
                    sorted->entries[i].size,
                    sorted->entries[i].path) < 0) {
            fclose(f);
            unlink(tmp_path);
            free(sorted);
            return -1;
        }
    }

    // Flush userspace buffer, then fsync the fd so bytes hit the disk
    // before we rename over the old index.
    if (fflush(f) != 0)        { fclose(f); unlink(tmp_path); free(sorted); return -1; }
    if (fsync(fileno(f)) != 0) { fclose(f); unlink(tmp_path); free(sorted); return -1; }
    fclose(f);
    free(sorted);

    if (rename(tmp_path, INDEX_FILE) != 0) {
        unlink(tmp_path);
        return -1;
    }
    return 0;
}

// Stage a file for the next commit.
// Returns 0 on success, -1 on error.
int index_add(Index *index, const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) {
        fprintf(stderr, "error: cannot stat '%s'\n", path);
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "error: '%s' is not a regular file\n", path);
        return -1;
    }

    // Read the whole file into memory.
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    size_t file_size = (size_t)st.st_size;
    void *buffer = malloc(file_size ? file_size : 1);
    if (!buffer) { fclose(f); return -1; }

    if (file_size > 0 && fread(buffer, 1, file_size, f) != file_size) {
        free(buffer); fclose(f); return -1;
    }
    fclose(f);

    // Write the contents as a blob.
    ObjectID blob_id;
    if (object_write(OBJ_BLOB, buffer, file_size, &blob_id) != 0) {
        free(buffer);
        return -1;
    }
    free(buffer);

    // Mode: executable or regular.
    uint32_t mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;

    // Update existing entry, or append a new one.
    IndexEntry *existing = index_find(index, path);
    if (existing) {
        existing->mode      = mode;
        existing->hash      = blob_id;
        existing->mtime_sec = (uint64_t)st.st_mtime;
        existing->size      = (uint32_t)file_size;
    } else {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: index is full\n");
            return -1;
        }
        IndexEntry *e = &index->entries[index->count++];
        e->mode      = mode;
        e->hash      = blob_id;
        e->mtime_sec = (uint64_t)st.st_mtime;
        e->size      = (uint32_t)file_size;
        snprintf(e->path, sizeof(e->path), "%s", path);
    }

    return index_save(index);
}