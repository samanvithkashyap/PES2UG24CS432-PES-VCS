// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"
//
// Example single entry (conceptual):
//   "100644 hello.txt\0" followed by 32 raw bytes of SHA-256

#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1; // Malformed data

        // Parse mode into an isolated buffer
        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1; // Skip space

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1; // Malformed data

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0'; // Ensure null-terminated

        ptr = null_byte + 1; // Skip null byte

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1; 
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

// Helper for qsort to ensure consistent tree hashing
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
// Returns 0 on success, -1 on error.
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    // Estimate max size: (6 bytes mode + 1 byte space + 256 bytes name + 1 byte null + 32 bytes hash) per entry
    size_t max_size = tree->count * 296; 
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    // Create a mutable copy to sort entries (Git requirement)
    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        
        // Write mode and name (%o writes octal correctly for Git standards)
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // +1 to step over the null terminator written by sprintf
        
        // Write binary hash
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
//
// HINTS - Useful functions and concepts for this phase:
//   - index_load      : load the staged files into memory
//   - strchr          : find the first '/' in a path to separate directories from files
//   - strncmp         : compare prefixes to group files belonging to the same subdirectory
//   - Recursion       : you will likely want to create a recursive helper function 
//                       (e.g., `write_tree_level(entries, count, depth)`) to handle nested dirs.
//   - tree_serialize  : convert your populated Tree struct into a binary buffer
//   - object_write    : save that binary buffer to the store as OBJ_TREE
//
// Returns 0 on success, -1 on error.
// Forward declaration (implemented in object.c).
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// Recursive helper: builds one tree object covering index entries [start, end)
// that all share `prefix_len` bytes of common path prefix. Writes the tree
// to the object store and returns its hash via id_out.
static int build_tree(const Index *idx, int start, int end,
                      size_t prefix_len, ObjectID *id_out) {
    Tree tree;
    tree.count = 0;

    int i = start;
    while (i < end) {
        const char *path = idx->entries[i].path;
        const char *rest = path + prefix_len;       // path segment below this dir
        const char *slash = strchr(rest, '/');

        if (!slash) {
            // Plain file at this level → add a blob entry
            TreeEntry *e = &tree.entries[tree.count++];
            e->mode = idx->entries[i].mode;
            e->hash = idx->entries[i].hash;
            snprintf(e->name, sizeof(e->name), "%s", rest);
            i++;
        } else {
            // Subdirectory. Group contiguous entries that continue into
            // the same child directory.
            size_t child_name_len = (size_t)(slash - rest);
            size_t child_prefix_len = prefix_len + child_name_len + 1; // include '/'

            int group_start = i;
            int j = i + 1;
            while (j < end) {
                const char *p = idx->entries[j].path;
                if (strlen(p) < child_prefix_len) break;
                if (memcmp(p, idx->entries[group_start].path, child_prefix_len) != 0) break;
                j++;
            }

            // Recurse to build the subtree
            ObjectID sub_id;
            if (build_tree(idx, group_start, j, child_prefix_len, &sub_id) != 0)
                return -1;

            TreeEntry *e = &tree.entries[tree.count++];
            e->mode = 0040000;        // directory mode
            e->hash = sub_id;
            if (child_name_len >= sizeof(e->name)) return -1;
            memcpy(e->name, rest, child_name_len);
            e->name[child_name_len] = '\0';

            i = j;
        }

        if (tree.count >= MAX_TREE_ENTRIES) return -1;
    }

    // Serialize this tree and write it to the object store.
    void *serialized;
    size_t serialized_len;
    if (tree_serialize(&tree, &serialized, &serialized_len) != 0) return -1;

    int rc = object_write(OBJ_TREE, serialized, serialized_len, id_out);
    free(serialized);
    return rc;
}

int tree_from_index(ObjectID *id_out) {
    Index idx;
    if (index_load(&idx) != 0) return -1;

    return build_tree(&idx, 0, idx.count, 0, id_out);
}