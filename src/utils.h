#include <sys/types.h>

#define NUM_BUCKETS 8191
#define KEY_SPACE (((int) 'z') - ((int) '0'))
#define GET_BUCKET(db, idx) ((db)->h_table->buckets[(idx)])
#define MIN(a, b) ((a)<(b) ? (a) : (b))
#define DB_END(db) (((db)->m_ptr) + ((db)->db_size))

// Since 65535 is max port, port strings can be at most 5 chars + null terminator
#define PORT_STRLEN 6
#define MAX_PORTNUM 65535

// A single bucket in the hash table. 
// Since word points directly into the memory-mapped database, there is no need
// to explicitly keep track of the offset.
typedef struct bucket {
  char *word;
  int used;
} bucket;

typedef struct hash_table {
  int num_buckets; // number of buckets the hash table contains
  bucket *buckets; // an array of size num_buckets
} hash_table;

// The value array associated with a key in the database.
typedef struct value_array {
  int len; 
  unsigned int arr[];
} value_array; 

typedef struct database {
  char *m_ptr;         /* ptr to start of db in memory (in binary postings format) */ 
  size_t db_size;      /* size of db in bytes */
  hash_table *h_table; /* hash table used to efficiently search the database */
} database;

/* -------------------- Parent Process Helper Functions --------------------- */

database *load_database(char *db_filename);
char *get_partition(database *db, int total_nodes, int node_id, size_t *length);

/* ------------------ Hash Table Related Helper Functions ------------------- */

void build_hash_table(database *db);

int lookup_insert(hash_table *ht, char *word);
int lookup_find(hash_table *ht, char *word);

char *find_entry(database *db, char *key);

/* -------------------- String Handling Helper Functions -------------------- */

int port_number_to_str(int port, char *buff);
void request_line_to_key(char *request_line);
int entry_to_str(char *entry_offset, char *buffer, int len);
int value_array_to_str(value_array *va, char *buffer, int len);

/* ----------------- Value Array Handling Helper Functions ------------------ */

value_array *get_intersection(value_array *va_1, value_array *va_2);
value_array *get_value_array(char *entry_offset);
value_array *create_value_array(char *entry_str);

/* --------------------- Miscellanious Helper Functions --------------------- */

char *get_next_key_offset(char *entry_offset);

int find_node(char *key, int total_nodes);

size_t round_up(size_t n, size_t mult);

/* Custom functions */
int is_found(char* key, char* result);
char* generate_not_found(char* key);
char* generate_two_not_found(char* key1, char* key2);