#include "csapp/csapp.h"
#include "utils.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

// You may assume that all requests sent to a node are less than this length
#define REQUESTLINELEN 128
#define HOSTNAME "localhost"

// Cache related constants
#define MAX_OBJECT_SIZE 512 // object here refers to the posting list or result list being cached
#define MAX_CACHE_SIZE MAX_OBJECT_SIZE*128

/* This struct contains all information needed for each node */
typedef struct node_info {
  int node_id;     // node number
  int port_number; // port number
  int listen_fd;   // file descriptor of socket the node is using
} node_info;

/* Variables that all nodes will share */

// Port number of the parent process. After each node has been spawned, it 
// attempts to connect to this port to send the parent the request for it's own 
// section of the database.
int PARENT_PORT = 0;

// Number of nodes that were created. Must be between 1 and 8 (inclusive).
int TOTAL_NODES = 0;

// A dynamically allocated array of TOTAL_NODES node_info structs.
// The parent process creates this and populates it's values so when it creates
// the nodes, they each know what port number the others are using.
node_info *NODES = NULL;

/* ------------  Variables specific to each child process / node ------------ */

// After forking each node (one child process) changes the value of this variable
// to their own node id.
// Note that node ids start at 0. If you're just implementing the single node 
// server this will be set to 0.
int NODE_ID = -1;

// Each node will fill this struct in with it's own portion of the database.
database partition = {NULL, 0, NULL};

/** @brief Called by a child process (node) when it wants to request its partition
 *         of the database from the parent process. This will be called ONCE by 
 *         each node in the "digest" phase.
 *  
 *  @todo  Implement this function. This function will need to:
 *         - Connect to the parent process. HINT: the port number to use is 
 *           stored as an int in PARENT_PORT.
 *         - Send a request line to the parent. The request needs to be a string
 *           of the form "<nodeid>\n" (the ID of the node followed by a newline) 
 *         - Read the response of the parent process. The response will start 
 *           with the size of the partition followed by a newline. After the 
 *           newline character, the next size bytes of the response will be this
 *           node's partition of the database.
 *         - Set the global partition variable. 
 */
void request_partition(void) {
  // TODO: implement this function. 
  int child_fd;
  char request[REQUESTLINELEN];
  char port_name[REQUESTLINELEN];
  char size[REQUESTLINELEN];
  // create request string <nodeid>\n
  sprintf(request, "%d\n", NODE_ID);

  // convert port number to string
  if (port_number_to_str(PARENT_PORT, port_name) == -1) {
    fprintf(stderr, "Wrong port number.\n");
    exit(1);
  }

  // connect with parent process to get partition
  child_fd = Open_clientfd(HOSTNAME, port_name);
  if (child_fd != -1) {
    write(child_fd, request, strlen(request));
    // read in size of the db

    int i = 0;
    char ch;
    while (i < REQUESTLINELEN - 1) {
      ssize_t bytes_read = read(child_fd, &ch, 1);

      if(bytes_read == -1) {
        perror("Read error");
        break;
      } else if(bytes_read == 0) {
        // end of file
        break;
      }

      size[i++] = ch;
      // end at \n
      if(ch == '\n') {
        break;
      }

    }

    size[i] = '\0';


    //if((read(child_fd, size, REQUESTLINELEN)) == 0) {
    //  exit(1);
    //}
    sscanf(size, "%lu", &(partition.db_size));
    ssize_t b_read;

    // read in database
    partition.m_ptr = (char*) malloc(partition.db_size);
    char buffer[partition.db_size];
    // while ((b_read = read(child_fd, buffer, partition.db_size)) > 0) {
    //   strcat(partition.m_ptr, buffer);
    // }
    size_t total_bytes_read = 0;

    while (total_bytes_read < partition.db_size) {
        ssize_t bytes_read = read(child_fd, buffer, partition.db_size - total_bytes_read);

        if (bytes_read <= 0) {
            break;
        }

        memcpy(partition.m_ptr + total_bytes_read, buffer, bytes_read);

        total_bytes_read += bytes_read;
    }
    build_hash_table(&partition);
    Close(child_fd);
  }
}

/** 
 * This function is to search whether the key is in the whole database (including
 * other nodes). It will connect to other nodes if necessary.
 * This works only for one node.
 * @return formatted string if found; NULL if not found
*/
char* get_one_result_string(char* key) {
  char* result_offset;
  char* result = (char*) malloc(512);
  int size;
  value_array* array;

  // find inside this node
  result_offset = find_entry(&partition, key);
  // if found inside this node
  if (result_offset) {
    entry_to_str(result_offset, result, 512);
    return result;
  }

  // if not found inside this node, connect with other node
  int id = find_node(key, TOTAL_NODES);
  if (NODE_ID != id) {
    char port_name[128];
    port_number_to_str(NODES[id].port_number, port_name);
    int fd = Open_clientfd(HOSTNAME, port_name);

    strcat(key, "\n");
    write(fd, key, strlen(key));
    read(fd, result, 512);
    Close(fd);
    if (is_found(key, result)) {
      return result;
    }
    
  }
  free(result);
  return NULL;
}

char* get_two_result(char* key1, char* key2) {
  char* result1 = get_one_result_string(key1);
  char* result2 = get_one_result_string(key2);
  char* final_result;

  if(!result1 && !result2) {
    final_result = generate_two_not_found(key1, key2);
    return final_result;
  }

  if(!result1) {
    final_result = generate_not_found(key1);
    return final_result;
  }

  if(!result2) {
    final_result = generate_not_found(key2);
    return final_result;
  }

  value_array* va1 = create_value_array(result1);
  value_array* va2 = create_value_array(result2);
  value_array* intersection = get_intersection(va1, va2);
  free(va1);
  free(va2);
  
  final_result = (char*) malloc(512);
  sprintf(final_result, "%s,%s", key1, key2);
  char str[512];
  value_array_to_str(intersection, str, 512);
  strcat(final_result, str);

  free(intersection);
  return final_result;

}

/** @brief The main server loop for a node. This will be called by a node after
 *         it has finished the digest phase. The server will run indefinitely,
 *         responding to requests. Each request is a single line. 
 *
 *  @note  The parent process creates the listening socket that the node should
 *         use to accept incoming connections. This file descriptor is stored in
 *         NODES[NODE_ID].listen_fd. 
*/
void node_serve(void) {
  // TODO: implement this function. 
  int connfd, size;
  socklen_t clientlen;
  struct sockaddr_storage clientaddr;
  char buf[REQUESTLINELEN];

  // start process loop
  while (1) {
    // accept with client
    clientlen = sizeof(struct sockaddr_storage);
    connfd = Accept(NODES[NODE_ID].listen_fd, (SA *) &clientaddr, &clientlen);
    char key[REQUESTLINELEN];
    read(connfd, key, REQUESTLINELEN);
    request_line_to_key(key);

    char space = ' ';
    if (!(strchr(key, space))){
      char *result = get_one_result_string(key);
      if (result) {
        write(connfd, result, strlen(result));
        free(result);
      } else {
        result = generate_not_found(key);
        write(connfd, result, strlen(result));
        free(result);
      }
    } else {
      char* key1 = strtok(key, " ");
      char* key2 = strtok(NULL, " ");
      char* result = get_two_result(key1, key2);

      write(connfd, result, strlen(result));
      free(result);
    }
  
    
    Close(connfd);
  }
  
}



/** @brief Called after a child process is forked. Initialises all information
 *         needed by an individual node. It then calls request_partition to get
 *         the database partition that belongs to this node (the digest phase). 
 *         It then calls node_serve to begin responding to requests (the serve
 *         phase). Since the server is designed to run forever (unless forcibly 
 *         terminated) this function should not return.
 * 
 *  @param node_id Value between 0 and TOTAL_NODES-1 that represents which node
 *         number this is. The global NODE_ID variable will be set to this value
 */
void start_node(int node_id) {
  NODE_ID = node_id;

  // close all listen_fds except the one that this node should use.
  for (int n = 0; n < TOTAL_NODES; n++) {
    if (n != NODE_ID)
      Close(NODES[n].listen_fd);
  }

  request_partition();
  node_serve();

  //free(partition.m_ptr);
}



/** ----------------------- PARENT PROCESS FUNCTIONS ----------------------- **/

/* The functions below here are for the initial parent process to use (spawning
 * child processes, partitioning the database, etc). 
 * You do not need to modify this code.
*/


/** @brief  Tries to create a listening socket on the port that start_port 
 *          points to. If it cannot use that port, it will subsequently try
 *          increasing port numbers until it successfully creates a listening 
 *          socket, or it has run out of valid ports. The value at start_port is
 *          set to the port_number the listening socket was opened on. The file
 *          descriptor of the listening socket is returned.
 * 
 *  @param  start_port The value that start_port points to is used as the first 
 *          port to try. When the function returns, the value is updated to the
 *          port number that the listening socket can use. 
 *  @return The file descriptor of the listening socket that was created, or -1
 *          if no listening socket has been created.
*/
int get_listenfd(int *start_port) {
  char portstr[PORT_STRLEN]; 
  int port, connfd;
  for (port = *start_port; port < MAX_PORTNUM; port++) {
    port_number_to_str(port, portstr);
    connfd = open_listenfd(portstr);
    if (connfd != -1) { // found a port to use
      *start_port = port;
      return connfd;
    }
  }
  return -1;
}

/** @brief  Called by the parent to handle a single request from a node for its
 *          partition of the database. 
 *
 *  @param  db The database that will be partitioned. 
 *  @param  connfd The connected file descriptor to read the request (a node id) 
 *          from. The partition of the database is written back in response.
 *  @return If there is an error in the request returns -1. Otherwise returns 0.
*/
int parent_handle_request(database *db, int connfd) {
  char request[REQUESTLINELEN];
  char responseline[REQUESTLINELEN];
  char *response;
  int node_id;
  ssize_t rl;
  size_t partition_size = 0;
  if ((rl = read(connfd, request, REQUESTLINELEN)) < 0) {
    fprintf(stderr, "parent_handle_request read error: %s\n", strerror(errno));
    return -1;
  }
  sscanf(request, "%d", &node_id);
  if ((node_id < 0) || (node_id >= TOTAL_NODES)) {
    response = "Invalid Request.\n";
    partition_size = strlen(response);
  } else {
    response = get_partition(db, TOTAL_NODES, node_id, &partition_size);
  }
  snprintf(responseline, REQUESTLINELEN, "%lu\n", partition_size);
  rl = write(connfd, responseline, strlen(responseline));
  rl = write(connfd, response, partition_size);
  return 0;
}

/** Called by the parent process to load in the database, and wait for the child
 *  nodes it created to send a message requesting their portion of the database.
 *  After it has received the same number of requests as nodes, it unmaps the 
 *  database. 
 *
 *  @param db_path path to the database file being loaded in. It is assumed that
 *         the entries contained in this file are already sorted in alphabetical
 *         order.
 */
void parent_serve(char *db_path, int parent_connfd) {
  // The parent doesn't need to create/populate the hash table.
  database *db = load_database(db_path);
  struct sockaddr_storage clientaddr;
  socklen_t clientlen = sizeof(clientaddr);
  int connfd = 0;
  int requests = 0;

  while (requests < TOTAL_NODES) {
    connfd = accept(parent_connfd, (SA *)&clientaddr, &clientlen);
    parent_handle_request(db, connfd);
    Close(connfd);
    requests++;
  }
  // Parent has now finished it's job.
  Munmap(db->m_ptr, db->db_size);
}

/** @brief Called after the parent has finished sending each node its partition 
 *         of the database. The parent waits in a loop for any child processes 
 *         (nodes) to terminate, and prints to stderr information about why the 
 *         child process terminated. 
*/
void parent_end() {
  int stat_loc;
  pid_t pid;
  while (1) {
    pid = wait(&stat_loc);
    if (pid < 0 && (errno == ECHILD))
      break;
    else {
      if (WIFEXITED(stat_loc))
        fprintf(stderr, "Process %d terminated with exit status %d\n", pid, WEXITSTATUS(stat_loc));
      else if (WIFSIGNALED(stat_loc))
        fprintf(stderr, "Process %d terminated by signal %d\n", pid, WTERMSIG(stat_loc));
    }
  }
}

int main(int argc, char const *argv[]) {
  int start_port;    // port to begin search
  int parent_connfd; // parent listens here to handle distributing database 
  int n_connfd;      
  pid_t pid;
  
  if (argc != 4) {
    fprintf(stderr, "usage: %s [num_nodes] [starting_port] [name_of_file]\n", argv[0]);
    exit(1);
  }
  
  sscanf(argv[1], "%d", &TOTAL_NODES);
  sscanf(argv[2], "%d", &start_port);

  if (TOTAL_NODES < 1 || (TOTAL_NODES > 8)) {
    fprintf(stderr, "Invalid node number given.\n");
    exit(1);
  } else if ((start_port < 1024) || start_port >= (MAX_PORTNUM - TOTAL_NODES)) {
    fprintf(stderr, "Invalid starting port given.\n");
    exit(1);
  }

  NODES = calloc(TOTAL_NODES, sizeof(node_info));
  parent_connfd = get_listenfd(&start_port);
  PARENT_PORT = start_port;

  for (int n = 0; n < TOTAL_NODES; n++) {
    start_port++; // start search at previously assigned port + 1
    n_connfd = get_listenfd(&start_port);
    if (n_connfd < 0) {
      fprintf(stderr, "get_listenfd error\n");
      exit(1);
    }
    NODES[n].listen_fd = n_connfd;
    NODES[n].node_id = n;
    NODES[n].port_number = start_port;
  }

  // Begin forking all child processes.
  for (int n = 0; n < TOTAL_NODES; n++) {
    if ((pid = Fork()) == 0) { // child process
      Close(parent_connfd);
      start_node(n);
      exit(1);
    } else {
      node_info node = NODES[n];
      fprintf(stderr, "NODE %d [PID: %d] listening on port %d\n", n, pid, node.port_number);
    }
  }

  // Parent closes all fd's that belong to it's children
  for (int n = 0; n < TOTAL_NODES; n++)
    Close(NODES[n].listen_fd);

  // Parent can now begin waiting for children to send messages to contact.
  parent_serve((char *) argv[3], parent_connfd);
  Close(parent_connfd);

  parent_end();

  return 0;
}
