# Distributed Album System

A distributed system for managing and sharing image albums, implementing
different concurrency models and approaches to Conflict-free Replicated Data
Types (CRDTs) for data consistency across nodes.

## Project Overview

This project demonstrates a distributed album management system with the
following components:

- **Central Server**: Written in Erlang, coordinates initial connections and
  metadata distribution
- **Client Applications**: Python clients that connect to the central server and
  each other using TCP and ZeroMQ
- **Data Synchronization**: Uses CRDTs (Conflict-free Replicated Data Types) to
  ensure data consistency across distributed nodes

## Architecture

The system follows a hybrid architecture:

1. Initial connection and authentication via central Erlang server
2. Direct peer-to-peer communication between clients using ZeroMQ for metadata
   and album updates
3. Vector clocks for causal consistency in message delivery
4. CRDT implementations for conflict resolution

## Branches and Implementation Approaches

This repository contains three main branches, each implementing a different
approach to solving distributed state management:

### Main Branch

The main branch implements an operation-based approach using a custom ORSet
(Observed-Remove Set) CRDT with:

- Thread-based concurrency using Python's threading library
- Queue-based communication between threads
- Version tracking with (replica_id, counter) tuples
- Direct mutation for add/remove operations
- Effect-based operation processing

### ReactiveX Branch

The ReactiveX branch reimplements the client using Reactive Extensions (Rx)
patterns:

- Reactive streams for inter-thread communication using RxPy
- Subject-based event propagation
- More structured CRDT implementation with explicit Dot objects for versioning
- Separate add_set and remove_set for tracking element status
- State-based merging for synchronization
- ThreadPoolScheduler for concurrent processing

### State-Based Branch

The state-based branch implements a state-based CRDT approach:

- Full state synchronization rather than operation-based updates
- Simpler merge semantics at the cost of larger message sizes
- Different conflict resolution strategy

## Getting Started

### Prerequisites

- Erlang/OTP (for the central server)
- Python 3.7+ (for clients)
- Required Python packages:
  - pyzmq==26.0.3
  - Rx==3.2.0 (only for ReactiveX branch)

### Installation

1. Clone the repository:

   ```
   git clone [repository URL]
   ```

2. Start the Erlang central server:

   ```
   cd central-server
   rebar3 compile
   rebar3 shell
   ```

   inside rebar3 shell:
   ```
   server_app:start(PORT)
   ```

3. Start a Python client (choosing your preferred implementation):

   ```
   # Main branch
   cd client-python
   python main.py [port]

   # ReactiveX branch
   git checkout ReactiveX
   cd client-python
   python main.py [port]

   # State-based branch
   git checkout state_based
   cd client-python
   python main.py [port]
   ```

## Client Usage

Once connected, clients can interact with the album system using the following
commands:

- `/login [username]` - Login with a username
- `/enterAlbum [album_name]` - Enter a specific album
- `/getImages` - List all images in the current album with their ratings
- `/getMetadata` - Get full metadata of the album
- `/getOrSet` - Display the internal state of the ORSet CRDT
- `/addImage [image_name] [hash] [size]` - Add a new image to the album
- `/removeImage [image_name]` - Remove an image from the album
- `/addUser [user_name]` - Add a user to the album
- `/removeUser [user_name]` - Remove a user from the album
- `/rateImage [image_name] [rating]` - Rate an image (0-5)
- `/quit` - Disconnect from the album and server

## CRDT Implementation Details

### ORSet (Observed-Remove Set)

The project implements different variants of an ORSet CRDT across branches:

#### Main Branch Implementation

- Elements tracked with version vectors `(replica_id, counter)`
- Each element has associated versions and a value
- Add operation creates a new version and adds it to the element's version set
- Remove operation preserves the versions being removed for conflict resolution
- Effect function applies remote operations by manipulating the version sets

#### ReactiveX Branch Implementation

- Uses explicit `Dot` objects representing unique identifiers for operations
- Maintains separate `add_set` and `remove_set` for each element
- An element exists if it has versions in add_set that aren't in remove_set
- Cleaner merge operation that combines add_sets and remove_sets
- JSON serialization of CRDT state for network transmission

#### State-Based Branch Implementation

- Full state synchronization approach
- State-based CRDTs that focus on mergeable data structures
- Simpler conceptual model but potentially higher bandwidth usage
- Eliminates the need for causal delivery of operations

## Technical Details

### Concurrency Models

#### Main Branch

- Uses Python's threading and queue modules
- Thread communication via shared queues:
  - `message_queue`: For output messages
  - `tcp_send_queue`: For TCP socket communication
  - `zmq_send_queue`: For ZeroMQ message distribution
  - `zmq_connect_queue`: For ZeroMQ connection management
- Shared flags for coordination via threading.Event

#### ReactiveX Branch

- Uses ReactiveX (Rx) for event-driven programming
- Thread communication via Rx Subjects:
  - `message_subject`: For output messages
  - `tcp_send_subject`: For TCP socket communication
  - `zmq_send_subject`: For ZeroMQ message distribution
  - `zmq_connect_subject`: For ZeroMQ connection management
  - `terminate_subject` and `use_zmq_for_sending_subject`: As reactive flags
- ThreadPoolScheduler for background processing
- Declarative pipelines with operators like `take_until`, `filter`, and
  `observe_on`

### Network Communication

- Initial connection to Erlang server via TCP
- Peer discovery through the central server
- Direct P2P communication via ZeroMQ PUB/SUB sockets
- Message format is JSON with vector clocks for causal ordering

## Development Notes

- The central server handles initial connections and user authentication
- Clients communicate directly with each other using ZeroMQ after initial setup
- Each client maintains its own replica of the data and synchronizes using CRDTs
- Vector clocks ensure causal consistency in message delivery

## Project Structure

```
.
├── client-python/        # Python client implementation
│   ├── main.py           # Main client code (different in each branch)
│   └── requirements.txt  # Python dependencies
├── server-erlang/        # Erlang central server
│   ├── src/              # Server source code
│   └── start_server.sh   # Script to start the Erlang server
└── README.md             # This file
```

