## Cross-Server Network Testing

The default `go test` command runs local integration tests (as defined in `fastrpc_test.go`). A separate, network-aware test suite is available in `fastrpc_network_test.go` to test `FastRpc` in a true two-server environment.

These tests are skipped unless a `FASTRPC_MASTER_ADDR` environment variable is set.

### Prerequisites

1. **Go:** Both servers must have the Go toolchain installed (`go version 1.21+`).
2. **Git:** Both servers must have `git` installed.
3. **Two Servers:**

   * **Server B (Master):** The server that will run the `FastRpc` master.
   * **Server A (Client):** The server that will run the `go test` client.
4. **SSH Access:** You need SSH access to both servers, and **Server A must have SSH key access to Server B** if they are in different subnets and you wish to use SSH tunneling.

### Setup Instructions

#### On Server B (The Master Server)

1. Log in to Server B via SSH:

   ```sh
   ssh user@server_b_ip
   ```

2. Clone the `FastRpc` repository:

   ```sh
   git clone https://github.com/AmanTrance/FastRpc.git
   cd FastRpc
   ```

3. Build and run the dedicated test server:

   ```sh
   go build -o test_server ./examples/test_server/main.go

   ./test_server
   ```

   (You may want to use a tool like `screen` or `nohup` to keep the server running in the background.)

#### On Server A (The Client / Test-Runner)

1. Log in to Server A via SSH in a **separate terminal**:

   ```sh
   ssh user@server_a_ip
   ```

2. Clone the `FastRpc` repository:

   ```sh
   git clone https://github.com/AmanTrance/FastRpc.git
   cd FastRpc
   ```

3. Install Go test dependencies:

   ```sh
   go get github.com/stretchr/testify/assert
   ```

### Running the Network Tests

How you run the test depends on your network setup.

#### Scenario 1: Servers in the Same Subnet (No Firewall)

This is the simplest case. Server A can directly reach Server B.

On **Server A**, run the following commands:

```sh
export FASTRPC_MASTER_ADDR="<server_b_ip>:10000"

go test -v -run=TestNetwork
```

The tests will connect to `server_b_ip:10000` and run the full suite.

#### Scenario 2: Servers in Different Subnets (Firewalled)

This is the most common and robust setup. Server A *cannot* directly reach `server_b_ip:10000`. We will use **SSH Local Port Forwarding** (our "wormhole") to securely bypass the firewall.

On **Server A**, run these commands:

1. **Build the SSH Tunnel:**
   This command tells SSH to "listen on my `localhost:10000`, and forward any traffic through the tunnel to `localhost:10000` on the *other side* (Server B)."

   ```sh
   ssh -L 10000:localhost:10000 user@server_b_ip -N -f
   ```

   *(Note: This requires Server A's SSH public key to be in Server B's `authorized_keys` file, which you have already set up!)*

2. **Run the Tests:**
   Now, we tell the test runner to connect to our *local* end of the tunnel.

   ```sh
   export FASTRPC_MASTER_ADDR="127.0.0.1:10000"

   go test -v -run=TestNetwork
   ```

The tests on Server A will connect to `127.0.0.1:10000`, SSH will magically forward that traffic to Server B, and you will get a full, secure, end-to-end network test.



