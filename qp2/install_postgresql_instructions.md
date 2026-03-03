# PostgreSQL Installation and Configuration Instructions

This document provides steps to install PostgreSQL on Ubuntu, create a user named `dhs` with no password, and set up a database named `user_data` with `dhs` as the owner.

## 1. Install PostgreSQL

Update your package list and install the PostgreSQL server and its contrib modules:

```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

Start the PostgreSQL service and enable it to run automatically on system boot:

```bash
sudo systemctl enable --now postgresql
```

## 2. Create the 'dhs' User and 'user_data' Database

Switch to the `postgres` system user, which is the administrative user for PostgreSQL, to create the new user and database.

1.  Open the PostgreSQL interactive shell (`psql`):

    ```bash
    sudo -u postgres psql
    ```

2.  Execute the following SQL commands within the `psql` shell:

    ```sql
    -- Create user (role) named 'dhs' with no password initially
    CREATE USER dhs;

    -- Create the database named 'user_data'
    CREATE DATABASE user_data;

    -- Grant ownership of the 'user_data' database to the 'dhs' user
    ALTER DATABASE user_data OWNER TO dhs;

    -- Grant all privileges on the 'user_data' database to 'dhs' to ensure full access
    GRANT ALL PRIVILEGES ON DATABASE user_data TO dhs;

    -- Exit the psql interactive shell
    \q
    ```

## 3. Configure "No Password" Access (Trust Authentication)

To allow the `dhs` user to connect to the `user_data` database without a password, you need to configure PostgreSQL's host-based authentication file (`pg_hba.conf`). This uses `trust` authentication for local connections.

1.  Identify your PostgreSQL version. Common versions are `12`, `14`, or `16`. You can find it by checking the directory names in `/etc/postgresql/`.

2.  Edit the `pg_hba.conf` file. Replace `14` in the path below with your actual PostgreSQL version number if it's different:

    ```bash
    sudo nano /etc/postgresql/14/main/pg_hba.conf
    ```

3.  Locate the section for "local" connections (Unix domain socket connections). You will typically find a line like:

    ```text
    # "local" is for Unix domain socket connections only
    local   all             all                                     peer
    ```

4.  **Add a new line** to explicitly enable `trust` authentication for the `dhs` user connecting to the `user_data` database via a local Unix socket:

    ```text
    # TYPE  DATABASE        USER            ADDRESS                 METHOD
    local   user_data       dhs                                     trust
    ```

    You can place this line above or below the existing `local   all   all   peer` line. The order matters; PostgreSQL processes rules from top to bottom. More specific rules should generally come before more general ones.

    *Self-correction: While changing `peer` to `trust` for `local all all` works, it makes *all* local users able to connect to *all* databases without a password, which is overly broad. The specific entry for `user_data` and `dhs` is more secure.*

5.  Save the changes to the file (press `Ctrl+O`, then `Enter`, then `Ctrl+X` to exit `nano`).

## 4. Allow Subnet Connections (Optional)

To allow the `dhs` user to connect to the `user_data` database from other machines within the same local network (subnet) without a password, you need to configure both `pg_hba.conf` and `postgresql.conf`.

### 4.1. Configure `pg_hba.conf` for Subnet Access

1.  Using the same file you edited above (`/etc/postgresql/14/main/pg_hba.conf`), add the following line.
    **Remember to replace `192.168.1.0/24` with your actual network subnet in CIDR notation.**

    ```text
    # TYPE  DATABASE        USER            ADDRESS                 METHOD
    host    user_data       dhs             192.168.1.0/24          trust
    ```
    *   `host`: Specifies that this rule applies to TCP/IP connections (from network hosts).
    *   `user_data`: The specific database that `dhs` can access.
    *   `dhs`: The user role allowed to connect.
    *   `192.168.1.0/24`: The network address range allowed to connect.
    *   `trust`: Allows connections without a password. (For production, consider `md5` or `scram-sha-256` and set a password for `dhs`.)

2.  Save and close `pg_hba.conf`.

### 4.2. Configure `postgresql.conf` to Listen on Network Interfaces

By default, PostgreSQL often only listens for connections from `localhost`. To accept connections from other machines, you need to configure `listen_addresses`.

1.  Edit the main PostgreSQL configuration file:

    ```bash
    sudo nano /etc/postgresql/14/main/postgresql.conf
    ```

2.  Find the `listen_addresses` parameter (you might need to uncomment it) and change it to `'*'` to listen on all available network interfaces:

    ```ini
    listen_addresses = '*'          # what IP address(es) to listen on;
                                    # comma-separated list of addresses;
                                    # defaults to 'localhost'; use '*' for all
    ```
    *This setting is ideal for servers with dynamic IPs (DHCP) as it automatically binds to whatever IP the server currently has.*

3.  Save and close `postgresql.conf`.

## 5. Restart PostgreSQL

For all configuration changes (`pg_hba.conf` and `postgresql.conf`) to take effect, you must restart the PostgreSQL service:

```bash
sudo systemctl restart postgresql
```

## 6. Verify Connection (from a remote machine)

From a machine *within the specified subnet*, you should now be able to connect:

```bash
psql -h <PostgreSQL_Server_IP> -U dhs -d user_data
```

## 7. Connecting with Dynamic IPs (DHCP)

If your PostgreSQL server uses DHCP and its IP address changes frequently, using a hardcoded IP address in your application will break the connection.

**Solution: Use the Hostname**

Linux and modern networks typically support resolving local hostnames (often via mDNS/Bonjour, ending in `.local`).

1.  **Find the Server's Hostname:**
    On the PostgreSQL server, run:
    ```bash
    hostname
    ```
    (Let's assume the output is `myserver`)

2.  **Update Application Configuration:**
    Use the hostname instead of the IP address in your connection string.

    `postgresql://dhs@myserver.local/user_data`
    
    *Note: If `.local` doesn't work on your network, try just the hostname `myserver` or configure a Static DHCP Lease (Reservation) in your network router settings to ensure the server always receives the same IP.*

## 8. Basic Database Operations

Once connected, you can use the following commands within the `psql` shell to inspect your database.

1.  **Connect to the Database:**
    ```bash
    psql -U dhs -d user_data -h localhost
    ```

2.  **Common Commands:**

    *   `\l` : List all databases.
    *   `\dt` : List all tables in the current database (and schema).
    *   `\d` : List all relations (tables, sequences, views).
    *   `\d <table_name>` : Describe the structure (columns, types) of a specific table.
        *   Example: `\d spreadsheet`
    *   `SELECT * FROM <table_name>;` : Query data from a table.
        *   Example: `SELECT * FROM spreadsheet;`
    *   `\q` : Quit the `psql` shell.
