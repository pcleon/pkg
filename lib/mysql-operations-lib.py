#!/usr/bin/env python3
"""
MySQL Operations Library

A comprehensive library for MySQL database operations using pymysql.
Includes functions for connection management, query execution, replication setup,
and common database administration tasks.
"""

import pymysql
import pymysql.cursors
import time
import logging
from typing import List, Dict, Any, Tuple, Optional, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mysql_ops")

class MySQLClient:
    """MySQL operations client using pymysql"""
    
    def __init__(self, host: str, port: int = 3306, user: str = 'root', 
                 password: str = '', charset: str = 'utf8mb4', 
                 connect_timeout: int = 10, cursorclass=None):
        """
        Initialize MySQL client connection parameters
        
        Args:
            host: MySQL server hostname or IP
            port: MySQL server port
            user: MySQL username
            password: MySQL password
            charset: Character set for connection
            connect_timeout: Connection timeout in seconds
            cursorclass: Custom cursor class (default: DictCursor)
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.charset = charset
        self.connect_timeout = connect_timeout
        self.cursorclass = cursorclass or pymysql.cursors.DictCursor
        self.conn = None
        
    def connect(self, database: str = None) -> bool:
        """
        Connect to MySQL server
        
        Args:
            database: Optional database name to connect to
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=database,
                charset=self.charset,
                connect_timeout=self.connect_timeout,
                cursorclass=self.cursorclass
            )
            logger.info(f"Connected to MySQL server at {self.host}:{self.port}")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to connect to MySQL server: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close MySQL connection if open"""
        if self.conn and self.conn.open:
            self.conn.close()
            logger.info(f"Disconnected from MySQL server at {self.host}:{self.port}")
    
    def reconnect(self, database: str = None) -> bool:
        """
        Reconnect to MySQL server
        
        Args:
            database: Optional database name to connect to
            
        Returns:
            bool: True if reconnection successful, False otherwise
        """
        self.disconnect()
        return self.connect(database)
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            List of dictionaries containing query results
        """
        if not self.conn or not self.conn.open:
            logger.warning("Not connected to MySQL server. Attempting to reconnect...")
            if not self.connect():
                logger.error("Failed to reconnect to MySQL server")
                return []
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
                logger.debug(f"Query executed successfully: {query}")
                return result
        except pymysql.Error as e:
            logger.error(f"Error executing query: {e}")
            return []
    
    def execute_write(self, query: str, params: tuple = None) -> int:
        """
        Execute a write query (INSERT, UPDATE, DELETE) and return affected row count
        
        Args:
            query: SQL query to execute
            params: Parameters for the query
            
        Returns:
            int: Number of affected rows
        """
        if not self.conn or not self.conn.open:
            logger.warning("Not connected to MySQL server. Attempting to reconnect...")
            if not self.connect():
                logger.error("Failed to reconnect to MySQL server")
                return 0
        
        try:
            with self.conn.cursor() as cursor:
                affected_rows = cursor.execute(query, params)
                self.conn.commit()
                logger.debug(f"Write query executed successfully: {query}")
                return affected_rows
        except pymysql.Error as e:
            logger.error(f"Error executing write query: {e}")
            self.conn.rollback()
            return 0
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """
        Execute a batch write query and return affected row count
        
        Args:
            query: SQL query to execute
            params_list: List of parameter tuples for the query
            
        Returns:
            int: Number of affected rows
        """
        if not self.conn or not self.conn.open:
            logger.warning("Not connected to MySQL server. Attempting to reconnect...")
            if not self.connect():
                logger.error("Failed to reconnect to MySQL server")
                return 0
        
        try:
            with self.conn.cursor() as cursor:
                affected_rows = cursor.executemany(query, params_list)
                self.conn.commit()
                logger.debug(f"Batch query executed successfully: {query}")
                return affected_rows
        except pymysql.Error as e:
            logger.error(f"Error executing batch query: {e}")
            self.conn.rollback()
            return 0
    
    def get_version(self) -> str:
        """
        Get MySQL server version
        
        Returns:
            str: MySQL server version
        """
        result = self.execute_query("SELECT VERSION() as version")
        if result:
            return result[0]['version']
        return "Unknown"
    
    def get_databases(self) -> List[str]:
        """
        Get list of databases
        
        Returns:
            List[str]: List of database names
        """
        result = self.execute_query("SHOW DATABASES")
        return [row['Database'] for row in result]
    
    def get_tables(self, database: str) -> List[str]:
        """
        Get list of tables in a database
        
        Args:
            database: Database name
            
        Returns:
            List[str]: List of table names
        """
        result = self.execute_query(f"SHOW TABLES FROM `{database}`")
        key = f"Tables_in_{database}"
        return [row[key] for row in result]
    
    def get_table_structure(self, database: str, table: str) -> List[Dict[str, Any]]:
        """
        Get table structure
        
        Args:
            database: Database name
            table: Table name
            
        Returns:
            List[Dict]: Table structure information
        """
        return self.execute_query(f"DESCRIBE `{database}`.`{table}`")
    
    def get_indexes(self, database: str, table: str) -> List[Dict[str, Any]]:
        """
        Get table indexes
        
        Args:
            database: Database name
            table: Table name
            
        Returns:
            List[Dict]: Table indexes information
        """
        return self.execute_query(f"SHOW INDEX FROM `{database}`.`{table}`")
    
    def get_process_list(self) -> List[Dict[str, Any]]:
        """
        Get list of running processes
        
        Returns:
            List[Dict]: Process information
        """
        return self.execute_query("SHOW FULL PROCESSLIST")
    
    def kill_process(self, process_id: int) -> bool:
        """
        Kill a specific process
        
        Args:
            process_id: Process ID to kill
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write(f"KILL {process_id}")
            logger.info(f"Process {process_id} killed successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to kill process {process_id}: {e}")
            return False
    
    def get_variables(self, pattern: str = '%') -> Dict[str, Any]:
        """
        Get MySQL server variables
        
        Args:
            pattern: Optional pattern to filter variables
            
        Returns:
            Dict: Dictionary of variable names and values
        """
        result = self.execute_query(f"SHOW GLOBAL VARIABLES LIKE '{pattern}'")
        return {row['Variable_name']: row['Value'] for row in result}
    
    def get_status(self, pattern: str = '%') -> Dict[str, Any]:
        """
        Get MySQL server status
        
        Args:
            pattern: Optional pattern to filter status variables
            
        Returns:
            Dict: Dictionary of status names and values
        """
        result = self.execute_query(f"SHOW GLOBAL STATUS LIKE '{pattern}'")
        return {row['Variable_name']: row['Value'] for row in result}
    
    def create_database(self, database: str, if_not_exists: bool = True) -> bool:
        """
        Create a new database
        
        Args:
            database: Database name
            if_not_exists: Add IF NOT EXISTS clause
            
        Returns:
            bool: True if successful, False otherwise
        """
        clause = "IF NOT EXISTS" if if_not_exists else ""
        try:
            self.execute_write(f"CREATE DATABASE {clause} `{database}`")
            logger.info(f"Database '{database}' created successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to create database '{database}': {e}")
            return False
    
    def drop_database(self, database: str, if_exists: bool = True) -> bool:
        """
        Drop a database
        
        Args:
            database: Database name
            if_exists: Add IF EXISTS clause
            
        Returns:
            bool: True if successful, False otherwise
        """
        clause = "IF EXISTS" if if_exists else ""
        try:
            self.execute_write(f"DROP DATABASE {clause} `{database}`")
            logger.info(f"Database '{database}' dropped successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to drop database '{database}': {e}")
            return False
    
    def backup_database(self, database: str, output_file: str) -> bool:
        """
        Backup a database using mysqldump (requires system access)
        
        Args:
            database: Database name
            output_file: Output file path
            
        Returns:
            bool: True if successful, False otherwise
        """
        import subprocess
        
        cmd = [
            "mysqldump",
            f"--host={self.host}",
            f"--port={self.port}",
            f"--user={self.user}",
            f"--password={self.password}",
            "--single-transaction",
            "--routines",
            "--triggers",
            "--events",
            database
        ]
        
        try:
            with open(output_file, 'w') as f:
                subprocess.run(cmd, stdout=f, check=True)
            logger.info(f"Database '{database}' backed up to '{output_file}' successfully")
            return True
        except subprocess.SubprocessError as e:
            logger.error(f"Failed to backup database '{database}': {e}")
            return False
    
    def restore_database(self, database: str, input_file: str) -> bool:
        """
        Restore a database from a mysqldump file (requires system access)
        
        Args:
            database: Database name
            input_file: Input file path
            
        Returns:
            bool: True if successful, False otherwise
        """
        import subprocess
        
        cmd = [
            "mysql",
            f"--host={self.host}",
            f"--port={self.port}",
            f"--user={self.user}",
            f"--password={self.password}",
            database
        ]
        
        try:
            with open(input_file, 'r') as f:
                subprocess.run(cmd, stdin=f, check=True)
            logger.info(f"Database '{database}' restored from '{input_file}' successfully")
            return True
        except subprocess.SubprocessError as e:
            logger.error(f"Failed to restore database '{database}': {e}")
            return False
    
    # Replication Management
    
    def get_replication_status(self) -> Dict[str, Any]:
        """
        Get replication status
        
        Returns:
            Dict: Replication status information
        """
        result = self.execute_query("SHOW SLAVE STATUS")
        return result[0] if result else {}
    
    def setup_replication(self, master_host: str, master_port: int, 
                          master_user: str, master_password: str, 
                          auto_position: bool = True) -> bool:
        """
        Set up replication from a master server
        
        Args:
            master_host: Master server hostname
            master_port: Master server port
            master_user: Replication user
            master_password: Replication password
            auto_position: Use GTID auto position
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Stop slave if running
            self.execute_write("STOP SLAVE")
            
            # Set up replication
            change_master_sql = f"""
            CHANGE MASTER TO
                MASTER_HOST = '{master_host}',
                MASTER_PORT = {master_port},
                MASTER_USER = '{master_user}',
                MASTER_PASSWORD = '{master_password}',
                MASTER_AUTO_POSITION = {1 if auto_position else 0}
            """
            self.execute_write(change_master_sql)
            
            # Start slave
            self.execute_write("START SLAVE")
            
            logger.info(f"Replication setup successfully from {master_host}:{master_port}")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to set up replication: {e}")
            return False
    
    def start_replication(self) -> bool:
        """
        Start replication
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write("START SLAVE")
            logger.info("Replication started successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to start replication: {e}")
            return False
    
    def stop_replication(self) -> bool:
        """
        Stop replication
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write("STOP SLAVE")
            logger.info("Replication stopped successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to stop replication: {e}")
            return False
    
    def reset_replication(self) -> bool:
        """
        Reset replication
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write("RESET SLAVE ALL")
            logger.info("Replication reset successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to reset replication: {e}")
            return False
    
    def check_replication_health(self) -> Dict[str, Any]:
        """
        Check replication health
        
        Returns:
            Dict: Replication health information
        """
        status = self.get_replication_status()
        if not status:
            return {'status': 'Not a replica'}
        
        health = {
            'status': 'Healthy',
            'io_running': status.get('Slave_IO_Running', 'No'),
            'sql_running': status.get('Slave_SQL_Running', 'No'),
            'seconds_behind_master': status.get('Seconds_Behind_Master', 'NULL'),
            'last_error': status.get('Last_Error', ''),
            'last_io_error': status.get('Last_IO_Error', ''),
            'last_sql_error': status.get('Last_SQL_Error', '')
        }
        
        if health['io_running'] != 'Yes' or health['sql_running'] != 'Yes':
            health['status'] = 'Error'
        elif health['seconds_behind_master'] != 'NULL' and int(health['seconds_behind_master']) > 300:
            health['status'] = 'Warning'
        
        return health
    
    # User Management
    
    def create_user(self, username: str, password: str, host: str = '%') -> bool:
        """
        Create a new user
        
        Args:
            username: Username
            password: Password
            host: Host from which the user can connect
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write(f"CREATE USER '{username}'@'{host}' IDENTIFIED BY '{password}'")
            logger.info(f"User '{username}'@'{host}' created successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to create user '{username}'@'{host}': {e}")
            return False
    
    def drop_user(self, username: str, host: str = '%') -> bool:
        """
        Drop a user
        
        Args:
            username: Username
            host: Host from which the user can connect
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write(f"DROP USER '{username}'@'{host}'")
            logger.info(f"User '{username}'@'{host}' dropped successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to drop user '{username}'@'{host}': {e}")
            return False
    
    def grant_privileges(self, username: str, password: str, database: str = '*', 
                         table: str = '*', host: str = '%', privileges: str = 'ALL') -> bool:
        """
        Grant privileges to a user
        
        Args:
            username: Username
            password: Password
            database: Database name
            table: Table name
            host: Host from which the user can connect
            privileges: Privileges to grant
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write(f"GRANT {privileges} ON `{database}`.`{table}` TO '{username}'@'{host}'")
            self.execute_write("FLUSH PRIVILEGES")
            logger.info(f"Privileges granted to '{username}'@'{host}' on '{database}.{table}'")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to grant privileges to '{username}'@'{host}': {e}")
            return False
    
    def revoke_privileges(self, username: str, database: str = '*', 
                          table: str = '*', host: str = '%', privileges: str = 'ALL') -> bool:
        """
        Revoke privileges from a user
        
        Args:
            username: Username
            database: Database name
            table: Table name
            host: Host from which the user can connect
            privileges: Privileges to revoke
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write(f"REVOKE {privileges} ON `{database}`.`{table}` FROM '{username}'@'{host}'")
            self.execute_write("FLUSH PRIVILEGES")
            logger.info(f"Privileges revoked from '{username}'@'{host}' on '{database}.{table}'")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to revoke privileges from '{username}'@'{host}': {e}")
            return False
    
    def get_users(self) -> List[Dict[str, Any]]:
        """
        Get list of users
        
        Returns:
            List[Dict]: User information
        """
        return self.execute_query("SELECT * FROM mysql.user")
    
    # Performance Monitoring
    
    def get_slow_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get slow queries from slow query log
        
        Args:
            limit: Maximum number of queries to return
            
        Returns:
            List[Dict]: Slow query information
        """
        return self.execute_query(f"""
            SELECT * FROM mysql.slow_log
            ORDER BY start_time DESC
            LIMIT {limit}
        """)
    
    def get_table_size(self, database: str = None) -> List[Dict[str, Any]]:
        """
        Get table sizes
        
        Args:
            database: Optional database name to filter tables
            
        Returns:
            List[Dict]: Table size information
        """
        where_clause = f"WHERE table_schema = '{database}'" if database else ""
        return self.execute_query(f"""
            SELECT 
                table_schema as 'database',
                table_name as 'table',
                round(((data_length + index_length) / 1024 / 1024), 2) as 'size_mb'
            FROM information_schema.TABLES
            {where_clause}
            ORDER BY (data_length + index_length) DESC
        """)
    
    def get_innodb_status(self) -> str:
        """
        Get InnoDB status
        
        Returns:
            str: InnoDB status
        """
        result = self.execute_query("SHOW ENGINE INNODB STATUS")
        if result:
            return result[0]['Status']
        return ""
    
    def optimize_table(self, database: str, table: str) -> bool:
        """
        Optimize a table
        
        Args:
            database: Database name
            table: Table name
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write(f"OPTIMIZE TABLE `{database}`.`{table}`")
            logger.info(f"Table '{database}.{table}' optimized successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to optimize table '{database}.{table}': {e}")
            return False
    
    def analyze_table(self, database: str, table: str) -> bool:
        """
        Analyze a table
        
        Args:
            database: Database name
            table: Table name
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write(f"ANALYZE TABLE `{database}`.`{table}`")
            logger.info(f"Table '{database}.{table}' analyzed successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to analyze table '{database}.{table}': {e}")
            return False
    
    def get_locks(self) -> List[Dict[str, Any]]:
        """
        Get current locks
        
        Returns:
            List[Dict]: Lock information
        """
        return self.execute_query("""
            SELECT * FROM performance_schema.data_locks
        """)
    
    def get_deadlocks(self) -> List[Dict[str, Any]]:
        """
        Get recent deadlocks
        
        Returns:
            List[Dict]: Deadlock information
        """
        innodb_status = self.get_innodb_status()
        deadlocks = []
        
        # Extract deadlock information from InnoDB status
        if "LATEST DETECTED DEADLOCK" in innodb_status:
            deadlock_section = innodb_status.split("LATEST DETECTED DEADLOCK")[1]
            deadlock_section = deadlock_section.split("TRANSACTIONS")[0]
            deadlocks.append({"deadlock_info": deadlock_section.strip()})
        
        return deadlocks
    
    # Backup and Recovery Functions
    
    def create_backup_user(self, username: str = 'backup_user', 
                          password: str = None, host: str = 'localhost') -> bool:
        """
        Create a backup user with appropriate privileges
        
        Args:
            username: Backup username
            password: Backup password (auto-generated if None)
            host: Host from which the backup user can connect
            
        Returns:
            bool: True if successful, False otherwise
        """
        if password is None:
            import random
            import string
            password = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
        
        try:
            # Create user
            self.create_user(username, password, host)
            
            # Grant necessary privileges
            self.execute_write(f"""
                GRANT RELOAD, LOCK TABLES, REPLICATION CLIENT, CREATE TABLESPACE, PROCESS, 
                      SUPER, REPLICATION SLAVE, BACKUP_ADMIN ON *.* TO '{username}'@'{host}'
            """)
            self.execute_write("FLUSH PRIVILEGES")
            
            logger.info(f"Backup user '{username}'@'{host}' created successfully")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to create backup user '{username}'@'{host}': {e}")
            return False
    
    def get_binary_logs(self) -> List[Dict[str, Any]]:
        """
        Get list of binary logs
        
        Returns:
            List[Dict]: Binary log information
        """
        return self.execute_query("SHOW BINARY LOGS")
    
    def purge_binary_logs(self, before_date: str) -> bool:
        """
        Purge binary logs before a specified date
        
        Args:
            before_date: Date in YYYY-MM-DD format
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.execute_write(f"PURGE BINARY LOGS BEFORE '{before_date}'")
            logger.info(f"Binary logs purged before {before_date}")
            return True
        except pymysql.Error as e:
            logger.error(f"Failed to purge binary logs: {e}")
            return False
    
    # MySQL Health Check
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a comprehensive health check of the MySQL server
        
        Returns:
            Dict: Health check results
        """
        health = {
            'status': 'Healthy',
            'version': self.get_version(),
            'uptime': self.get_status('Uptime')['Uptime'],
            'connections': {
                'max_connections': self.get_variables('max_connections')['max_connections'],
                'current_connections': self.get_status('Threads_connected')['Threads_connected'],
                'connection_errors': self.get_status('Connection_errors%')
            },
            'memory': {
                'innodb_buffer_pool_size': self.get_variables('innodb_buffer_pool_size')['innodb_buffer_pool_size'],
                'innodb_buffer_pool_usage': self.get_status('Innodb_buffer_pool%')
            },
            'threads': {
                'running': self.get_status('Threads_running')['Threads_running'],
                'connected': self.get_status('Threads_connected')['Threads_connected'],
                'created': self.get_status('Threads_created')['Threads_created']
            },
            'queries': {
                'slow_queries': self.get_status('Slow_queries')['Slow_queries'],
                'questions': self.get_status('Questions')['Questions'],
                'com_select': self.get_status('Com_select')['Com_select'],
                'com_insert': self.get_status('Com_insert')['Com_insert'],
                'com_update': self.get_status('Com_update')['Com_update'],
                'com_delete': self.get_status('Com_delete')['Com_delete']
            }
        }
        
        # Check for replication if it's a replica
        replication_status = self.get_replication_status()
        if replication_status:
            health['replication'] = self.check_replication_health()
            if health['replication']['status'] != 'Healthy':
                health['status'] = health['replication']['status']
        
        # Check for high thread usage
        max_connections = int(health['connections']['max_connections'])
        current_connections = int(health['connections']['current_connections'])
        if current_connections > max_connections * 0.8:
            health['status'] = 'Warning'
            health['warnings'] = health.get('warnings', []) + [f'High connection usage: {current_connections}/{max_connections}']
        
        # Check for high thread running
        threads_running = int(health['threads']['running'])
        if threads_running > 30:
            health['status'] = 'Warning'
            health['warnings'] = health.get('warnings', []) + [f'High number of running threads: {threads_running}']
        
        return health

# Usage example
if __name__ == "__main__":
    # Create a MySQL client
    mysql = MySQLClient(
        host="localhost",
        port=3306,
        user="root",
        password="password"
    )
    
    # Connect to the MySQL server
    if mysql.connect():
        # Get server version
        version = mysql.get_version()
        print(f"MySQL Server Version: {version}")
        
        # Get list of databases
        databases = mysql.get_databases()
        print(f"Databases: {', '.join(databases)}")
        
        # Perform a health check
        health = mysql.health_check()
        print(f"Server Health: {health['status']}")
        
        # Clean up
        mysql.disconnect()
