#!/usr/bin/env python3
"""
MySQL Clone Recovery Tool

This script performs database recovery using MySQL's clone plugin feature.
It executes from machine A, with source database B and target database C.
"""

import argparse
import sys
import time
import paramiko
import pymysql
from tqdm import tqdm
import re
import os
import getpass

class MySQLCloneRecovery:
    def __init__(self, source_host, source_port, source_user, source_password,
                 target_host, target_port, target_user, target_password):
        """Initialize with connection parameters for source and target databases."""
        self.source_host = source_host
        self.source_port = source_port
        self.source_user = source_user
        self.source_password = source_password
        
        self.target_host = target_host
        self.target_port = target_port
        self.target_user = target_user
        self.target_password = target_password
        
        self.source_conn = None
        self.target_conn = None
        self.ssh_client = None
        
    def connect_to_source(self):
        """Connect to source database."""
        try:
            self.source_conn = pymysql.connect(
                host=self.source_host,
                port=self.source_port,
                user=self.source_user,
                password=self.source_password
            )
            print(f"✓ Successfully connected to source database at {self.source_host}:{self.source_port}")
            return True
        except pymysql.MySQLError as err:
            if err.args[0] == 1045:
                print(f"❌ Error: Invalid credentials for source database")
            elif err.args[0] == 2003:
                print(f"❌ Error: Cannot connect to source database at {self.source_host}:{self.source_port}")
            else:
                print(f"❌ Error connecting to source database: {err}")
            return False
    
    def connect_to_target(self):
        """Connect to target database."""
        try:
            self.target_conn = pymysql.connect(
                host=self.target_host,
                port=self.target_port,
                user=self.target_user,
                password=self.target_password
            )
            print(f"✓ Successfully connected to target database at {self.target_host}:{self.target_port}")
            return True
        except pymysql.MySQLError as err:
            if err.args[0] == 1045:
                print(f"❌ Error: Invalid credentials for target database")
            elif err.args[0] == 2003:
                print(f"❌ Error: Cannot connect to target database at {self.target_host}:{self.target_port}")
            else:
                print(f"❌ Error connecting to target database: {err}")
            return False
    
    def check_plugin_status(self, connection, host_type):
        """Check if clone plugin is installed and active."""
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT PLUGIN_NAME, PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME = 'clone'")
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            print(f"❌ Clone plugin is not installed on {host_type} database")
            return False
        elif result['PLUGIN_STATUS'] != 'ACTIVE':
            print(f"❌ Clone plugin is installed but not active on {host_type} database")
            return False
        else:
            print(f"✓ Clone plugin is active on {host_type} database")
            return True
    
    def check_active_connections(self):
        """Check for active transactions on target database."""
        cursor = self.target_conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("""
            SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO
            FROM INFORMATION_SCHEMA.PROCESSLIST
            WHERE COMMAND not in ('Daemon','Sleep') AND USER != %s AND ID != CONNECTION_ID()
        """, (self.target_user,))
        active_connections = cursor.fetchall()
        cursor.close()
        
        if active_connections:
            print(f"❌ Found {len(active_connections)} active connections on target database:")
            for conn in active_connections:
                print(f"  - ID: {conn['ID']}, User: {conn['USER']}@{conn['HOST']}, DB: {conn['DB']}, Command: {conn['COMMAND']}, Time: {conn['TIME']}s")
            return False
        else:
            print("✓ No active connections found on target database")
            return True
    
    def ssh_connect(self, host):
        """Connect to host via SSH."""
        try:
            self.ssh_client = paramiko.SSHClient()
            self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh_client.connect(hostname=host, username=getpass.getuser())
            print(f"✓ Successfully connected to {host} via SSH")
            return True
        except Exception as e:
            print(f"❌ Error connecting to {host} via SSH: {str(e)}")
            return False
    
    def execute_clone(self):
        """Execute clone operation from source to target."""
        try:
            print("\n=== Starting Clone Operation ===")
            
            # Get MySQL data directory of target
            cursor = self.target_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("SELECT @@datadir as datadir")
            datadir = cursor.fetchone()['datadir']
            
            # Set valid donor list
            donor_stmt=f"SET GLOBAL clone_valid_donor_list = '{self.source_host}:{self.source_port}'"
            cursor.execute(donor_stmt)
            cursor.close()
            
            # Prepare and execute clone statement
            clone_stmt = f"""
            CLONE INSTANCE FROM '{self.source_user}'@'{self.source_host}':{self.source_port}
            IDENTIFIED BY '{self.source_password}'
            """
            # Start monitoring progress in a separate SSH session
            if self.ssh_connect(self.target_host):
                # Start background monitoring
                monitor_cmd = f"watch -n 1 'mysql -u{self.target_user} -p{self.target_password} -e \"SELECT * FROM performance_schema.clone_progress\"'"
                stdin, stdout, stderr = self.ssh_client.exec_command(f"nohup {monitor_cmd} > /tmp/clone_progress.log 2>&1 &")
                
                # Execute clone statement
                print(f"Starting clone from {self.source_host} to {self.target_host}...")
                cursor = self.target_conn.cursor()
                
                # The connection will be closed during clone, so we need to execute and catch the expected error
                try:
                    cursor.execute(clone_stmt)
                except pymysql.err.OperationalError as e:
                    if "Lost connection" in str(e):
                        print("Connection lost as expected during clone operation.")
                    else:
                        raise e
                
                # Monitor progress
                print("\nMonitoring clone progress...")
                self._monitor_clone_progress()
                
                return True
            else:
                print("❌ Cannot establish SSH connection to monitor progress")
                return False
                
        except pymysql.MySQLError as err:
            print(f"❌ Error during clone operation: {err}")
            return False
    
    def _monitor_clone_progress(self):
        """Monitor clone progress and display a progress bar."""
        try:
            # Wait for clone to initialize
            time.sleep(5)
            
            # Connect to MySQL for monitoring
            monitoring_conn = None
            retries = 0
            max_retries = 30
            
            # Wait for MySQL to come back online
            pbar = tqdm(total=100, desc="Waiting for target database to come back online")
            while retries < max_retries:
                try:
                    monitoring_conn = pymysql.connect(
                        host=self.target_host,
                        port=self.target_port,
                        user=self.target_user,
                        password=self.target_password
                    )
                    break
                except:
                    retries += 1
                    pbar.update(retries * 3)
                    time.sleep(2)
            pbar.close()
            
            if not monitoring_conn:
                print("❌ Could not reconnect to target database after clone started")
                return
            
            # Monitor clone progress
            cursor = monitoring_conn.cursor(pymysql.cursors.DictCursor)
            completed = False
            error_reported = False
            
            pbar = tqdm(total=100, desc="Clone Progress")
            
            while not completed:
                cursor.execute("SELECT * FROM performance_schema.clone_progress")
                progress_data = cursor.fetchall()
                
                if not progress_data:
                    # No progress data could mean clone completed or failed
                    cursor.execute("SELECT * FROM performance_schema.clone_status")
                    status = cursor.fetchone()
                    if status and status.get('ERROR_NO') == 0:
                        pbar.update(100 - pbar.n)
                        completed = True
                    elif not error_reported:
                        print(f"❌ Clone operation failed or no progress data available")
                        error_reported = True
                    time.sleep(2)
                    continue
                
                # Calculate overall progress
                total_work = 0
                completed_work = 0
                for task in progress_data:
                    total_work += task.get('WORK_ESTIMATED', 0)
                    completed_work += task.get('WORK_COMPLETED', 0)
                
                if total_work > 0:
                    progress_percent = min(int((completed_work / total_work) * 100), 100)
                    pbar.update(progress_percent - pbar.n)
                
                # Check if clone completed
                cursor.execute("SELECT * FROM performance_schema.clone_status")
                status = cursor.fetchone()
                if status:
                    if status.get('STATE') == 'Completed':
                        pbar.update(100 - pbar.n)
                        completed = True
                    elif status.get('STATE') == 'Failed' and not error_reported:
                        print(f"❌ Clone operation failed: {status.get('ERROR_MESSAGE')}")
                        error_reported = True
                
                time.sleep(2)
            
            pbar.close()
            cursor.close()
            monitoring_conn.close()
            
            if completed:
                print("✓ Clone operation completed successfully")
            
        except Exception as e:
            print(f"❌ Error monitoring clone progress: {str(e)}")
    
    def validate_clone(self):
        """Validate the clone operation was successful."""
        try:
            # Try to connect to the target database
            new_conn = pymysql.connect(
                host=self.target_host,
                port=self.target_port,
                user=self.target_user,
                password=self.target_password
            )
            
            cursor = new_conn.cursor(pymysql.cursors.DictCursor)
            
            # Check clone status
            cursor.execute("SELECT * FROM performance_schema.clone_status")
            status = cursor.fetchone()
            
            if status and status.get('STATE') == 'Completed' and status.get('ERROR_NO') == 0:
                print("✓ Clone validation successful")
                
                # Show some basic server information
                cursor.execute("SELECT @@version as version, @@server_id as server_id")
                server_info = cursor.fetchone()
                print(f"\nTarget Server Information:")
                print(f"  - MySQL Version: {server_info['version']}")
                print(f"  - Server ID: {server_info['server_id']}")
                
                cursor.close()
                new_conn.close()
                return True
            else:
                print(f"❌ Clone validation failed: {status.get('ERROR_MESSAGE') if status else 'Unknown error'}")
                cursor.close()
                new_conn.close()
                return False
                
        except pymysql.MySQLError as err:
            print(f"❌ Error during clone validation: {err}")
            return False
    
    def cleanup(self):
        """Close all connections."""
        if self.source_conn:
            self.source_conn.close()
        if self.target_conn:
            self.target_conn.close()
        if self.ssh_client:
            self.ssh_client.close()

def main():
    """Main function to parse arguments and execute the clone recovery."""
    parser = argparse.ArgumentParser(description='MySQL Clone Recovery Tool')
    parser.add_argument('--source-host', required=True, help='Source database hostname')
    parser.add_argument('--source-port', type=int, default=33306, help='Source database port')
    parser.add_argument('--source-user', type=str, default='', help='Source database username')
    parser.add_argument('--source-password', type=str, default='', help='Source database password')
    parser.add_argument('--target-host', required=True, help='Target database hostname')
    parser.add_argument('--target-port', type=int, default=33306, help='Target database port')
    parser.add_argument('--target-user', type=str, default='', help='Target database username')
    parser.add_argument('--target-password', type=str, default='', help='Target database password')
    parser.add_argument('--force', action='store_true', help='Force clone even if active connections exist')
    
    args = parser.parse_args()
    
    # Initialize the recovery tool
    recovery = MySQLCloneRecovery(
        args.source_host, args.source_port, args.source_user, args.source_password,
        args.target_host, args.target_port, args.target_user, args.target_password
    )
    
    try:
        # Pre-flight checks
        print("\n=== Pre-flight Checks ===")
        if not recovery.connect_to_source():
            print("❌ Aborting: Cannot connect to source database")
            sys.exit(1)
            
        if not recovery.connect_to_target():
            print("❌ Aborting: Cannot connect to target database")
            sys.exit(1)
        
        # Check clone plugin status
        if not recovery.check_plugin_status(recovery.source_conn, "source"):
            print("❌ Aborting: Clone plugin issue on source database")
            sys.exit(1)
            
        if not recovery.check_plugin_status(recovery.target_conn, "target"):
            print("❌ Aborting: Clone plugin issue on target database")
            sys.exit(1)
        
        # Check for active connections
        if not recovery.check_active_connections() and not args.force:
            print("❌ Aborting: Active connections found on target database")
            print("   Use --force to proceed anyway")
            sys.exit(1)
        
        # Execute clone operation
        if recovery.execute_clone():
            # Validate the clone
            recovery.validate_clone()
        else:
            print("❌ Clone operation failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Operation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
    finally:
        recovery.cleanup()

if __name__ == "__main__":
    main()
