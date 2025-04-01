#!/usr/bin/env python3
"""
SSH Operations Library

A comprehensive library for SSH operations using paramiko.
Includes functions for connection management, command execution,
file transfers, and remote system management.
"""

import os
import sys
import re
import time
import socket
import logging
import paramiko
from typing import List, Dict, Any, Tuple, Optional, Union, BinaryIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ssh_ops")

class SSHClient:
    """SSH operations client using paramiko"""
    
    def __init__(self, host: str, port: int = 22, username: str = None, 
                 password: str = None, key_filename: str = None, 
                 timeout: int = 10, auto_add_policy: bool = True):
        """
        Initialize SSH client connection parameters
        
        Args:
            host: SSH server hostname or IP
            port: SSH server port
            username: SSH username (default: current user)
            password: SSH password (optional)
            key_filename: Path to private key file (optional)
            timeout: Connection timeout in seconds
            auto_add_policy: Automatically add host key policy
        """
        self.host = host
        self.port = port
        self.username = username or os.getlogin()
        self.password = password
        self.key_filename = key_filename
        self.timeout = timeout
        self.auto_add_policy = auto_add_policy
        self.client = None