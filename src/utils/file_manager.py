"""
File Manager Utility
Handles temporary file creation and cleanup.
"""

import os
import tempfile
import shutil
from pathlib import Path
from typing import List, Optional


class TempFileManager:
    """Manages temporary files and directories."""
    
    def __init__(self, base_dir: Optional[str] = None):
        """
        Initialize temp file manager.
        
        Args:
            base_dir: Optional base directory for temp files. 
                     If None, uses system temp directory.
        """
        if base_dir:
            self.temp_dir = Path(base_dir)
            self.temp_dir.mkdir(parents=True, exist_ok=True)
        else:
            # Create a temporary directory
            self.temp_dir = Path(tempfile.mkdtemp(prefix="s3_adls_connector_"))
        
        self.created_files: List[str] = []
        self.created_dirs: List[str] = []
    
    def create_temp_file(self, filename: str) -> str:
        """
        Create a temporary file path (doesn't create the file itself).
        
        Args:
            filename: Name of the file
        
        Returns:
            Full path to the temporary file
        """
        file_path = self.temp_dir / filename
        return str(file_path)
    
    def cleanup(self):
        """Delete all temporary files and directories."""
        # Delete all created files
        for file_path in self.created_files:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"Warning: Could not delete file {file_path}: {e}")
        
        # Delete temp directory if we created it
        try:
            if self.temp_dir.exists() and self.temp_dir.is_dir():
                shutil.rmtree(self.temp_dir)
        except Exception as e:
            print(f"Warning: Could not delete directory {self.temp_dir}: {e}")
    
    def get_temp_dir(self) -> str:
        """Get the temporary directory path."""
        return str(self.temp_dir)

