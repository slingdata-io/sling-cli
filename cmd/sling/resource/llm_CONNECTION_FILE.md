# Sling File System Connection MCP Guide for LLMs

## Table of Contents

1. [Introduction and Overview](#1-introduction-and-overview)
2. [MCP File System Tool Integration](#2-mcp-file-system-tool-integration)
3. [File System Operations](#3-file-system-operations)
4. [File Listing and Discovery](#4-file-listing-and-discovery)
5. [File Copy Operations](#5-file-copy-operations)
6. [File Inspection and Metadata](#6-file-inspection-and-metadata)
7. [Common Usage Patterns](#7-common-usage-patterns)
8. [Storage-Specific Examples](#8-storage-specific-examples)
9. [Best Practices](#9-best-practices)
10. [Troubleshooting File Operations](#10-troubleshooting-file-operations)

---

## 1. Introduction and Overview

### What are File System Operations?

File system operations in Sling provide comprehensive functionality for interacting with file-based connections, including cloud storage, local file systems, and file transfer protocols. These operations enable file listing, copying, inspection, and metadata retrieval across multiple storage platforms.

### Key Capabilities

- **File Listing**: Browse directories and discover files with pattern matching
- **File Copying**: Transfer files between different storage systems
- **File Inspection**: Get detailed metadata about files and directories
- **Cross-Platform Support**: Works with 13+ file system types with consistent interface
- **Pattern Matching**: Advanced filtering with glob patterns for file discovery

### Supported File System Types

- **Cloud Storage**: AWS S3, Google Cloud Storage, Azure Blob Storage
- **S3-Compatible**: MinIO, DigitalOcean Spaces, Cloudflare R2, Wasabi, Backblaze B2
- **File Transfer**: FTP, SFTP
- **Cloud Drives**: Google Drive
- **Local**: Local file system

---

## 2. MCP File System Tool Integration

The `file_system` tool provides file system-specific operations through various actions. All operations require an existing file system connection to be configured.

### Available Actions

- `docs` - Get comprehensive file system documentation
- `list` - List files in a file system connection
- `copy` - Copy files between connections
- `inspect` - Inspect file/directory metadata

### Basic Tool Usage

```json
{
  "action": "action_name",
  "input": {
    "connection": "FILE_CONNECTION_NAME",
    "parameter1": "value1",
    "parameter2": "value2"
  }
}
```

### Getting Documentation

```json
{
  "action": "docs",
  "input": {}
}
```

**Important**: File system operations require a Pro token and are rate-limited.

---

## 3. File System Operations

### Prerequisites

Before using file system operations:

1. **Connection Setup**: File system connection must be configured using the `connection` tool
2. **Connection Testing**: Verify connection works with `connection` tool `test` action
3. **Permissions**: Connection must have appropriate read/write permissions
4. **Token**: Operations require a valid Sling CLI Pro token

### Operation Categories

#### File Discovery Operations
- `list` - Browse directories and list files

#### File Management Operations
- `copy` - Transfer files between connections
- `inspect` - Get file and directory metadata

---

## 4. File Listing and Discovery

### Listing Files

List files and directories in a file system connection:

```json
{
  "action": "list",
  "input": {
    "connection": "MY_S3",
    "path": "data/",
    "recursive": false,
    "only": "files"
  }
}
```

**Parameters:**
- `connection` (required) - File system connection name
- `path` (required) - Path to list files from
- `recursive` (optional) - Whether to list files recursively (default: false)
- `only` (optional) - Filter results to only 'files' or 'folders'

**Filter Options:**
- `"files"` - Show only files
- `"folders"` - Show only directories/folders
- *omit parameter* - Show both files and folders

### Basic File Listing

List files in a specific directory:
```json
{
  "action": "list",
  "input": {
    "connection": "MY_S3",
    "path": "data/csv_files/",
    "recursive": false,
    "only": "files"
  }
}
```

### Recursive Listing

List all files recursively in a folder structure:
```json
{
  "action": "list",
  "input": {
    "connection": "LOCAL_FS",
    "path": "/home/user/documents",
    "recursive": true
  }
}
```

### Directory-Only Listing

List only directories/folders:
```json
{
  "action": "list",
  "input": {
    "connection": "MY_AZURE",
    "path": "containers/",
    "only": "folders"
  }
}
```

### Path Patterns

File system paths vary by connection type:

**Cloud Storage (S3, GCS, Azure):**
- `data/` - List contents of data folder
- `logs/2024/` - List contents of logs/2024 folder
- `/` or `""` - List root-level contents

**Local File System:**
- `/absolute/path/` - Absolute path (Unix/Linux)
- `C:\Windows\` - Absolute path (Windows)
- `relative/path/` - Relative path

**FTP/SFTP:**
- `/home/user/` - Absolute path
- `uploads/` - Relative path from connection root

---

## 5. File Copy Operations

### Copying Files

Copy files between different connections:

```json
{
  "action": "copy",
  "input": {
    "source_location": "MY_S3/data/input.csv",
    "target_location": "LOCAL/tmp/output.csv",
    "recursive": false
  }
}
```

**Parameters:**
- `source_location` (required) - Source in format `connection_name/path`
- `target_location` (required) - Target in format `connection_name/path`
- `recursive` (optional) - Copy recursively for directories (default: false)

### Location String Format

The location string format combines connection name and path:

**Format:** `CONNECTION_NAME/path/to/resource`

**Examples:**
- Cloud storage: `MY_S3/folder/file.txt`
- Local relative: `LOCAL/relative/path/file.txt`
- Local absolute: `LOCAL//absolute/path/file.txt` *(note double slash for absolute)*

### Single File Copy

Copy a single file between connections:
```json
{
  "action": "copy",
  "input": {
    "source_location": "local/path/to/source.csv",
    "target_location": "s3/bucket/folder/destination.csv",
    "recursive": false
  }
}
```

### Directory Copy

Copy entire directory recursively:
```json
{
  "action": "copy",
  "input": {
    "source_location": "s3/bucket/source_folder/",
    "target_location": "local/backup/target_folder/",
    "recursive": true
  }
}
```

### Multi-Cloud Copy

Copy files between different cloud providers:
```json
{
  "action": "copy",
  "input": {
    "source_location": "AWS_S3/data/export.csv",
    "target_location": "GCS_BUCKET/imports/data.csv"
  }
}
```

### Local to Cloud Backup

Backup local files to cloud storage:
```json
{
  "action": "copy",
  "input": {
    "source_location": "LOCAL//home/user/documents/",
    "target_location": "BACKUP_S3/daily-backup/documents/",
    "recursive": true
  }
}
```

---

## 6. File Inspection and Metadata

### Inspecting Files

Get metadata about files or directories:

```json
{
  "action": "inspect",
  "input": {
    "connection": "MY_S3",
    "path": "data/large_dataset/",
    "recursive": true
  }
}
```

**Parameters:**
- `connection` (required) - File system connection name
- `path` (required) - Path to inspect
- `recursive` (optional) - Get recursive statistics for directories (default: false)

### File Metadata

Inspect a single file:
```json
{
  "action": "inspect",
  "input": {
    "connection": "MY_S3",
    "path": "data/large_file.parquet"
  }
}
```

**Returns:**
- File size
- Creation time
- Modification time
- Content type/MIME type
- Storage class (cloud storage)
- Permissions (where applicable)

### Directory Statistics

Get recursive directory statistics:
```json
{
  "action": "inspect",
  "input": {
    "connection": "LOCAL_FS",
    "path": "/var/log/",
    "recursive": true
  }
}
```

**Returns:**
- Total size
- File count
- Directory count
- Size distribution
- Last modified times

### Batch File Inspection

Inspect multiple files by combining with list operation:
```json
// First list files
{
  "action": "list",
  "input": {
    "connection": "MY_S3",
    "path": "data/",
    "only": "files"
  }
}

// Then inspect specific files based on list results
{
  "action": "inspect",
  "input": {
    "connection": "MY_S3",
    "path": "data/important_file.csv"
  }
}
```

---

## 7. Common Usage Patterns

### File Discovery Workflow

1. **Browse Root Directory**
```json
{
  "action": "list",
  "input": {
    "connection": "MY_STORAGE",
    "path": ""
  }
}
```

2. **Explore Specific Folders**
```json
{
  "action": "list",
  "input": {
    "connection": "MY_STORAGE",
    "path": "data/2024/",
    "recursive": true,
    "only": "files"
  }
}
```

3. **Inspect Interesting Files**
```json
{
  "action": "inspect",
  "input": {
    "connection": "MY_STORAGE",
    "path": "data/2024/large_dataset.parquet"
  }
}
```

### Data Migration Workflow

1. **List Source Files**
```json
{
  "action": "list",
  "input": {
    "connection": "OLD_STORAGE",
    "path": "legacy_data/",
    "recursive": true
  }
}
```

2. **Copy Files to New Location**
```json
{
  "action": "copy",
  "input": {
    "source_location": "OLD_STORAGE/legacy_data/",
    "target_location": "NEW_STORAGE/migrated_data/",
    "recursive": true
  }
}
```

3. **Verify Copy Completed**
```json
{
  "action": "inspect",
  "input": {
    "connection": "NEW_STORAGE",
    "path": "migrated_data/",
    "recursive": true
  }
}
```

### Backup and Sync Pattern

1. **Inspect Source Directory**
```json
{
  "action": "inspect",
  "input": {
    "connection": "LOCAL",
    "path": "/important/data/",
    "recursive": true
  }
}
```

2. **Create Timestamped Backup**
```json
{
  "action": "copy",
  "input": {
    "source_location": "LOCAL//important/data/",
    "target_location": "BACKUP_S3/backups/2024-08-24/data/",
    "recursive": true
  }
}
```

3. **Verify Backup Size**
```json
{
  "action": "inspect",
  "input": {
    "connection": "BACKUP_S3",
    "path": "backups/2024-08-24/",
    "recursive": true
  }
}
```

### File Organization Pattern

1. **List Unorganized Files**
```json
{
  "action": "list",
  "input": {
    "connection": "MESSY_STORAGE",
    "path": "uploads/",
    "only": "files"
  }
}
```

2. **Copy Files to Organized Structure**
```json
{
  "action": "copy",
  "input": {
    "source_location": "MESSY_STORAGE/uploads/document1.pdf",
    "target_location": "ORGANIZED_STORAGE/documents/2024/document1.pdf"
  }
}
```

3. **Verify Organization**
```json
{
  "action": "list",
  "input": {
    "connection": "ORGANIZED_STORAGE",
    "path": "documents/",
    "recursive": true
  }
}
```

---

## 8. Storage-Specific Examples

### AWS S3

```json
// List S3 bucket contents
{
  "action": "list",
  "input": {
    "connection": "MY_S3",
    "path": "data/logs/",
    "recursive": false
  }
}

// Copy to S3 with folder structure
{
  "action": "copy",
  "input": {
    "source_location": "LOCAL/exports/",
    "target_location": "MY_S3/processed/daily/",
    "recursive": true
  }
}

// Inspect S3 object metadata
{
  "action": "inspect",
  "input": {
    "connection": "MY_S3",
    "path": "processed/daily/report.parquet"
  }
}
```

### Google Cloud Storage

```json
// List GCS bucket
{
  "action": "list",
  "input": {
    "connection": "MY_GCS",
    "path": "analytics/",
    "only": "files"
  }
}

// Copy between GCS and local
{
  "action": "copy",
  "input": {
    "source_location": "MY_GCS/analytics/results.csv",
    "target_location": "LOCAL/downloads/gcs_results.csv"
  }
}
```

### Azure Blob Storage

```json
// List Azure container
{
  "action": "list",
  "input": {
    "connection": "MY_AZURE",
    "path": "container/data/",
    "recursive": true
  }
}

// Copy to Azure Blob
{
  "action": "copy",
  "input": {
    "source_location": "LOCAL/uploads/file.json",
    "target_location": "MY_AZURE/container/processed/file.json"
  }
}
```

### Local File System

```json
// List local directory
{
  "action": "list",
  "input": {
    "connection": "LOCAL",
    "path": "/tmp/processing/",
    "only": "files"
  }
}

// Copy local files
{
  "action": "copy",
  "input": {
    "source_location": "LOCAL//home/user/data/",
    "target_location": "LOCAL//tmp/backup/",
    "recursive": true
  }
}

// Inspect local file
{
  "action": "inspect",
  "input": {
    "connection": "LOCAL",
    "path": "/tmp/large_file.db"
  }
}
```

### SFTP

```json
// List SFTP directory
{
  "action": "list",
  "input": {
    "connection": "MY_SFTP",
    "path": "/uploads/",
    "recursive": false
  }
}

// Copy from SFTP to cloud
{
  "action": "copy",
  "input": {
    "source_location": "MY_SFTP/uploads/data.csv",
    "target_location": "MY_S3/ingestion/sftp_data.csv"
  }
}
```

### FTP

```json
// List FTP directory
{
  "action": "list",
  "input": {
    "connection": "MY_FTP",
    "path": "/pub/data/",
    "only": "files"
  }
}

// Download from FTP
{
  "action": "copy",
  "input": {
    "source_location": "MY_FTP/pub/data/report.txt",
    "target_location": "LOCAL/downloads/ftp_report.txt"
  }
}
```

---

## 9. Best Practices

### File Operations

1. **Test Connections First**: Always verify connection works before file operations
2. **Use Specific Paths**: Avoid overly broad recursive operations on large directories  
3. **Check Permissions**: Ensure connections have appropriate read/write access
4. **Monitor Transfer Progress**: For large files, consider breaking into smaller operations
5. **Verify Copy Results**: Use inspect to confirm successful file transfers

### Performance Optimization

1. **Batch Operations**: Group related file operations when possible
2. **Avoid Deep Recursion**: Limit recursive operations on deep directory structures
3. **Use Filters**: Apply "only" parameter to reduce unnecessary results
4. **Regional Proximity**: Use storage regions close to your processing location
5. **Compression**: Enable compression for large file transfers when supported

### Security and Access

1. **Least Privilege**: Grant minimal necessary permissions to connections
2. **Credential Management**: Use secure credential storage and rotation
3. **Path Validation**: Validate paths to prevent unauthorized access
4. **Encryption**: Use encrypted connections (HTTPS, SFTP) when available
5. **Access Logging**: Monitor file access patterns for security

### Error Prevention

1. **Path Existence**: Verify paths exist before operations
2. **Space Availability**: Check available storage space for large copies
3. **Overwrite Protection**: Be cautious with copy operations that may overwrite
4. **Connection Timeouts**: Set appropriate timeouts for large operations
5. **Retry Logic**: Implement retry patterns for network-dependent operations

---

## 10. Troubleshooting File Operations

### Common File Operation Errors

**Path Not Found**
- Verify path exists and is accessible
- Check connection permissions
- Ensure proper path format for connection type

**Permission Denied**
- Verify connection credentials have necessary permissions
- Check file/directory access rights
- Ensure write permissions for copy target locations

**Connection Timeouts**
- Check network connectivity
- Verify storage service availability
- Consider breaking large operations into smaller chunks

### List Operation Issues

**Empty Results**
- Verify path exists and contains files
- Check connection permissions for directory access
- Ensure proper path format (trailing slashes, etc.)

**Slow Listing**
- Avoid overly broad recursive operations
- Use specific paths instead of root-level listings
- Consider pagination for very large directories

**Missing Files**
- Check if files have been moved or deleted
- Verify connection time zone settings
- Confirm path case sensitivity requirements

### Copy Operation Issues

**Copy Failures**
- Verify source file exists and is accessible
- Check target location has write permissions
- Ensure sufficient storage space at target
- Confirm no file locks or exclusive access issues

**Incomplete Copies**
- Check for network interruptions during transfer
- Verify file sizes match between source and target
- Consider retry mechanisms for large files

**Overwrite Issues**
- Check if target file already exists
- Verify overwrite permissions
- Consider backup strategies before overwriting

### Inspect Operation Issues

**Missing Metadata**
- Some storage types provide limited metadata
- Check if connection supports detailed inspection
- Verify file exists and is accessible

**Slow Inspection**
- Large directories with recursive inspection can be slow
- Consider non-recursive inspection first
- Use specific file paths when possible

### Storage-Specific Issues

**AWS S3**
- Verify S3 bucket permissions and IAM roles
- Check S3 bucket region matches connection configuration
- Ensure S3 bucket exists and is accessible

**Google Cloud Storage**
- Confirm GCS bucket permissions
- Verify service account has appropriate roles
- Check if GCS APIs are enabled for the project

**Azure Blob Storage**
- Verify Azure storage account access keys
- Check container permissions and access levels
- Ensure storage account is accessible from your location

**Local File System**
- Check file system permissions (chmod/chown on Unix)
- Verify paths exist and are accessible
- Consider disk space limitations

**FTP/SFTP**
- Verify server accessibility and credentials
- Check firewall settings for FTP passive/active modes
- Confirm SSH key authentication for SFTP

### Getting Help

When troubleshooting file system operations:

1. **Test Basic Connectivity**: Use `connection` tool to verify connection works
2. **Start Simple**: Begin with basic list operations before complex copies
3. **Check Paths**: Verify all paths exist and are properly formatted
4. **Monitor Permissions**: Ensure appropriate access rights for all operations
5. **Use Debug Logging**: Enable debug mode in connection testing

### Error Message Patterns

**"could not init file connection - connection must be a file/storage type"**
- Using file operations on a database connection
- Verify connection type is appropriate for file operations

**"access denied" or "permission denied"**
- Insufficient permissions for file/directory access
- Check connection credentials and access rights

**"path not found" or "no such file or directory"**
- Specified path does not exist
- Verify path spelling and existence

**"connection timeout"**
- Network or server connectivity issue
- Check network connection and server availability

---

This comprehensive guide covers all aspects of file system operations using the Sling MCP `file_system` tool. All operations provide structured access to file resources across multiple storage platforms with consistent interfaces and error handling.