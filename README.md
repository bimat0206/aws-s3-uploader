# S3 Folder Uploader

## Overview
This application allows you to upload a local folder to an S3 bucket with flexible AWS credential configuration and allows multiple files to be uploaded simultaneously, which is faster than normal.

## Prerequisites
- Go 1.16+ installed
- AWS credentials configured via one of the following methods:
  1. AWS CLI profile
  2. Explicit access key and secret key
  3. Default AWS credential chain (environment variables, AWS config file)

## Installation
1. Clone the repository
2. Run `go mod tidy` to install dependencies

## Configuration
Create a `config.json` file with the following structure:

```json
{
    "aws_profile": "",           // Optional: AWS CLI profile name
    "access_key": "",             // Optional: AWS Access Key ID
    "secret_key": "",             // Optional: AWS Secret Access Key
    "region": "us-east-1",        // Required: AWS Region
    "bucket_name": "my-bucket",   // Required: S3 Bucket Name
    "local_path": "/path/to/local/folder", // Required: Local folder to upload
    "s3_prefix": "uploads/"       // Optional: Prefix in S3 bucket
}
```

### Credential Configuration Methods (in order of priority)
1. **AWS CLI Profile**: Set `aws_profile` to use an existing AWS CLI profile
2. **Explicit Credentials**: Provide `access_key` and `secret_key`
3. **Default Credential Chain**: Relies on environment variables or AWS config file

## Usage
Run the application:
```bash
go run main.go [optional_config_path]
```

If no config path is provided, it will look for `config.json` in the current directory.

## Features
- Concurrent file uploads
- Flexible AWS credential configuration
- Preserves local folder structure in S3
- Optional S3 prefix support
- Error handling and reporting

## Error Handling
- Validates required configuration fields
- Provides detailed error messages
- Continues uploading other files if some fail

## Performance
- The concurrent upload approach allows multiple files to be uploaded simultaneously
- Large files or many small files will benefit from this approach
- Network and AWS S3 service limits may impact maximum concurrent uploads

## Security Considerations
- Do not commit `config.json` with actual credentials to version control
- Use environment variables or AWS CLI profiles for sensitive credentials
