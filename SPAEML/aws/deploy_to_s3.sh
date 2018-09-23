#!/bin/bash

usage () {
    echo "--------------------------------------------------------------------"
    echo "This script will compile and upload the jar file to a S3 bucket."
    echo "It can optionally skip compilation & upload additional input files."
    echo "All objects uploaded are set to expire in one month."
    echo "--------------------------------------------------------------------"
    echo "WARNINGS:"
    echo "  Must be run in the project directory."
    echo "  AWS CLI must be installed."
    echo "  AWS credentials must be set up. Use 'aws configure' to set up."
    echo "--------------------------------------------------------------------"
    echo "Usage: $0 [-b <bucket name>] [--j <jar file>] [--input <file1> <file2> ...]"
    echo "  required:"
    echo "      -b <bucket name>"
    echo "  optional:"
    echo "      --j <jar file>              to skip compilation and upload jar file directly"
    echo "      --input <file1> <file2> ... to upload input files"
    exit 1
}

DEFAULT_JAR_PATH="target/SPAEML-0.0.1-jar-with-dependencies.jar"
PROJECT_DIRECTORY="SPAEML"

if [[ "$(pwd)" != *"$PROJECT_DIRECTORY" ]]; then
    echo "The script must be run in the project directory!"
    exit 1
fi

# Get bucket name

if [ "$#" -lt 2 ]; then
    usage
fi

if [ "$1" == "-b" ]; then
    BUCKET_NAME=$2
    shift
    shift
else
    usage
fi

# Get Jar file

if [ "$1" == "--j" ]; then
    if [ "$#" -lt 2 ]; then
        usage
    fi
    JAR_FILE_PATH=$2
    shift
    shift
else
    echo "Compiling source code..."
    mvn clean
    if mvn package; then
        echo "--------------------------------------------------------------------"
        echo "Compilation succeeded!"
        echo "JAR file generated at $DEFAULT_JAR_PATH"
        JAR_FILE_PATH=$DEFAULT_JAR_PATH
    else
        echo "--------------------------------------------------------------------"
        echo "Compilation failed!"
        echo "Please fix your code before deployment."
        exit 1
    fi
fi

# Upload to S3

JAR_FILE_NAME=$(basename $JAR_FILE_PATH)

# Set expiration date to one month from now
EXPIRATION_DATE=$(date -v +1m +%FT%TZ)

echo "Uploading files to S3 bucket $BUCKET_NAME..."
echo "NOTE: All expiration date is set to $EXPIRATION_DATE"
echo "Uploading JAR file at $JAR_FILE_PATH..."

aws s3 cp $JAR_FILE_PATH s3://$BUCKET_NAME/$JAR_FILE_NAME --expires $EXPIRATION_DATE

if [ "$1" == "--input" ]; then
    shift
    echo "Uploading input files..."
    for input_file in "$@"
    do
        echo "Uploading input file at $input_file..."
        INPUT_FILE_NAME=$(basename $input_file)
        aws s3 cp $input_file s3://$BUCKET_NAME/$INPUT_FILE_NAME --expire $EXPIRATION_DATE
    done
fi

echo "--------------------------------------------------------------------"
echo "Done!"
