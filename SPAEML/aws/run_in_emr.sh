#!/bin/bash

usage() {
    echo "This script will create a self-terminating EMR cluster to auto-run SPAEML steps."
    echo "--------------------------------------------------------------------"
    echo "Make sure that all JAR file and input files are deployed to S3 before running this."
    echo "--------------------------------------------------------------------"
    echo "Usage: $0 [-b <bucket name>] [-j <jar name>] [-g <genotype input file>] [-p <phenotype input file>] [-o <output file>] [options...]"
    echo "  required:"
    echo "      -b <bucket name>"
    echo "      -j <jar name>"
    echo "      -g <genotype input file>"
    echo "      -p <phenotype input file>"
    echo "      -o <output file>"
    echo "      -l <run LASSO (true|false)>"
    echo "  optional:"
    echo "      --c <instance-count (integer)>   to specify number of EC2 instances (default=3)"
    echo "      --t <instance-type (string)>   to specify type of EC2 instances (default=m4.large)"
    exit 1
}     

SUBNET_ID="subnet-3a15e677"
CLUSTER_NAME="genomics-epiquant-cluster"
EMR_RELEASE="emr-5.17.0"
EC2_KEY_PAIR="genomics-epiquant-beta-keypair"
INSTANCE_TYPE="m4.large"
INSTANCE_COUNT=3

# Reset POSIX variable in case getopts has been used previously in shell.
OPTIND=1

while getopts "b:j:g:p:o:c:t:l:" opt; do
    case "$opt" in
    b)  BUCKET_NAME=$OPTARG
        ;;
    j)  JAR_FILE=$OPTARG
        ;;
    g)  GENOTYPE_FILE=$OPTARG
        ;;
    p)  PHENOTYPE_FILE=$OPTARG
        ;;
    o)  OUTPUT_FILE=$OPTARG
        ;;
    c)  INSTANCE_COUNT=$OPTARG
        ;;
    t)  INSTANCE_TYPE=$OPTARG
        ;;
    l)  RUN_LASSO=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

if [[ -z $BUCKET_NAME ]] || [[ -z $JAR_FILE ]] || [[ -z $GENOTYPE_FILE ]] || [[ -z $PHENOTYPE_FILE ]] || [[ -z $OUTPUT_FILE ]] || [[ -z $RUN_LASSO ]]; then
    usage
fi

echo "--------------------------------------------------------------------"
echo "Configurations:"
echo "  bucket name     = $BUCKET_NAME"
echo "  jar file        = $JAR_FILE"
echo "  genotype file   = $GENOTYPE_FILE"
echo "  phenotype file  = $PHENOTYPE_FILE"
echo "  output file     = $OUTPUT_FILE"
echo "  run lasso       = $RUN_LASSO"
echo "  instance count  = $INSTANCE_COUNT"
echo "  instance type   = $INSTANCE_TYPE"
echo "--------------------------------------------------------------------"

echo "Creating EMR cluster..."

if aws emr create-cluster                       \
    --name $CLUSTER_NAME                        \
    --release-label $EMR_RELEASE                \
    --applications Name=SPARK                   \
    --ec2-attributes KeyName=$EC2_KEY_PAIR      \
    --instance-type $INSTANCE_TYPE              \
    --instance-count $INSTANCE_COUNT            \
    --auto-terminate                            \
    --log-uri s3://genomics-epiquant-emr-logs   \
    --use-default-roles                         \
    --enable-debugging                          \
    --ec2-attributes SubnetId=$SUBNET_ID        \
    --steps Type=CUSTOM_JAR,Name="Spark Program",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","Main","s3://$BUCKET_NAME/$JAR_FILE","StepwiseModelSelection","--epiq","$GENOTYPE_FILE","-P","$PHENOTYPE_FILE","-o","$OUTPUT_FILE","--aws","--bucket","$BUCKET_NAME","--lasso","$RUN_LASSO"];
then
    echo "--------------------------------------------------------------------"
    echo "Done!"
else
    echo "--------------------------------------------------------------------"
    echo "Failed to create cluster!"
fi
