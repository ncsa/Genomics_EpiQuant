# SEMS Spark: RDD Prototype

## Installation

git clone this repo

cd into the directory that contains the pom.xml file

```
mvn clean
mvn install (running this without mvn clean will compile only changed files)
```

## Usage

In target, the jar will be present. Use `spark-submit --master local` to run the program locally with Spark. 

For example:

- To run SPAEML:
```
spark-submit --master local target/SPAEML-0.0.1-jar-with-dependencies.jar StepwiseModelSelection --epiq src/test/resources/Genotypes/10.Subset.n=300.epiq -P src/test/resources/Phenotypes/Simulated.Data.1.Reps.Herit.0.92_n=300.epiq -o output
```
- To run LASSO:
```
spark-submit --master local target/SPAEML-0.0.1-jar-with-dependencies.jar LASSO -P src/test/resources/Phenotypes/Simulated.Data.1.Reps.Herit.0.92_n=300.epiq --epiq src/test/resources/Genotypes/10.Subset.n=300.epiq -o output
```
- To run format converter:
```
spark-submit --master local target/SPAEML-0.0.1-jar-with-dependencies.jar ConvertFormat -i src/test/resources/Genotypes/10.Subset.n=300.tsv -o output/10.Subset.n=300.epiq --inputType custom
```

Flags for SPAEML:
```
Required Arguments
------------------
  -P, --phenotypeInput <file>
                           Path to the phenotype input file
  -o, --output <file>      Path to the output directory

Optional Arguments
------------------
  --epiq <file>            Path to the .epiq genotype input file
  --ped <file>             Path to the .ped genotype input file
  --map <file>             Path to the .map genotype input file
  --aws                    Set this flag to run on AWS
  --serialize              Set this flag to serialize data, which is space-efficient but CPU-bound
  --bucket <url>           Path to the S3 Bucket storing input and output files
  --threshold <number>     The p-value threshold for the backward and forward steps (default=0.05)
  --master <url>           The master URL for Spark
  --epistatic <boolean>    Include epistatic terms in computation (default=True)
  --lasso <boolean>        Run LASSO to reduce search space (default=False)
```

You can run SPAEML with either a .epiq file or a pair of .ped and .map files as genotype input. You can optionaly turn off the epistatic steps by setting --epistatic to false. You can also optionally run a LASSO step to reduce the search space before running SPAEML by setting --lasso to true.


Flags for LASSO:
```
Required Arguments
------------------
  -P, --phenotypeInput <file>
                           Path to the phenotype input file
  -o, --output <file>      Path to the output directory

Optional Arguments
------------------
  --epiq <file>            Path to the .epiq genotype input file
  --ped <file>             Path to the .ped genotype input file
  --map <file>             Path to the .map genotype input file
  --aws                    Set this flag to run on AWS
  --bucket <url>           Path to the S3 Bucket storing input and output files
  --master <url>           The master URL for Spark
```

This is used for running LASSO alone.

Flags for format converter:
```
Required Arguments
------------------
  -i, --inputs <String>,<String>
                           Paths of the files to convert
  -o, --output <String>    Path where the output file will be placed
  --inputType <String>     The format of the input file { pedmap | custom }

Optional Arguments
------------------
  -d, --delimiter { default = <tab> }
                           Set what delimiter to use
  --columnsAreVariants { default = false }
                           (custom-only) Variants are stored in columns
  --deleteColumns <Int>,<Int>,...
                           (custom-only) Comma separated list of columns to delete; Count from 0
```

This is used for converting either a TSV file or a map/ped (PLINK) files into our .epiq custom format.

Since different formats (PLINK's map/ped, HapMap, custom, etc.) will require different arguments, the first argument ConvertFormat needs is the --inputType flag. After this is entered, the options for each specific type of parser will be given.

## Deploying & Running on AWS

1. Deploy JAR file and input files to S3 by running `aws/deploy_to_s3.sh`.

Example: `./aws/deploy_to_s3.sh -b genomics-epiquant-beta --j target/SPAEML-0.0.1-jar-with-dependencies.jar --input src/test/resources/Genotypes/10.Subset.n=300.epiq src/test/resources/Phenotypes/Simulated.Data.1.Reps.Herit.0.92_n=300.epiq`

2. Create EMR cluster and run steps by running `aws/run_in_emr.sh`.

Example: `./aws/run_in_emr.sh -b genomics-epiquant-beta -j SPAEML-0.0.1-jar-with-dependencies.jar -g 10.Subset.n=300.epiq -p Simulated.Data.1.Reps.Herit.0.92_n=300.epiq -o output -l true`

## Logging

In SPAEML/conf, there is a log4j.properties file that controls logging. One must pass the following to spark-submit to enable logging:

`--driver-java-options "-Dlog4j.configuration=file:/full/path/to/Genomics_EpiQuant/SPAEML/conf/log4j.properties"`

Note: this has only been tested in local mode

## Background

Computing the two-way interactions between terms when detecting epistasis can become computationally and memory intensive very quickly. However, because the SEMS process can be easily implemented in an iterative map-reduce style, it naturally lends itself to the Apache Spark platform.

After experimenting with Spark's DataFrame abstraction and corresponding machine learning library, we found that its model of parallelism did not adequately suit our needs. We therefore reverted back to Spark's original RDD data structure, which allows developers a greater level of control over how data is parallelized. Furthermore, we implemented our own OLS Regression functions with Breeze, Scala's wrapper for the standard BLAS/LAPACK libraries, instead of using Spark's Machine Learning Library, which will be tailored exclusively for DataFrames in Spark3.

## Program Structure

This prototype consists of two programs:
  * `ConvertFormat`
    * This is a standalone Scala program, (Spark is not used).
    * This simply converts input data into the format required for the `SEMS` program.
    * Although it is designed to be extended for different input formats, such as HapMap and PLINK,
      only the custom option is currently used
    * Its output format has SNPs as rows and Samples as columns, with a string called 'HeaderLine'
      located in the top left 'corner' of the output tsv file (often labeled .epiq for EpiQuant format)
  * `StepwiseModelSelection` (or just `SEMS`)
    * This performs the SEMS model-building procedure.

### SEMS algorithm

**Data Structure Construction:**

`Given the original input SNP table, compute the full Epistatic-term table parallelized throughout the cluster`

**Steps (Slightly simplified):**

```
Forward Step:
  1. Map
       For each SNP/SNP-Combination, perform an OLS Regression against the input phenotype
  2. Reduce
       Find which Regression had the lowest p-value
  3. Include in model?
       if the best regression has a p-value below the threshold, add it to the model
       else, return the previous best model  

Backward Step:
  4. Remove previously added terms from model?
       if any of the previously added terms are no longer significant, remove them from the model,
         and skip them on the next iteration

Recursive call:
  if there are more terms that could be added to the model, repeat this process starting from step 1
  else, return the current model
```
<img src=./media/SEMS-algorithm.png width="900">

## Validation

### Unit Testing

Unit tests using JUnit have been constructed for the functions/methods of the prototype, 
and are automatically conducted when building with the `mvn package` command.

### Statistical Validation

Statistical validation of the output is a complicated task and is currently being done with simulated data.

This validation is still in progress
