# SEMS Spark: RDD Prototype

## Background

Computing the two-way interactions between terms when detecting epistasis can become computationally and memory intensive very quickly. However, because the SEMS process can be easily implemented in an iterative map-reduce style, it naturally lends itself to the Apache Spark platform.

After experimenting with Spark's DataFrame abstraction and corresponding machine learning library, we found that its model of parallelism did not adequately suit our needs. We therefore reverted back to Spark's original RDD data structure, which allows developers a greater level of control over how data is parallelized. Furthermore, we implemented our own OLS Regression functions using the Breeze Linear Algebra Library, Scala's wrapper for the standard BLAS/LAPACK libraries.

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

Data Structure Construction
* Given the original input SNP table, compute the full Epistatic-term table parallelized throughout the cluster

**Steps (Slightly simplified):**
```
Forward Step:
  1. Map
       For each SNP/SNP-Combination, perform an OLS Regression against the input phenotype
  2. Reduce
       Find which Regression had the lowest p-value
  3. Add to model?
       if the best regression has a p-value below the threshold, add it to the model
       else, return the previous best model  

Backward Step:
  4. Check previously added terms
       if any of the previously added terms are no longer significant, remove them from the model,
         and skip them on the next iteration

Recursive call:
  if there are more terms that could be added to the model, repeat this process starting from step 1
  else, return the current model
```

## Installation

### Git clone

`git clone https://github.com/ncsa/NCSA-Genomics_EpiQuant_SEMS_Spark`

### Build 

This package is built with Maven

From the directory that contains the pom.xml file, execute the following command:

`mvn package`

The jar with dependencies included will be created within the newly created `target` directory

## Usage

### 

## Validation

### Unit Testing

Unit tests using JUnit have been constructed for the functions/methods of the prototype, 
and are automatically conducted when building with the `mvn package` command.

### Statistical Validation

Statistical validation of the output is a complicated task and is currently being done with simulated data.

This validation is still in progress
