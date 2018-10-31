# File formats

We must accept files from various formats and convert them to an intermediate format that is easily usable by all of the tools.

## Plink

Plink is a toolkit with many different file formats: https://www.cog-genomics.org/plink/2.0/formats 

From looking at Hail's source (https://github.com/hail-is/hail/blob/master/hail/src/main/scala/is/hail/io/plink/LoadPlink.scala), the
  files they focus on are .bim, .fam, and .bed.
  
### .bed (https://www.cog-genomics.org/plink/2.0/formats#bed)

Bed is the primary format, and it must be accompanied by additional .fam and .bim files.

It is a binary format.

### .ped/.map

A plain-text format that represents genotype information (older than .bed/.fam/.bim formats)

#### .ped

* https://www.cog-genomics.org/plink/1.9/formats#ped

Original standard text format for sample pedigree information and genotype calls. Normally must be accompanied by a .map file;

Contains no header line, and one line per sample with 2V+6 fields where V is the number of variants. 
  The first six fields are the same as those in a .fam file. 

```
1   Family ID ('FID')
2   Within-family ID ('IID'; cannot be '0')
3   Within-family ID of father ('0' if father isn't in dataset)
4   Within-family ID of mother ('0' if mother isn't in dataset)
5   Sex code ('1' = male, '2' = female, '0' = unknown)
6   Phenotype value ('1' = control, '2' = case, '-9'/'0'/non-numeric = missing data if case/control)
```

The 7th and 8th fields are allele calls for the first variant in the .map file ('0' = no call);
  the 9th and 10th are allele calls for the second variant; and so on.

If all alleles are single-character, PLINK 1.9 will correctly parse the more compact 'compound genotype' variant of this format,
 where each genotype call is represented as a single two-character string. This does not require the use of an additional 
 loading flag. You can produce such a file with '--recode compound-genotypes'.

#### .map

* https://www.cog-genomics.org/plink/1.9/formats#map

Variant information file accompanying a .ped text pedigree + genotype table.

A text file with no header file, and one line per variant with the following 3-4 fields:

    Chromosome code. PLINK 1.9 also permits contig names here, but most older programs do not.
    Variant identifier
    Position in morgans or centimorgans (optional; also safe to use dummy value of '0')
    Base-pair coordinate

All lines must have the same number of columns (so either no lines contain the morgans/centimorgans column, or all of them do).

# Note on converting genotype data into our format

http://zzz.bwh.harvard.edu/plink/dataman.shtml

The genotypic information is usually stored in pairs representing the base call for each allele, A T, G G, C T, etc..

Plink has a tool to turn this information into 0, 1, and 2, which is how we want the data.

**recodeAD**
```
...
which, assuming C is the minor allele, will recode genotypes as follows:

     SNP       SNP_A ,  SNP_HET
     ---       -----    -----
     A A   ->    0   ,   0
     A C   ->    1   ,   1
     C C   ->    2   ,   0
     0 0   ->   NA   ,  NA

Note: We want to use the equivalent of PLINK's recodeA, because it omits the SNP_HET info. 
Note: When an allele that is both non-major and non-minor is invovled, the corresponding value is NULL.
```


