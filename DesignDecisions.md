Mar 7th 2018,

Could not use floats, need to stick with doubles
After switching to floats, junit tests broke (not enough percision)

March 12th, 2018,

Changed the OLSRegression class to an abstract class with two children, one for denseMatrices and one for Sparse.

Note, that this only (potentially) save space for each regression step (which only contains as many columns as there are terms in each tested model). This will help if the data is sparce, but the giant table is still distributed across the Spark cluster in dense form.

To really impact the memory footprint, we would need to change the dense vectors we have in the (Key, Value) => (SNP_name, [values]) RDD to sparce vectors (assuming the data is sparse)

March 14th, 2018,

Jacob has defined abstract classes that implement the Statistics and SEAMS, with subclasses that perform the Sparse or Dense Vector/Matrix.

Note that the phenotype vector is still represented as a DenseVector at this point, as the phenotype data is ususally continuous with few zeros.

March 15th, 2018,

The functions for the CSCMatrix (SparseMatrices) are really lacking. They can multiply and transpose matrices, but cannot find the inverse. Jacob is beginning to think that we should just have Sparse Vectors in the RDDs storing the distrbuted SNP values, and then, when the time comes to take that information and do the stats, we remain using DenseMatrices.

If we store the RDD data as Sparse Vectors, but turn them into DenseVectors/Matrices when they are actually used, it won't save on the memory footprint (Because we run the regression building step for EACH Sparse Vector in the RDD.)
