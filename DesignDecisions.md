Mar 7th 2018,

Could not use floats, need to stick with doubles
After switching to floats, junit tests broke (not enough percision)

March 12th, 2018,

Changed the OLSRegression class to an abstract class with two children, one for denseMatrices and one for Sparse.

Note, that this only (potentially) save space for each regression step (which only contains as many columns as there are terms in each tested model). This will help if the data is sparce, but the giant table is still distributed across the Spark cluster in dense form.

To really impact the memory footprint, we would need to change the dense vectors we have in the (Key, Value) => (SNP_name, [values]) RDD to sparce vectors (assuming the data is sparse)
