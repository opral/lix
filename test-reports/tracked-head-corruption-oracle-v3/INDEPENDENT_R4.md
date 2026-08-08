# Independent R4 discriminator binding

R4 exact object:

    head 7ff277c297e93eba83da09bf12f83d6485a8458b
    tree a0e6be2c9029144497b75e4f9dcd6b001d71fec9
    parent 1d9c47728377c6ec7d2646704d51f3aadb11c773
    parent-to-head full-index binary 55d018ea5389898414dbf7844053c5339b316bf36652574b86983c1c8cb43b4b
    stable patch ID 1ad9a7cc93f5386920032d3d1b1cebc8febaa43d

R4's three files are independently preserved at the exact object and cover
20 cases over GlobalSelector, BranchSelector, StateRoot, CatalogRoot, and
CheckpointRoot. Its source is an independent red discriminator reference; this
package's model is the direct corrected v3 and splits CatalogRoot into the
authenticated CommitCatalog and ChangeCatalog domains, for 24 cases.
