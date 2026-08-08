# Structural positive fixture: bodyless declaration plus reachable body

This candidate-shaped fixture includes a legitimate bodyless trait method
declaration, `BranchSelector::active_branch_id`, in the scanned ForkTree
source path. The verifier must skip that declaration while still traversing
the concrete `reachable_helper` called from the provider plan. The valid
constructors retain one caller-owned `ForkTreeReadFacade` acquisition each.
