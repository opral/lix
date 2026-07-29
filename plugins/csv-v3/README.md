# CSV Plugin API v3

Hard-cut Component v3 port of the CSV table/row schemas. Exact file bytes,
durable rows, and the row locator index live in host-owned immutable arenas.
Equal-length edits read one affected locator window and one durable row; the
full semantic graph is reconstructed only for unsupported fallback edits.
