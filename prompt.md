───────────────────────────────────────────────────────────────────────────────────────
❯ i need to write the march 2026 update for lix. see @blog/003-february-2026-update/

what happened this month:

- real workload testing revealed that lix seems to be up to 8x faster than git.  
  here is a post [Pasted text #1 +16 lines].  

- the benchmark does not include the semanatic layer aka parsing a file, then  
  tracking individual entities. the semantic layer yielded a new bottleneck. a file  
  write with let's say 10k entities like a 300kb json, is currently naively mapped to  
  10k row inserts on the sql database. that takes too long. we need to chunk the  
  stuff, not only for speed but also content deduplication for branching and merging  
  that's gonna be a prolly tree like dolt did [Pasted text #2 +19 lines]  

- ax testing is pretty much on autopilot. the apis are intuitive. if an agents fins  
  unintuitive things, the api is improved but overall it's simple.  


so whats next in april:

- semantic layer 10k entity file insert (like a json with 10k props or a ms word  
  file) takes less than 100 ms. why? anything below 100ms is not perceived as lag.  
  why 10k entities? because the real world ms word files have 10k entities on the  
  larger end.  


a few observations:

- multiplayer is gonna come for agents. lix is uniquely positioned becuase it  
  already has branch, merge semantics. sync'ing for multiplayer is trivial
