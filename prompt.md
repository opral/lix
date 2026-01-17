
We want to write the introducing lix blog post.

I created an outline in @blog/001-introducing-lix/

crawl the content of this repo to get context. also crawl the blog posts of samuelstroschein.com

furthermore, i attached an internal blog post that provides more context on the git failure. 

the target audience are developers. 




Building on git was our failure mode
Building lix was scratch based on WASM SQLite is the right decision.
Samuel Stroschein
Oct 10, 2024

This memo is part of a recap we did after an onsite in October 2024.

Two things happened in June 2024:

The focus shifted from inlang to lix.

The decision was made to (finally) break git compatibility. 

1. Focus shift from inlang to lix
The focus from inlang shifted to lix. “Investors did not value growth on inlang’s side as much as progress on lix side” [Opral Company Update June 2024]. Shifting the focus to lix entailed that the previous strategy of “inlang's growth is our GTM" was obsolete. With it, the requirement to stay git compatible. We saw git compatibility as a must-have to reduce adoption friction [Lix SDK requirements - September 2023]. After all, every localized project saves translation files in git [Does a git-based architecture make sense? March 2022]. It was clear that git compatibility would ease adoption. The unforeseen cost of remaining git compatible was unknown at the time. 

2. Breaking git compatibility
Because the strategy of “inlang’s growth is our GTM”, which assumed necessary git compatibility, we delayed building differentiating lix technology like multi-format support and semantic change tracking, which are incompatible with git.

The result was 0 perceived progress for investors, users, or third-party developers. Perception-wise, we only had a lengthy memo on what lix could potentially do last summer. The internal development of lix, which fixed bugs left and right for inlang and research on lix requirements, was invisible to internal and external team members. The signal of the failed fundraise and conversations with third-party developers was clear: Talk is cheap. Go build and prove that you can build change control and show the impact.




After 2 years of lix development, we had zero perceived progress. Lix did not support file formats other than text files, semantic querying of changes was impossible, and building apps with lix was insane. Apps had to store their data as text files in a git repo instead of using a database! 
Breaking git compatibility was necessary to build differentiating technology and, thereby, achieve perceived progress. The insight to break git compatibility led to a series of four memos titled “Accelerate lix by years” in July 2024. The memos can be summarized as follows:

If we find a way to embed lix in git, we control all workflows and don’t have to be git compatible anymore except for the embedding interface.

Don’t even try to have a git-compatible embedding interface. It prevents us from focusing on differentiating technology and is irrelevant to users. 

What differentiates lix can be built in a few weeks with SQLite by not only breaking git compatibility but dropping git as a foundation altogether. 

“No-brainer to build lix on SQLite and say farewell to git.”

The decision to rebuild lix (& inlang) on SQLite was not supported by some team members. Doubt existed (still exists) that git might be faster than building from scratch. A major reason for un-alignment. Un-alignment which irritated uninvolved team members and decreased the morale of everyone. I’ll take the next section to double down on explaining why git was our failure mode.

Git was our failure mode of no perceived progress
If we had continued using git as the foundation for lix, our chance of failing would have been manyfold higher. 

Despite lix targeting everyone but developers, having different requirements (multi-file format vs text-only files, semantic change tracking vs line changes in text files, backend for apps vs a CLI), and a different goal (change control a company vs version source code), we were tying resources in one workaround after another. A few examples:

We had to rebuild databases for the GTM apps because git only allowed storing text files [example issues #1122, #1519, #1772]. It took over 6 months and 3 engineers* to settle on an RxDB + text files in the git approach to get persistence in git [#70, #1585]. *Samuel, Martin, Jürgen

Fulfilling GTM requirements like lazy cloning of git repos took 4 engineers* over a year of discussions and prototyping to hit production [#915, #1500]. *Samuel, Shannon, Jan, Martin

The path that git led us down was undifferentiated, with no perceived progress. It’s undifferentiated to rebuild a text-based database if SQLite exists. It’s undifferentiated to have lazy checkout if apps built with SQLite, Postgres, Firebase, etc. don’t have that issue.




The irresistible “but we are only one workaround away” 
The thought of breaking away from git existed for a long time, first expressed in May 2023 [Should we drop direct support for "legacy" git hosts?]. But, the break away from git never happened because we were trapped in believing that a) we need git and b) “git gives us so much for free”. 

So much for free that a vicious cycle of “we only need to work around this one thing in git” began. A cycle that never ended, and will never end, because “now we found the ultimate workaround”. No, we didn’t find the ultimate workaround(s). Git is unsuited for storing different file formats, building apps on top of them, and semantically tracking changes. Hard facts: 

We worked for over 2 years on lix with 0 perceivable progress

Hiring requires deep git and low-level CS expertise, which churned through 3 engineers 

“But we are only one workaround away now, really!” No, we are not.




Conclusion
We corrected the course from the A to the B path.

The strategy shift from inlang to lix was a necessary step to ease future funding. While shifting the focus from inlang to lix, it became clear that git was not needed for the GTM and was actively harmful toward perceivable progress. We sank years into git workarounds that would have been avoided if would had built lix from scratch leveraging new technology like SQLite WASM and OPFS. 

We took the step to build lix from scratch 8 weeks ago. A massive change causes un-alignment, inter-dependency mess, and frustration. The onsite confirmed that we made the right decision. The perceivable progress over the last 8 weeks for lix was more than the last 24 months combined. 

