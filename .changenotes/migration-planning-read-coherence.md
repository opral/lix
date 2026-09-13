---
type: patch
---

Fixed migration planning and snapshot verification failing when OPFS reads expire during heartbeat commits. Candidate scans now resume bounded read units under an unchanged bank revision and epoch fence; publication keeps its existing revision preconditions. This preserves stored rows and pending edits without restarting completed migration writes.

Explicit browser replica conversion now shares the same admission queue as repository opening. Concurrent tabs coordinate conversion, disconnected callers release ownership, and repeated conversion validates the published replica instead of reopening a competing engine. Existing sessions and retained recovery sources are preserved.

Opening an additional session outside an admitted partial replica scope now returns the scope error before attempting to read unprepared branch metadata.
