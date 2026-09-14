---
type: minor
---
Support `OLD.column`, `NEW.column`, `OLD.*`, and `NEW.*` in SQL RETURNING for inserts, updates, deletes, and upserts. Returned row images describe each statement independently of the committed operation's endpoint diff, with typed NULLs for absent images and selective file content reads.
