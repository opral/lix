---
type: patch
scope: engine, js-sdk
---

Added a typed file API for storing, reading, querying, and versioning file bytes in Lix:

```js
await lix.fs.writeFile("/orders.xlsx", bytes);
const bytes = await lix.fs.readFile("/orders.xlsx");
await lix.fs.mkdir("/exports/");
await lix.fs.rm("/orders.xlsx");
```

```sql
SELECT data FROM lix_file WHERE path = '/orders.xlsx';
SELECT data FROM lix_file_history WHERE path = '/orders.xlsx';
```
