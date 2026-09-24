# CSV description-amendment fixture

Built from the CSV plugin crate at Lix revision `83b47246d` (September 18,
2026). The compiled guest and packaged schemas
match. Compared with `../v2/plugin_csv.lixplugin`, the CSV row schema adds
column descriptions without changing its structure.

SHA-256: `448218f2fc8bed4f7c8f672c2f269ce09ac6ce017d6cf6d97e0c0bd9a162fe9c`

Keep this archive frozen. Tests upgrade the older V2 fixture to this genuine
build; changing only archive JSON would leave the guest's compiled schema
fingerprint inconsistent with its manifest schema.
