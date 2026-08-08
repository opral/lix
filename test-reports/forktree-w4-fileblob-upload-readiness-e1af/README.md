# W4 e1af readiness package

TEST/REPORT-only package bound to exact
`e1af471b9ab0f598dafa7c2ddec7867667c81740`.

The source gate is intentionally RED because file-content and multipart
publication still retain legacy CAS/upload authorities. Run only the static
calibration:

```bash
bash test-reports/forktree-w4-fileblob-upload-readiness-e1af/source_gate_red.sh \
  "$PWD" e1af471b9ab0f598dafa7c2ddec7867667c81740
```
