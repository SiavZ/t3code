# Adapt before adopting

Rank 14 addresses a real self-hosting concern, but the native operating-system control already solves it. T3 should document its state boundary only when users need that deployment guidance.

## 14. A01-OPS-006. Document encrypted deployments

When self-hosted users ask for this threat model, publish one deployment note listing T3 home, userdata, traces, update snapshots, database backups, configured provider homes, and saved client connection data. Recommend FileVault for desktop Macs and an encrypted volume mounted before the Linux user service starts. Include a restore check. Do not ship a mount helper, store encryption passphrases, or duplicate full-disk encryption inside T3. See [A01-OPS-006](../07-operations-and-data-lifecycle.md).
