# Audit 01 inventory

This pass compares production Dux behavior with production T3 Code behavior. `Equivalent` means the user outcome exists even when the architecture or UI differs. `Partial / different` means the core outcome exists but a material Dux behavior does not. `Gap` means no first-party equivalent was found after graph and source inspection.

The [T3-focused shortlist](t3-useful/00-index.md) filters these findings by product fit and recommends what to build, defer, or reshape.

| Area                          | Equivalent | Partial / different |    Gap | Findings                           |
| ----------------------------- | ---------: | ------------------: | -----: | ---------------------------------- |
| Workspaces and sessions       |          3 |                   4 |      5 | A01-WS-001–012                     |
| Provider runtime              |          1 |                   3 |      1 | A01-PROV-001–005                   |
| Terminals and workflows       |          5 |                   2 |      0 | A01-TERM-001–005, A01-FLOW-001–002 |
| Git and source control        |          3 |                   4 |      0 | A01-GIT-001–007                    |
| Automation and peer routing   |          0 |                   0 |      5 | A01-AUTO-001–003, A01-PEER-001–002 |
| Interface and configuration   |          3 |                   2 |      0 | A01-UI-001–004, A01-CFG-001        |
| Operations and data lifecycle |          1 |                   8 |      1 | A01-DATA-001–002, A01-OPS-001–008  |
| **Total**                     |     **16** |              **23** | **12** | **51**                             |

The status is about Dux feature parity, not overall product quality. Several T3 Code implementations are broader than Dux—for example, remote clients, richer PR creation, provider-native protocols, and structured trace export—but those advantages do not erase a distinct Dux behavior when the workflows differ.
