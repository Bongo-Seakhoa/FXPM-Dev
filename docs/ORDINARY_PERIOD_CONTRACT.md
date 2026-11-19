# Fresh ordinary period contracts

This is an isolated source candidate. It has not been run or landed.

Use the existing historical R&D command with `--fresh-contract` to compile a new
ordinary input before dispatch:

```text
python pm_main.py declared-rnd --contract input.json --report report.json --fresh-contract
```

The supplied JSON stays unchanged. The launcher saves an immutable
`report.effective-contract.json` and records its path, the supplied input's
canonical SHA and the effective contract SHA in the report. A resumed invocation
loads that saved effective contract. It does not compile a new selection, reload
the deployment configuration for compilation, or renew a source window.

Omitted selection means the first accepted period for the adopted ten symbols:
EURUSD, GBPAUD, USDJPY, USDZAR, USDNOK, XAUUSD, XAGUSD, BTCUSD, US500 and APPLE.
An explicit `period_selection` with `mode: "handover"` selects the first two.
All thirteen require `final_study_13: true`; this remains mandatory for every
other symbol. An empty or crossed explicit ID list is refused rather than
treated as an omitted selection. Development contracts retain their existing
classification and selection behavior.

Ordinary fresh compilation is the single operator entry point for new one- and
two-period work. After the ordinary route is validated and landed, the next
hygiene change retires redundant fresh DEV one/two constructors and scheduler
issuance. Saved DEV one-to-three-period readers and history must remain. K3
software fixtures alone do not grant a new operator run. This candidate does not
translate or reissue saved DEV contracts, remove their readers, or authorize
additional runtime.

Compilation uses the existing config resolver and accepted roster derivation.
It writes the actual selected slot IDs and the existing full-roster digest into
the effective row before contract and workspace hashes are calculated. Issuers
use the same selection normalizer and the existing precommit digest; no extra
selection digest is introduced. A source window is issued only by the existing
worker flow, under its existing authority and limits.

Ordinary one- and two-period accounts describe the selected span and cycle count.
Their completion does not claim completion of a thirteen-period study. Native
prefix fences have their own ordinary selected-period edition. Ordinary M5
fences retain the source summary, lease-release proofs, view and consumer
bindings, and data-admission contamination flags. Neither route becomes a
development route merely to obtain partial-period support. Qualification,
capital, live and order authority remain false.

M5 ordinary inputs use neutral `m5_source` metadata with `selected_slot_ids`.
Its supplied view plan and source pins must already match the selected periods.
Compilation does not build or truncate views, change source pins, manufacture an
admission record, or change a cost profile.

Existing saved contracts and reports without fresh-compilation metadata retain
their original hashes. Ordinary saved precommits without a period-selection
field still mean thirteen periods. Source identity remains independently bound:
old source-bound receipts require their captured implementation or an already
authorised compatibility mechanism. This candidate creates no renewal.

The seven Native semantic-verifier sources are outside this change. The separate
M5 constructor roster includes Forward and Spool. The current 74-file candidate
already changes Forward from canonical base `a005819c` to `6df3a02d`, and Spool
from `02003e01` to `7a2bd339`; this next phase changes them again. Pins created
against a different implementation cannot be silently relabelled. No stored
market pin was inspected, and no M5 data rebuild, profile change, financial run
or source-window permission is claimed.
