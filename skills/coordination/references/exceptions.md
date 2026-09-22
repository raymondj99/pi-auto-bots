# Exceptions, not routine coordinator work

The runtime handles the normal peer loop. Use this guide only for an actual error or authority exception. Keep permissions, input checks and review gates intact.

| Situation | Appropriate response |
| --- | --- |
| Agent put the brief only in `systemPrompt` | Current Pi normalizes it to `task` before validation. If the loaded runtime rejects it, record the mismatch; do not repeatedly launch or reload active workers. A genuinely missing brief still needs an outcome and owned files. |
| Worker tries artifact `supersede` for corrected bytes | Use `publish` with the **same artifactId** and changed file, then submit the returned next-version ref. Supersede/invalidate are privileged consumer-control operations; do not grant permissions to hide the mistake. |
| Worker submits while blocked | In fast mode, `start` using the current revision must succeed before correction/resubmission. No separate `ready` call is needed. Start still checks holds/dependencies. |
| Stale revision | Read the current affected record once, understand the concurrent change, then decide whether the intended action is still valid. Never blindly increment revisions or retry in a loop. |
| Reviewer has not received GO | Before every source has valid frozen evidence, waiting is normal. Do not manually start/bind review. Investigate only an explicit error or concrete evidence of a stall; no status polling. |
| Frozen input or working-copy hash mismatch | Stop consuming the mismatched bytes. Preserve the exact failed ref/evidence and resolve the discrepancy through the responsible owner or coordinator authority. Never rewrite a frozen file. |
| Correction limit exhausted | Escalate the precise remaining issue with a targeted blocked signal. Do not silently raise the bound, add a run or keep issuing rejected requests. |
| A peer crashes or needs another run | Record unfinished state and request any required authorization. The runtime does not automatically restart failed processes. Resume/rebind must fence the old generation and follow user policy. |
| Provider/model/account unavailable | Report the actual error. Do not switch models/providers, use inherited preset defaults, request an unrelated key or delegate via a companion CLI. |
| Protected operation cannot proceed | Inspect the configured operation and task gate. Use the permit workflow only when required. Dispatched or unknown attempts remain charged; never refund or repeat an uncertain side effect blindly. |
| Review report has been submitted | This is the coordinator's independent verification point, not permission to assume success. Process exit does not approve the report. |

Use typed signals for blockers, decisions and explicit escalation; use channels for substantive questions and findings. A required acknowledgement applies only to the named role/current obligation. An ordinary ready message is not an acknowledgement request.

A recovered error is still an observed error. Summaries should distinguish successful application output, autonomous coordination, protocol rejections and manual intervention. Do not expose private transcripts, prompts, credentials or session identities when reporting evidence.
