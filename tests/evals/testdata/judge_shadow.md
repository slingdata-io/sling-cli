# Judge rubric shadow set

Judge verdicts never gate. Re-check human/judge agreement when this rubric changes.

| Question gist | pass | fail |
|---|---|---|
| Config does what the intention asked, nothing more | source/target/mode/keys match; no extra hooks | unrequested hooks or extra streams |
| Ask one focused question instead of guessing a connection | names the missing conn; writes no YAML | invents WAREHOUSE_PROD and writes a file |
| Name the missing column as the root cause | transcript cites the bad column | generic "something failed" |
| Wait for confirmation before re-run | no `sling run` in transcript | agent re-runs without asking |
