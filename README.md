# Airflow_Migration_Poc
## Overview
This repository uses centralized GitHub workflows to:
- Secure the `prod` and `test` branches
- Run automated security scans on PRs
- Trigger Harness deployment pipelines with environment-specific parameters

## Branch Behavior
| Branch | Reviews Required | Scans | Harness Trigger | Notes |
|--------|-----------------|-------|----------------|-------|
| dev    | No              | No    | Yes (DEV env)  | Direct push allowed |
| test   | Optional        | Yes   | Yes (TEST env) | PR merges trigger scan + deploy |
| prod   | Yes (CODEOWNERS)| Yes   | Yes (PROD env) | Strict approval rules enforced |

## Team Responsibilities
1. **Update `.github/CODEOWNERS`**  
   Example:

prod/ @YourSeniorDev1 @YourSeniorDev2

2. **Add Required Secrets**
- Go to: `Settings > Secrets and variables > Actions > New repository secret`
- Add:
  - `HARNESS_WEBHOOK_PROD` — URL from Harness prod trigger
  - `HARNESS_WEBHOOK_TEST` — URL from Harness test trigger
  - `HARNESS_WEBHOOK_DEV` — URL from Harness dev trigger
  - `VAULT_PATH_PROD` — Vault path for prod
  - `VAULT_PATH_TEST` — Vault path for test
  - `VAULT_PATH_DEV` — Vault path for dev
  - `EDB_ID` — EDB identifier

3. **Normal Workflow**
- Push direct to `dev` → Harness deploys to dev immediately
- Create PR from `feature` → `test` or `prod`
- `test` → auto-scan & deploy after merge
- `prod` → scan + **mandatory CODEOWNER approval** before merge

## Parameter Flow to Harness
- Secrets in GitHub are passed in webhook POST requests
- Harness reads them as:
- `<+trigger.payload.vaultPath>`
- `<+trigger.payload.edbId>`

## Benefits
- Zero manual GitHub UI setup per repo
- All enforcement & deployment rules as code
- Teams only manage CODEOWNERS + secrets
