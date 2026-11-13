# Dummy database records

Run from the repository root using a Python environment with the server dependencies installed:

```sh
python -m scripts.seed_dummy_database 'postgresql://postgres:postgres@localhost:5433/doc_api_db'
```

The database must already have the current schema. The default dataset contains
10,000 DONE/FAILED jobs over 30 days, 40 owners, 20 workers, and eight engines with
three versions each. Group sizes, image counts, attempt counts and runtimes vary.
Some jobs have no assigned engine or worker. Four short synthetic logs are provided
per job. No files, runnable jobs or notifications are created; API credentials are
unusable by default and dummy engines are inactive. Existing records remain unchanged.

Options: `--jobs`, `--owners`, `--workers`, `--engines`, `--versions`, `--days`,
`--seed` (default 42), and `--prefix` for a unique label prefix. Each invocation
adds a new dataset in one transaction. Labels default to `dummy-db-<run suffix>`.
All dummy job, key and engine UUIDs start with the reserved prefix `d00df00d-`,
followed by random suffixes. The seed reproduces data distributions, not UUIDs.

To create usable test credentials, add `--usable-keys`. Run with the same
`HMAC_SECRET` environment setting as the API server. Each dummy key's ID and
secret are its label, so its API key is `KEY_PREFIX.name.name`, for example
`doc-api.dummy-db-test-user-01.dummy-db-test-user-01`. `KEY_PREFIX` defaults to
`doc-api`; use the server's configured prefix when authenticating. No credential
file is needed or written. Labels must fit the key ID limit of 32 characters,
and `--prefix` must contain only letters, digits, underscores or hyphens.
These predictable credentials are for local test data only. Existing dummy keys
are not changed by this option.

To remove **all filler runs** from the specified database:

```sh
python -m scripts.seed_dummy_database 'postgresql://postgres:postgres@localhost:5433/doc_api_db' --clear
```

Cleanup deletes records with the reserved UUID prefix, plus their image records
and cascading job notifications, in one transaction. It does not delete files.
Do not use this UUID prefix for ordinary records.
