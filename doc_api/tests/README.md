# Running tests

Install the service and test dependencies in your Python environment:

```sh
python -m pip install -e '.[server,dev]'
export TEST_DATABASE_URL='postgresql+asyncpg://test_user:password@localhost:5432/postgres'
python -m pytest doc_api/tests
```

`TEST_DATABASE_URL` points to an existing PostgreSQL maintenance database. Its user
must be able to connect and have `CREATEDB` permission. Each database test creates
a uniquely named `docapi_test_<uuid>` database owned by that user; only that new
database is migrated and dropped. The maintenance database's tables are untouched.
Use a dedicated test account/server where practical. An API server and deployment
API keys are unnecessary; the old `--base-url` and `--api-key-*` options are removed.
Without `TEST_DATABASE_URL`, database tests are skipped; pure unit tests still run.
Connection or permission errors fail setup rather than silently skipping coverage.

Each API test applies the Alembic migrations, runs FastAPI startup/shutdown, and
uses HTTPX's in-memory ASGI transport. Startup creates test-only API keys, and
requests exercise the real authentication dependencies. Upload directories use
pytest's per-test temporary paths. PostgreSQL connections are disposed and the
created database is dropped on teardown, including after failed assertions or
setup errors. Config and dependency overrides are restored. Independent pytest
processes (including pytest-xdist workers) use distinct databases and directories.
A forcibly killed test process cannot run teardown and may leave its temporary
database behind. Pytest retains some temporary file directories for debugging
according to its normal `tmp_path` retention policy.

In PyCharm, select the project interpreter with the dependencies above, use a
pytest run configuration targeting `doc_api/tests`, set the working directory to
the repository root, and add `TEST_DATABASE_URL` to its environment variables.
No API URL or API key arguments are needed.

`test_migrations.py` additionally builds an older schema through Alembic, inserts
legacy jobs and image rows, upgrades to head, and verifies backfilled counts and
data preservation, then exercises downgrade and re-upgrade.

Keep endpoint tests grouped by route in `test_user.py`, `test_worker.py`, and
`test_admin.py`, with authentication checks in `test_authentication.py` and
cross-role workflows in `test_scenario.py`. All these tests share the isolated
`client` and role-header fixtures in `conftest.py`; test bodies need no database
setup of their own. Cleanup, migrations, adapter units, schema validation, and
fixture teardown checks have separate modules. Application lifespan tests live in
`test_startup.py`, and server launcher tests in `test_run.py`. Shared fixtures live in
`conftest.py` and ordinary helper functions in `helpers.py`.

Use `client` with explicit role headers for every API request, for example
`await client.patch(url, headers=worker_headers, json=payload)`. No fixture
implicitly adds authentication headers. For database setup and assertions,
request `db_sessions` and use `async with db_sessions() as db:`. The `job_factory`
and `output_factory` fixtures prepare jobs in specific states for focused tests;
they use the same database as `client`. Database-only cleanup tests use these
fixtures directly without a separate API wrapper. Startup tests use the shared
`prepared_database` fixture to migrate and bind the same temporary database before
entering the lifespan themselves; this avoids an implicit development startup
before a production or startup-failure test. Launcher tests mock Uvicorn and DB
setup calls, so they never start a server or prepare a deployment database.

Name route tests `test_<method>_<resource>_<http_status>_<case>` (for example,
`test_patch_job_409_update_failed_job`), matching the existing endpoint sections.
Keep distinct HTTP outcomes in separate tests and use descriptive parameter IDs
for variations of a case. Unit tests and cleanup tests use behavior-based names.

User and worker suites stay separate. Shared behavior within each suite uses
`@pytest.mark.parametrize("user_role", ["user", "admin"])` or
`@pytest.mark.parametrize("worker_role", ["worker", "admin"])` to run the same
case with both credentials. The corresponding header fixture and workflow
fixtures use that role; unrelated actors retain their normal role. Each run has
its own temporary database and upload directories. Without parametrization,
`user_role` defaults to `user` and `worker_role` to `worker`.

Role-specific ownership/state denials, engine restrictions, and worker-only
update validation retain their own cases: admins intentionally bypass some of
these restrictions. Where a shared successful response exposes more data to an
admin, the test explicitly checks the expected fields for each role. This keeps
admin coverage alongside the operation it exercises without duplicating tests.

Read endpoints use `reader_role` with `user`, `readonly`, and `admin`, and explicit
`reader_headers`. Setup uploads still use the normal user/worker credentials.
For list tests, `reader_owner` identifies the user or readonly key whose jobs are
being queried; jobs created through the API are assigned to it in the test DB,
and `job_factory` defaults to it. This represents existing jobs retained by a key
that is now readonly, without allowing readonly uploads. Admin read tests keep
user-owned jobs. Listing checks cover own jobs, other owners, an empty own list,
filters, result availability, and role-specific fields. Detail and result tests
also exercise readonly access to jobs owned by another key.

## Measuring code coverage

From the repository root, after setting `TEST_DATABASE_URL`:

```sh
python -m pytest doc_api/tests --cov=doc_api.api --cov-config=.coveragerc --cov-report=term-missing
```

The dev dependencies include `pytest-cov`. The shared `.coveragerc` enables branch
coverage and greenlet support, which is needed to measure code reached through
async SQLAlchemy calls correctly. Coverage percentages complement the endpoint
checklist; they do not establish that every input combination is covered.

## Coverage and backlog

This is the maintained version of [issue #19's testing backlog](https://github.com/DCGM/DocAPI/issues/19),
updated against the current endpoints, test functions, and response assertions.
Checked items have implemented tests. Unchecked items describe remaining
coverage work. The list tracks test functions, including parameterized cases,
rather than a fixed test count. It is not a claim that all combinations of inputs and configuration have
been exhausted.

`test_readme_references_current_tests` checks that every current test function is
listed and that no reference points to a removed function. Update this inventory
in the same change as adding, renaming, moving, or removing tests.
`test_registered_routes_have_test_cases` checks the explicit route inventory in
`helpers.py` against all registered application routes, including root and debug
routes. Every protected route has missing/malformed/unknown/wrong-secret/inactive
key checks, plus checks for each disallowed role. These authentication matrices
are listed once below instead of repeated under each endpoint.

### Cross-role scenarios

- [x] upload job files. [`test_scenario.py::test_upload_job_files`](test_scenario.py).
- [x] download job files; `JOB_RETRIEVED`. [`test_scenario.py::test_download_job_files`](test_scenario.py).
- [x] user to readonly job access; `KEY_CREATED`, `JOB_CREATED`, `JOB_LEASED`, `JOB_RESULT_UPLOADED`, `JOB_COMPLETED`, `JOB_RETRIEVED`, `KEY_UPDATED`, `API_KEY_ROLE_FORBIDDEN`. [`test_scenario.py::test_user_to_readonly_job_access`](test_scenario.py).
- [x] job failed after max attempts; `JOB_RETRIEVED`. [`test_scenario.py::test_job_failed_after_max_attempts`](test_scenario.py).

### Authentication and route inventory

- [x] **200** — readonly; `API_KEY_VALID`. [`test_authentication.py::test_get_me_200_readonly`](test_authentication.py).
- [x] **200** — user; `API_KEY_VALID`. [`test_authentication.py::test_get_me_200_user`](test_authentication.py).
- [x] **200** — worker; `API_KEY_VALID`. [`test_authentication.py::test_get_me_200_worker`](test_authentication.py).
- [x] **200** — admin; `API_KEY_VALID`. [`test_authentication.py::test_get_me_200_admin`](test_authentication.py).
- [x] **401** — missing; `API_KEY_MISSING`. [`test_authentication.py::test_get_me_401_missing`](test_authentication.py).
- [x] **401** — invalid; `API_KEY_INVALID`. [`test_authentication.py::test_get_me_401_invalid`](test_authentication.py).
- [x] **403** — inactive; `API_KEY_INACTIVE`. [`test_authentication.py::test_get_me_403_inactive`](test_authentication.py).
- [x] **403** — readonly; `API_KEY_ROLE_FORBIDDEN`. [`test_authentication.py::test_get_admin_keys_403_readonly`](test_authentication.py).
- [x] **403** — user; `API_KEY_ROLE_FORBIDDEN`. [`test_authentication.py::test_get_admin_keys_403_user`](test_authentication.py).
- [x] **403** — worker; `API_KEY_ROLE_FORBIDDEN`. [`test_authentication.py::test_get_admin_keys_403_worker`](test_authentication.py).
- [x] **401** — authentication. [`test_authentication.py::test_route_401_authentication`](test_authentication.py).
- [x] **403** — inactive key; `API_KEY_INACTIVE`. [`test_authentication.py::test_route_403_inactive_key`](test_authentication.py).
- [x] **403** — role forbidden; `API_KEY_ROLE_FORBIDDEN`. [`test_authentication.py::test_route_403_role_forbidden`](test_authentication.py).
- [x] **200** — alternative credentials; `API_KEY_VALID`. [`test_authentication.py::test_get_me_200_alternative_credentials`](test_authentication.py).
- [x] **401** — header takes precedence; `API_KEY_INVALID`. [`test_authentication.py::test_get_me_401_header_takes_precedence`](test_authentication.py).
- [x] registered routes have test cases. [`test_authentication.py::test_registered_routes_have_test_cases`](test_authentication.py).

### User and general routes

#### POST /v1/jobs - 201, 403, 404, 409, 422

- [x] **201** — default case. [`test_user.py::test_post_job_201`](test_user.py).
- [x] **201** — with engine. [`test_user.py::test_post_job_201_with_engine`](test_user.py).
- [x] **422** — invalid key values; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_post_job_422_invalid_key_values`](test_user.py).
- [x] **422** — extra keys; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_post_job_422_extra_keys`](test_user.py).
- [x] **422** — duplicate image names; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_post_job_422_duplicate_image_names`](test_user.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_post_job_422_invalid_request`](test_user.py).
- [x] **404** — engine not found; `ENGINE_NOT_FOUND`. [`test_user.py::test_post_job_404_engine_not_found`](test_user.py).
- [x] **409** — engine inactive; `ENGINE_INACTIVE`. [`test_user.py::test_post_job_409_engine_inactive`](test_user.py).
- [x] **403** — explicit engine version; `API_KEY_FORBIDDEN_FOR_ENGINE_VERSION`. [`test_user.py::test_post_job_403_explicit_engine_version`](test_user.py).
- [x] **422** — extension policy; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_post_job_422_extension_policy`](test_user.py).

#### GET /v1/jobs - 200, 422

- [x] **200** — default case; `JOBS_RETRIEVED`. [`test_user.py::test_get_jobs_200`](test_user.py).
- [x] **200** — with engines; `JOBS_RETRIEVED`. [`test_user.py::test_get_jobs_200_with_engines`](test_user.py).
- [x] **200** — result availability. [`test_user.py::test_get_jobs_200_result_availability`](test_user.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_get_jobs_422_invalid_request`](test_user.py).
- [x] **200** — date filter boundaries, UTC `Z`, positive/negative timezone offsets, and naive UTC. [`test_user.py::test_get_jobs_200_date_filter_boundaries`](test_user.py).
- [x] **200** — NULL dates with and without a filter. [`test_user.py::test_get_jobs_200_date_filter_null_dates`](test_user.py).
- [x] **200** — all date filters combined with state, engine name/version, and result availability. [`test_user.py::test_get_jobs_200_combined_date_filters`](test_user.py).
- [x] **200** — descending creation-date order with and without each date filter. [`test_user.py::test_get_jobs_200_date_filter_ordering`](test_user.py).
- [x] **200** — state filter. [`test_user.py::test_get_jobs_200_state_filter`](test_user.py).
- [x] **200** — engine filter combination. [`test_user.py::test_get_jobs_200_engine_filter_combination`](test_user.py).

#### GET /v1/jobs/{job_id} - 200, 403, 404, 422, 500

- [x] **200** — default case; `JOB_RETRIEVED`. [`test_user.py::test_get_job_200`](test_user.py).
- [x] **200** — with engine; `JOB_RETRIEVED`. [`test_user.py::test_get_job_200_with_engine`](test_user.py).
- [x] **200** — deletion visibility. [`test_user.py::test_get_job_200_deletion_visibility`](test_user.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_user.py::test_get_job_404_job_not_found`](test_user.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_user.py::test_get_job_403_job_access_forbidden`](test_user.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_get_job_422_invalid_request`](test_user.py).
- [x] **500** — database failure; `INTERNAL_ERROR`. [`test_user.py::test_get_job_500_database_failure`](test_user.py).

#### PUT /v1/jobs/{job_id}/images/{image_id}/files/image - 201, 200, 403, 404, 409, 415, 422, 400, 500

- [x] **201** — default case; `IMAGE_UPLOADED`. [`test_user.py::test_put_image_201`](test_user.py).
- [x] **201** — preserves deletion timestamps. [`test_user.py::test_put_image_201_preserves_deletion_timestamps`](test_user.py).
- [x] **200** — default case; `IMAGE_REUPLOADED`. [`test_user.py::test_put_image_200`](test_user.py).
- [x] **404** — default case; `IMAGE_NOT_FOUND_FOR_JOB`. [`test_user.py::test_put_image_404`](test_user.py).
- [x] **409** — default case; `JOB_NOT_IN_NEW`. [`test_user.py::test_put_image_409`](test_user.py).
- [x] **415** — default case; `IMAGE_INVALID`. [`test_user.py::test_put_image_415`](test_user.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_user.py::test_put_image_404_job_not_found`](test_user.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_user.py::test_put_image_403_job_access_forbidden`](test_user.py).
- [x] **409** — job not new; `JOB_NOT_IN_NEW`. [`test_user.py::test_put_image_409_job_not_new`](test_user.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_put_image_422_invalid_request`](test_user.py).
- [x] **500** — write failure. [`test_user.py::test_put_image_500_write_failure`](test_user.py).
- [x] **500** — database failure. [`test_user.py::test_put_image_500_database_failure`](test_user.py).
- [x] **400** — interrupted upload. [`test_user.py::test_put_image_400_interrupted_upload`](test_user.py).
- [x] **500** — failed input replacement preserves prior bytes and upload flags, removes staging files, and permits retry. [`test_user.py::test_put_image_500_reupload_preserves_file`](test_user.py).
- [x] **500** — stored upload read fails before destination writes; retry succeeds. [`test_user.py::test_put_image_500_read_failure`](test_user.py).
- [x] put image 500 commit failure. [`test_user.py::test_put_image_500_commit_failure`](test_user.py).
- [x] put image 201 concurrent inputs queue job. [`test_user.py::test_put_image_201_concurrent_inputs_queue_job`](test_user.py).

#### PUT /v1/jobs/{job_id}/images/{image_id}/files/alto - 201, 200, 400, 403, 404, 409, 422, 500

- [x] **201** — default case; `ALTO_UPLOADED`. [`test_user.py::test_put_alto_201`](test_user.py).
- [x] **201** — preserves deletion timestamps. [`test_user.py::test_put_alto_201_preserves_deletion_timestamps`](test_user.py).
- [x] **200** — default case; `ALTO_REUPLOADED`. [`test_user.py::test_put_alto_200`](test_user.py).
- [x] **400** — default case; `XML_PARSE_ERROR`. [`test_user.py::test_put_alto_400`](test_user.py).
- [x] **404** — default case; `IMAGE_NOT_FOUND_FOR_JOB`. [`test_user.py::test_put_alto_404`](test_user.py).
- [x] **409** — alto not required; `ALTO_NOT_REQUIRED`. [`test_user.py::test_put_alto_409_alto_not_required`](test_user.py).
- [x] **409** — job not in new; `JOB_NOT_IN_NEW`. [`test_user.py::test_put_alto_409_job_not_in_new`](test_user.py).
- [x] **422** — default case; `ALTO_SCHEMA_INVALID`. [`test_user.py::test_put_alto_422`](test_user.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_user.py::test_put_alto_404_job_not_found`](test_user.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_user.py::test_put_alto_403_job_access_forbidden`](test_user.py).
- [x] **409** — job not new; `JOB_NOT_IN_NEW`. [`test_user.py::test_put_alto_409_job_not_new`](test_user.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_put_alto_422_invalid_request`](test_user.py).
- [x] **422** — optional validation; `ALTO_SCHEMA_INVALID`. [`test_user.py::test_put_alto_422_optional_validation`](test_user.py).
- [x] **201** — root validation disabled. [`test_user.py::test_put_alto_201_root_validation_disabled`](test_user.py).
- [x] **500** — write failure. [`test_user.py::test_put_alto_500_write_failure`](test_user.py).
- [x] **500** — database failure. [`test_user.py::test_put_alto_500_database_failure`](test_user.py).
- [x] **400** — interrupted upload. [`test_user.py::test_put_alto_400_interrupted_upload`](test_user.py).
- [x] **500** — failed input replacement preserves prior bytes and upload flags, removes staging files, and permits retry. [`test_user.py::test_put_alto_500_reupload_preserves_file`](test_user.py).
- [x] **500** — stored upload read fails before destination writes; retry succeeds. [`test_user.py::test_put_alto_500_read_failure`](test_user.py).
- [x] put alto 500 commit failure. [`test_user.py::test_put_alto_500_commit_failure`](test_user.py).

#### PUT /v1/jobs/{job_id}/images/{image_id}/files/page - 201, 200, 400, 403, 404, 409, 422, 500

- [x] **201** — default case; `PAGE_UPLOADED`. [`test_user.py::test_put_page_201`](test_user.py).
- [x] **201** — preserves deletion timestamps. [`test_user.py::test_put_page_201_preserves_deletion_timestamps`](test_user.py).
- [x] **200** — default case; `PAGE_REUPLOADED`. [`test_user.py::test_put_page_200`](test_user.py).
- [x] **400** — default case; `XML_PARSE_ERROR`. [`test_user.py::test_put_page_400`](test_user.py).
- [x] **404** — default case; `IMAGE_NOT_FOUND_FOR_JOB`. [`test_user.py::test_put_page_404`](test_user.py).
- [x] **409** — page not required; `PAGE_NOT_REQUIRED`. [`test_user.py::test_put_page_409_page_not_required`](test_user.py).
- [x] **409** — job not in new; `JOB_NOT_IN_NEW`. [`test_user.py::test_put_page_409_job_not_in_new`](test_user.py).
- [x] **422** — default case; `PAGE_SCHEMA_INVALID`. [`test_user.py::test_put_page_422`](test_user.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_user.py::test_put_page_404_job_not_found`](test_user.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_user.py::test_put_page_403_job_access_forbidden`](test_user.py).
- [x] **409** — job not new; `JOB_NOT_IN_NEW`. [`test_user.py::test_put_page_409_job_not_new`](test_user.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_put_page_422_invalid_request`](test_user.py).
- [x] **422** — optional validation; `PAGE_SCHEMA_INVALID`. [`test_user.py::test_put_page_422_optional_validation`](test_user.py).
- [x] **201** — root validation disabled. [`test_user.py::test_put_page_201_root_validation_disabled`](test_user.py).
- [x] **500** — write failure. [`test_user.py::test_put_page_500_write_failure`](test_user.py).
- [x] **500** — database failure. [`test_user.py::test_put_page_500_database_failure`](test_user.py).
- [x] **400** — interrupted upload. [`test_user.py::test_put_page_400_interrupted_upload`](test_user.py).
- [x] **500** — failed input replacement preserves prior bytes and upload flags, removes staging files, and permits retry. [`test_user.py::test_put_page_500_reupload_preserves_file`](test_user.py).
- [x] **500** — stored upload read fails before destination writes; retry succeeds. [`test_user.py::test_put_page_500_read_failure`](test_user.py).
- [x] put page 500 commit failure. [`test_user.py::test_put_page_500_commit_failure`](test_user.py).

#### PUT /v1/jobs/{job_id}/files/metadata - 201, 200, 403, 404, 409, 422, 400, 500

- [x] **201** — default case; `META_JSON_UPLOADED`. [`test_user.py::test_put_metadata_201`](test_user.py).
- [x] **201** — preserves deletion timestamps. [`test_user.py::test_put_metadata_201_preserves_deletion_timestamps`](test_user.py).
- [x] **200** — default case; `META_JSON_REUPLOADED`. [`test_user.py::test_put_metadata_200`](test_user.py).
- [x] **409** — meta json not required; `META_JSON_NOT_REQUIRED`. [`test_user.py::test_put_metadata_409_meta_json_not_required`](test_user.py).
- [x] **409** — job not in new; `JOB_NOT_IN_NEW`. [`test_user.py::test_put_metadata_409_job_not_in_new`](test_user.py).
- [x] **422** — default case; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_put_metadata_422`](test_user.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_user.py::test_put_metadata_404_job_not_found`](test_user.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_user.py::test_put_metadata_403_job_access_forbidden`](test_user.py).
- [x] **409** — job not new; `JOB_NOT_IN_NEW`. [`test_user.py::test_put_metadata_409_job_not_new`](test_user.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_put_metadata_422_invalid_request`](test_user.py).
- [x] **500** — write failure. [`test_user.py::test_put_metadata_500_write_failure`](test_user.py).
- [x] **500** — database failure. [`test_user.py::test_put_metadata_500_database_failure`](test_user.py).
- [x] **400** — interrupted upload. [`test_user.py::test_put_metadata_400_interrupted_upload`](test_user.py).
- [x] **500** — failed input replacement preserves prior bytes and upload flags, removes staging files, and permits retry. [`test_user.py::test_put_metadata_500_reupload_preserves_file`](test_user.py).
- [x] put metadata 500 commit failure. [`test_user.py::test_put_metadata_500_commit_failure`](test_user.py).

#### PATCH /v1/jobs/{job_id} - 200, 403, 404, 409, 422

- [x] **200** — cancel new job. [`test_user.py::test_patch_job_200_cancel_new_job`](test_user.py).
- [x] **200** — cancel queued job; `JOB_CANCELLED`. [`test_user.py::test_patch_job_200_cancel_queued_job`](test_user.py).
- [x] **200** — cancel processing job; `JOB_CANCELLED`. [`test_user.py::test_patch_job_200_cancel_processing_job`](test_user.py).
- [x] **200** — cancel error job; `JOB_CANCELLED`. [`test_user.py::test_patch_job_200_cancel_error_job`](test_user.py).
- [x] **409** — cancel cancelled job; `JOB_UNCANCELLABLE`. [`test_user.py::test_patch_job_409_cancel_cancelled_job`](test_user.py).
- [x] **409** — cancel done job; `JOB_UNCANCELLABLE`. [`test_user.py::test_patch_job_409_cancel_done_job`](test_user.py).
- [x] **409** — cancel failed job; `JOB_UNCANCELLABLE`. [`test_user.py::test_patch_job_409_cancel_failed_job`](test_user.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_user.py::test_patch_job_404_job_not_found`](test_user.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_user.py::test_patch_job_403_job_access_forbidden`](test_user.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_patch_job_422_invalid_request`](test_user.py).
- [x] **403** — user may only cancel; `API_KEY_USER_FORBIDDEN`. [`test_user.py::test_patch_job_403_user_may_only_cancel`](test_user.py).

#### GET /v1/jobs/{job_id}/result - 200, 403, 404, 409, 410, 422, 425

- [x] **200** — default case. [`test_user.py::test_get_job_result_200`](test_user.py).
- [x] **409** — job cancelled; `JOB_CANCELLED`. [`test_user.py::test_get_job_result_409_job_cancelled`](test_user.py).
- [x] **409** — job failed; `JOB_FAILED`. [`test_user.py::test_get_job_result_409_job_failed`](test_user.py).
- [x] **410** — default case; `JOB_RESULT_GONE`. [`test_user.py::test_get_job_result_410`](test_user.py).
- [x] **410** — after cleanup; `JOB_RESULT_GONE`. [`test_user.py::test_get_job_result_410_after_cleanup`](test_user.py).
- [x] **425** — job new; `JOB_RESULT_NOT_READY`. [`test_user.py::test_get_job_result_425_job_new`](test_user.py).
- [x] **425** — job queued; `JOB_RESULT_NOT_READY`. [`test_user.py::test_get_job_result_425_job_queued`](test_user.py).
- [x] **425** — job processing; `JOB_RESULT_NOT_READY`. [`test_user.py::test_get_job_result_425_job_processing`](test_user.py).
- [x] **425** — job error; `JOB_RESULT_NOT_READY`. [`test_user.py::test_get_job_result_425_job_error`](test_user.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_user.py::test_get_job_result_404_job_not_found`](test_user.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_user.py::test_get_job_result_403_job_access_forbidden`](test_user.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_get_job_result_422_invalid_request`](test_user.py).
- [x] **200** — stream failure. [`test_user.py::test_get_job_result_200_stream_failure`](test_user.py).

#### GET / - 307

- [x] **307** — docs redirect. [`test_user.py::test_get_root_307_docs_redirect`](test_user.py).

#### GET /v1/engines - 200, 403, 422

- [x] **200** — public fields; `ENGINES_RETRIEVED`. [`test_user.py::test_get_engines_200_public_fields`](test_user.py).
- [x] **403** — admin filters. [`test_user.py::test_get_engines_403_admin_filters`](test_user.py).
- [x] **422** — invalid filter; `REQUEST_VALIDATION_ERROR`. [`test_user.py::test_get_engines_422_invalid_filter`](test_user.py).

#### GET /openapi.json - 200

- [x] **200** — individual engine/upload toggles, cached schema, successful calls, and authentication when hidden. [`test_user.py::test_get_openapi_200_endpoint_visibility`](test_user.py).
- [x] **200** — every User/Worker/Admin section combination with all/none/mixed toggles, method-level filtering, and successful hidden user/worker/admin/debug operations. [`test_user.py::test_get_openapi_200_sections`](test_user.py).
- [x] fresh-import environment binding and default flag values. [`test_user.py::test_openapi_visibility_flags_from_environment`](test_user.py).

#### GET /docs, /docs/oauth2-redirect, /redoc, /openapi.json - 200

- [x] **200** — public pages. [`test_user.py::test_get_documentation_200_public_pages`](test_user.py).
- [x] **200** — public pages. [`test_user.py::test_head_documentation_200_public_pages`](test_user.py).

### Worker routes

#### POST /v1/jobs/lease - 200

- [x] **200** — job leased; `JOB_RETRIEVED`. [`test_worker.py::test_post_job_lease_200_job_leased`](test_worker.py).
- [x] **200** — job leased with engine; `JOB_RETRIEVED`, `ENGINES_RETRIEVED`. [`test_worker.py::test_post_job_lease_200_job_leased_with_engine`](test_worker.py).
- [x] **200** — queue empty; `JOB_QUEUE_EMPTY`. [`test_worker.py::test_post_job_lease_200_queue_empty`](test_worker.py).
- [x] **200** — timeout boundaries; `JOB_LEASED`. [`test_worker.py::test_post_job_lease_200_timeout_boundaries`](test_worker.py).
- [x] **200** — concurrent assignment; `JOB_LEASED`. [`test_worker.py::test_post_job_lease_200_concurrent_assignment`](test_worker.py).

#### PATCH /v1/jobs/{job_id}/lease - 200, 403, 404, 409, 422

- [x] **200** — default case; `JOB_LEASE_EXTENDED`. [`test_worker.py::test_patch_job_lease_extend_200`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_patch_job_lease_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_patch_job_lease_403_job_access_forbidden`](test_worker.py).
- [x] **409** — job not processing; `JOB_NOT_IN_PROCESSING`. [`test_worker.py::test_patch_job_lease_409_job_not_processing`](test_worker.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_worker.py::test_patch_job_lease_422_invalid_request`](test_worker.py).

#### DELETE /v1/jobs/{job_id}/lease - 204, 403, 404, 409, 422

- [x] **204** — default case; `JOB_RETRIEVED`, `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_delete_job_lease_204`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_delete_job_lease_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_delete_job_lease_403_job_access_forbidden`](test_worker.py).
- [x] **409** — job not processing; `JOB_NOT_IN_PROCESSING`. [`test_worker.py::test_delete_job_lease_409_job_not_processing`](test_worker.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_worker.py::test_delete_job_lease_422_invalid_request`](test_worker.py).
- [x] **204** — attempts restored. [`test_worker.py::test_delete_job_lease_204_attempts_restored`](test_worker.py).

#### GET /v1/jobs/{job_id} - 200, 403, 404

- [x] **200** — default case; `JOB_RETRIEVED`. [`test_worker.py::test_get_job_200`](test_worker.py).
- [x] **200** — with engine; `JOB_RETRIEVED`. [`test_worker.py::test_get_job_200_with_engine`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_get_job_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_get_job_403_job_access_forbidden`](test_worker.py).

#### GET /v1/jobs/{job_id}/images/{image_id}/files/image - 200, 403, 404, 409, 410, 422

- [x] **200** — default case; `JOB_RETRIEVED`. [`test_worker.py::test_get_image_200`](test_worker.py).
- [x] **404** — default case; `JOB_RETRIEVED`, `IMAGE_NOT_FOUND_FOR_JOB`. [`test_worker.py::test_get_image_404`](test_worker.py).
- [x] **410** — default case; `JOB_RETRIEVED`, `IMAGE_GONE`. [`test_worker.py::test_get_image_410`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_get_image_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_get_image_403_job_access_forbidden`](test_worker.py).
- [x] **409** — job not processing; `JOB_NOT_IN_PROCESSING`. [`test_worker.py::test_get_image_409_job_not_processing`](test_worker.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_worker.py::test_get_image_422_invalid_request`](test_worker.py).
- [x] **200** — stream failure. [`test_worker.py::test_get_image_200_stream_failure`](test_worker.py).

#### GET /v1/jobs/{job_id}/images/{image_id}/files/alto - 200, 403, 404, 409, 410, 422

- [x] **200** — default case; `JOB_RETRIEVED`. [`test_worker.py::test_get_alto_200`](test_worker.py).
- [x] **404** — default case; `JOB_RETRIEVED`, `IMAGE_NOT_FOUND_FOR_JOB`. [`test_worker.py::test_get_alto_404`](test_worker.py).
- [x] **409** — default case; `JOB_RETRIEVED`, `ALTO_NOT_REQUIRED`. [`test_worker.py::test_get_alto_409`](test_worker.py).
- [x] **410** — default case; `JOB_RETRIEVED`, `ALTO_GONE`. [`test_worker.py::test_get_alto_410`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_get_alto_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_get_alto_403_job_access_forbidden`](test_worker.py).
- [x] **409** — job not processing; `JOB_NOT_IN_PROCESSING`. [`test_worker.py::test_get_alto_409_job_not_processing`](test_worker.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_worker.py::test_get_alto_422_invalid_request`](test_worker.py).
- [x] **200** — stream failure. [`test_worker.py::test_get_alto_200_stream_failure`](test_worker.py).

#### GET /v1/jobs/{job_id}/images/{image_id}/files/page - 200, 403, 404, 409, 410, 422

- [x] **200** — default case; `JOB_RETRIEVED`. [`test_worker.py::test_get_page_200`](test_worker.py).
- [x] **404** — default case; `JOB_RETRIEVED`, `IMAGE_NOT_FOUND_FOR_JOB`. [`test_worker.py::test_get_page_404`](test_worker.py).
- [x] **409** — default case; `JOB_RETRIEVED`, `PAGE_NOT_REQUIRED`. [`test_worker.py::test_get_page_409`](test_worker.py).
- [x] **410** — default case; `JOB_RETRIEVED`, `PAGE_GONE`. [`test_worker.py::test_get_page_410`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_get_page_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_get_page_403_job_access_forbidden`](test_worker.py).
- [x] **409** — job not processing; `JOB_NOT_IN_PROCESSING`. [`test_worker.py::test_get_page_409_job_not_processing`](test_worker.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_worker.py::test_get_page_422_invalid_request`](test_worker.py).
- [x] **200** — stream failure. [`test_worker.py::test_get_page_200_stream_failure`](test_worker.py).

#### GET /v1/jobs/{job_id}/files/metadata - 200, 403, 404, 409, 410, 422

- [x] **200** — default case; `JOB_RETRIEVED`. [`test_worker.py::test_get_metadata_200`](test_worker.py).
- [x] **409** — default case; `JOB_RETRIEVED`, `META_JSON_NOT_REQUIRED`. [`test_worker.py::test_get_metadata_409`](test_worker.py).
- [x] **410** — default case; `JOB_RETRIEVED`, `META_JSON_GONE`. [`test_worker.py::test_get_metadata_410`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_get_metadata_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_get_metadata_403_job_access_forbidden`](test_worker.py).
- [x] **409** — job not processing; `JOB_NOT_IN_PROCESSING`. [`test_worker.py::test_get_metadata_409_job_not_processing`](test_worker.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_worker.py::test_get_metadata_422_invalid_request`](test_worker.py).
- [x] **200** — stream failure. [`test_worker.py::test_get_metadata_200_stream_failure`](test_worker.py).

#### GET /v1/engines/{engine_id}/files - 200, 404, 410, 422

- [x] **200** — default case; `JOB_RETRIEVED`. [`test_worker.py::test_get_engine_files_200`](test_worker.py).
- [x] **404** — default case; `JOB_RETRIEVED`, `ENGINE_FILES_NOT_FOUND`. [`test_worker.py::test_get_engine_files_404`](test_worker.py).
- [x] **410** — default case; `JOB_RETRIEVED`, `ENGINE_FILES_GONE`. [`test_worker.py::test_get_engine_files_410`](test_worker.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_worker.py::test_get_engine_files_422_invalid_request`](test_worker.py).
- [x] **404** — engine not found; `ENGINE_NOT_FOUND`. [`test_worker.py::test_get_engine_files_404_engine_not_found`](test_worker.py).
- [x] **200** — stream failure. [`test_worker.py::test_get_engine_files_200_stream_failure`](test_worker.py).

#### POST /v1/jobs/{job_id}/result - 201, 200, 403, 404, 409, 415, 422, 400, 500

- [x] **201** — default case. [`test_worker.py::test_post_job_result_201`](test_worker.py).
- [x] **201** — preserves deletion timestamps. [`test_worker.py::test_post_job_result_201_preserves_deletion_timestamps`](test_worker.py).
- [x] **200** — default case; `JOB_RESULT_REUPLOADED`. [`test_worker.py::test_post_job_result_200`](test_worker.py).
- [x] **415** — default case; `JOB_RESULT_INVALID`. [`test_worker.py::test_post_job_result_415`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_post_job_result_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_post_job_result_403_job_access_forbidden`](test_worker.py).
- [x] **409** — job not processing; `JOB_NOT_IN_PROCESSING`. [`test_worker.py::test_post_job_result_409_job_not_processing`](test_worker.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_worker.py::test_post_job_result_422_invalid_request`](test_worker.py).
- [x] **201** — validation disabled; `JOB_RESULT_UPLOADED`. [`test_worker.py::test_post_job_result_201_validation_disabled`](test_worker.py).
- [x] **500** — upload failure. [`test_worker.py::test_post_job_result_500_upload_failure`](test_worker.py).
- [x] **415** — preserves existing file. [`test_worker.py::test_post_job_result_415_preserves_existing_file`](test_worker.py).
- [x] **400** — interrupted upload. [`test_worker.py::test_post_job_result_400_interrupted_upload`](test_worker.py).
- [x] cancellation after partial staging preserves prior data, removes the staging file, and permits retry. [`test_worker.py::test_post_job_result_cancelled_upload`](test_worker.py).
- [x] post job result 409 conflicting saves; staging holds no DB connection. [`test_worker.py::test_post_job_result_409_file_save_in_progress`](test_worker.py).
- [x] post job result 200 progress during staging. [`test_worker.py::test_post_job_result_200_progress_during_staging`](test_worker.py).
- [x] post job result 200 responsive during preparation. [`test_worker.py::test_post_job_result_200_responsive_during_preparation`](test_worker.py).
- [x] post job result cancelled during preparation. [`test_worker.py::test_post_job_result_cancelled_during_preparation`](test_worker.py).

#### POST /v1/jobs/{job_id}/artifacts - 201, 200, 403, 404, 409, 415, 422, 400, 500

- [x] **201** — default case. [`test_worker.py::test_post_job_artifacts_201`](test_worker.py).
- [x] **201** — preserves deletion timestamps. [`test_worker.py::test_post_job_artifacts_201_preserves_deletion_timestamps`](test_worker.py).
- [x] **200** — default case; `JOB_ARTIFACTS_REUPLOADED`. [`test_worker.py::test_post_job_artifacts_200`](test_worker.py).
- [x] **415** — default case; `JOB_ARTIFACTS_INVALID`. [`test_worker.py::test_post_job_artifacts_415`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_post_job_artifacts_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_post_job_artifacts_403_job_access_forbidden`](test_worker.py).
- [x] **409** — job not processing; `JOB_NOT_IN_PROCESSING`. [`test_worker.py::test_post_job_artifacts_409_job_not_processing`](test_worker.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_worker.py::test_post_job_artifacts_422_invalid_request`](test_worker.py).
- [x] **201** — validation disabled; `JOB_ARTIFACTS_UPLOADED`. [`test_worker.py::test_post_job_artifacts_201_validation_disabled`](test_worker.py).
- [x] **500** — upload failure. [`test_worker.py::test_post_job_artifacts_500_upload_failure`](test_worker.py).
- [x] **415** — preserves existing file. [`test_worker.py::test_post_job_artifacts_415_preserves_existing_file`](test_worker.py).
- [x] **400** — interrupted upload. [`test_worker.py::test_post_job_artifacts_400_interrupted_upload`](test_worker.py).
- [x] cancellation after partial staging preserves prior data, removes the staging file, and permits retry. [`test_worker.py::test_post_job_artifacts_cancelled_upload`](test_worker.py).
- [x] post job artifacts 409 conflicting saves; staging holds no DB connection. [`test_worker.py::test_post_job_artifacts_409_file_save_in_progress`](test_worker.py).
- [x] post job artifacts 200 progress during staging. [`test_worker.py::test_post_job_artifacts_200_progress_during_staging`](test_worker.py).

#### PATCH /v1/jobs/{job_id} - 200, 400, 403, 404, 409

- [x] **200** — update progress; `JOB_UPDATED`, `JOB_RETRIEVED`. [`test_worker.py::test_patch_job_200_update_progress`](test_worker.py).
- [x] **200** — job completed; `JOB_RETRIEVED`. [`test_worker.py::test_patch_job_200_job_completed`](test_worker.py).
- [x] **200** — job already completed; `JOB_ALREADY_COMPLETED`. [`test_worker.py::test_patch_job_200_job_already_completed`](test_worker.py).
- [x] **200** — job marked error; `JOB_RETRIEVED`. [`test_worker.py::test_patch_job_200_job_marked_error`](test_worker.py).
- [x] **200** — job already marked error; `JOB_ALREADY_MARKED_ERROR`. [`test_worker.py::test_patch_job_200_job_already_marked_error`](test_worker.py).
- [x] **200** — job marked failed; `JOB_MARKED_FAILED`. [`test_worker.py::test_patch_job_200_job_marked_failed`](test_worker.py).
- [x] **200** — job already marked failed; `JOB_ALREADY_MARKED_FAILED`. [`test_worker.py::test_patch_job_200_job_already_marked_failed`](test_worker.py).
- [x] **200** — error remains retryable; `JOB_MARKED_ERROR`. [`test_worker.py::test_patch_job_200_error_remains_retryable`](test_worker.py).
- [x] **400** — no fields; `JOB_UPDATE_NO_FIELDS`. [`test_worker.py::test_patch_job_400_no_fields`](test_worker.py).
- [x] **403** — failure unassigned worker. [`test_worker.py::test_patch_job_403_failure_unassigned_worker`](test_worker.py).
- [x] **409** — job result missing; `JOB_RESULT_MISSING`. [`test_worker.py::test_patch_job_409_job_result_missing`](test_worker.py).
- [x] **409** — job unfinishable; `JOB_UNFINISHABLE`. [`test_worker.py::test_patch_job_409_job_unfinishable`](test_worker.py).
- [x] **409** — update failed job. [`test_worker.py::test_patch_job_409_update_failed_job`](test_worker.py).
- [x] **409** — failure invalid source state; `JOB_UNFINISHABLE`. [`test_worker.py::test_patch_job_409_failure_invalid_source_state`](test_worker.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_worker.py::test_patch_job_404_job_not_found`](test_worker.py).
- [x] **403** — job access forbidden; `API_KEY_FORBIDDEN_FOR_JOB`. [`test_worker.py::test_patch_job_403_job_access_forbidden`](test_worker.py).
- [x] **403** — unsupported worker state; `API_KEY_WORKER_FORBIDDEN`. [`test_worker.py::test_patch_job_403_unsupported_worker_state`](test_worker.py).
- [x] **200** — progress clamped; `JOB_UPDATED`. [`test_worker.py::test_patch_job_200_progress_clamped`](test_worker.py).

### Admin and debug routes

#### GET /v1/admin/keys - 200

- [x] **200** — default case; `KEYS_RETRIEVED`. [`test_admin.py::test_get_keys_200`](test_admin.py).

#### POST /v1/admin/keys - 201, 409, 422

- [x] **201** — default case; `API_KEY_VALID`. [`test_admin.py::test_post_keys_201`](test_admin.py).
- [x] **409** — duplicate label; `KEY_ALREADY_EXISTS`. [`test_admin.py::test_post_keys_409_duplicate_label`](test_admin.py).
- [x] **422** — missing role; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_post_keys_422_missing_role`](test_admin.py).
- [x] **422** — extra key; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_post_keys_422_extra_key`](test_admin.py).
- [x] **409** — generation failure; `KEY_CREATION_FAILED`. [`test_admin.py::test_post_keys_409_generation_failure`](test_admin.py).

#### POST /v1/admin/keys/{label}/secret - 201, 404, 409

- [x] **201** — default case; `KEY_SECRET_CREATED`. [`test_admin.py::test_post_keys_secret_201`](test_admin.py).
- [x] **404** — default case; `KEY_NOT_FOUND`. [`test_admin.py::test_post_keys_secret_404`](test_admin.py).
- [x] **409** — generation failure; `KEY_SECRET_CREATION_FAILED`. [`test_admin.py::test_post_keys_secret_409_generation_failure`](test_admin.py).
- [x] **201** — rotates credentials; `KEY_SECRET_CREATED`, `API_KEY_INVALID`. [`test_admin.py::test_post_keys_secret_201_rotates_credentials`](test_admin.py).

#### PATCH /v1/admin/keys/{label} - 200, 400, 404, 409, 422

- [x] **200** — update label; `KEY_UPDATED`. [`test_admin.py::test_patch_key_200_update_label`](test_admin.py).
- [x] **200** — update role; `KEY_UPDATED`. [`test_admin.py::test_patch_key_200_update_role`](test_admin.py).
- [x] **200** — update active; `KEY_UPDATED`, `API_KEY_INACTIVE`. [`test_admin.py::test_patch_key_200_update_active`](test_admin.py).
- [x] **400** — no fields; `KEY_UPDATE_NO_FIELDS`. [`test_admin.py::test_patch_key_400_no_fields`](test_admin.py).
- [x] **404** — default case; `KEY_NOT_FOUND`. [`test_admin.py::test_patch_key_404`](test_admin.py).
- [x] **409** — default case; `KEY_ALREADY_EXISTS`. [`test_admin.py::test_patch_key_409`](test_admin.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_patch_key_422_invalid_request`](test_admin.py).

#### POST /v1/admin/engines - 201, 409, 422

- [x] **201** — default case. [`test_admin.py::test_post_engine_201`](test_admin.py).
- [x] **201** — new default; `ENGINE_CREATED`, `ENGINES_RETRIEVED`. [`test_admin.py::test_post_engine_201_new_default`](test_admin.py).
- [x] **201** — new active; `ENGINE_CREATED`, `ENGINES_RETRIEVED`. [`test_admin.py::test_post_engine_201_new_active`](test_admin.py).
- [x] **201** — new default and active; `ENGINE_CREATED`, `ENGINES_RETRIEVED`. [`test_admin.py::test_post_engine_201_new_default_and_active`](test_admin.py).
- [x] **409** — default case; `ENGINE_ALREADY_EXISTS`. [`test_admin.py::test_post_engine_409`](test_admin.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_post_engine_422_invalid_request`](test_admin.py).

#### PATCH /v1/admin/engines/{name}/{version} - 200, 400, 404, 409, 422

- [x] **200** — update default; `ENGINE_CREATED`, `ENGINE_UPDATED`, `ENGINES_RETRIEVED`. [`test_admin.py::test_patch_engine_200_update_default`](test_admin.py).
- [x] **200** — update active; `ENGINE_CREATED`, `ENGINE_UPDATED`, `ENGINES_RETRIEVED`. [`test_admin.py::test_patch_engine_200_update_active`](test_admin.py).
- [x] **200** — update description; `ENGINE_UPDATED`, `ENGINES_RETRIEVED`. [`test_admin.py::test_patch_engine_200_update_description`](test_admin.py).
- [x] **400** — no fields; `ENGINE_UPDATE_NO_FIELDS`. [`test_admin.py::test_patch_engine_400_no_fields`](test_admin.py).
- [x] **404** — default case; `ENGINE_NOT_FOUND`. [`test_admin.py::test_patch_engine_404`](test_admin.py).
- [x] **409** — default case; `ENGINE_CREATED`, `ENGINE_ALREADY_EXISTS`. [`test_admin.py::test_patch_engine_409`](test_admin.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_patch_engine_422_invalid_request`](test_admin.py).

#### PUT /v1/admin/engines/{name}/{version}/files - 201, 200, 404, 415, 422, 400, 500

- [x] **201** — default case; `ENGINE_FILES_UPLOADED`. [`test_admin.py::test_put_engine_files_201`](test_admin.py).
- [x] **200** — default case; `ENGINE_FILES_UPLOADED`, `ENGINES_RETRIEVED`, `ENGINE_FILES_REUPLOADED`. [`test_admin.py::test_put_engine_files_200`](test_admin.py).
- [x] **415** — default case; `ENGINE_FILES_INVALID`. [`test_admin.py::test_put_engine_files_415`](test_admin.py).
- [x] **404** — engine not found; `ENGINE_NOT_FOUND`. [`test_admin.py::test_put_engine_files_404_engine_not_found`](test_admin.py).
- [x] **422** — missing file; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_put_engine_files_422_missing_file`](test_admin.py).
- [x] **201** — validation disabled; `ENGINE_FILES_UPLOADED`. [`test_admin.py::test_put_engine_files_201_validation_disabled`](test_admin.py).
- [x] **500** — upload failure. [`test_admin.py::test_put_engine_files_500_upload_failure`](test_admin.py).
- [x] **415** — preserves existing file. [`test_admin.py::test_put_engine_files_415_preserves_existing_file`](test_admin.py).
- [x] **400** — interrupted upload. [`test_admin.py::test_put_engine_files_400_interrupted_upload`](test_admin.py).
- [x] **500** — database failure. [`test_admin.py::test_put_engine_files_500_database_failure`](test_admin.py).
- [x] cancellation after partial staging preserves prior data, removes the staging file, and permits retry. [`test_admin.py::test_put_engine_files_cancelled_upload`](test_admin.py).
- [x] put engine files 500 commit failure. [`test_admin.py::test_put_engine_files_500_commit_failure`](test_admin.py).
- [x] **409** — another process holds the file lock; process exit releases it. [`test_admin.py::test_put_engine_files_409_other_process_saving`](test_admin.py).
- [x] put engine files 409 conflicting saves; staging holds no DB connection. [`test_admin.py::test_put_engine_files_409_file_save_in_progress`](test_admin.py).
- [x] put engine files cancelled after install. [`test_admin.py::test_put_engine_files_cancelled_after_install`](test_admin.py).
- [x] put engine files 500 unknown commit outcome. [`test_admin.py::test_put_engine_files_500_unknown_commit_outcome`](test_admin.py).
- [x] put engine files 500 unresolved backup. [`test_admin.py::test_put_engine_files_500_unresolved_backup`](test_admin.py).
- [x] put engine files 200 abandoned staging. [`test_admin.py::test_put_engine_files_200_abandoned_staging`](test_admin.py).
- [x] put engine files 200 backup cleanup failure. [`test_admin.py::test_put_engine_files_200_backup_cleanup_failure`](test_admin.py).

#### GET /v1/admin/jobs/{job_id}/artifacts - 200, 404, 422

- [x] **200** — default case. [`test_admin.py::test_get_artifacts_200`](test_admin.py).
- [x] **404** — default case; `JOB_ARTIFACTS_NOT_FOUND`. [`test_admin.py::test_get_artifacts_404`](test_admin.py).
- [x] **422** — invalid request; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_get_artifacts_422_invalid_request`](test_admin.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_admin.py::test_get_artifacts_404_job_not_found`](test_admin.py).
- [x] **200** — stream failure. [`test_admin.py::test_get_artifacts_200_stream_failure`](test_admin.py).

#### GET /v1/jobs as ADMIN - 200

- [x] **200** — deletion visibility; `JOBS_RETRIEVED`. [`test_admin.py::test_get_jobs_200_deletion_visibility`](test_admin.py).

#### GET /v1/jobs/{job_id} as ADMIN - 200

- [x] **200** — deletion visibility; `JOB_RETRIEVED`. [`test_admin.py::test_get_job_200_deletion_visibility`](test_admin.py).

#### PATCH /v1/admin/jobs/{job_id} - 200, 404, 422

- [x] **200** — force update; `JOB_UPDATED`. [`test_admin.py::test_patch_job_200_force_update`](test_admin.py).
- [x] **422** — invalid state; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_patch_job_422_invalid_state`](test_admin.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_admin.py::test_patch_job_404_job_not_found`](test_admin.py).

#### PUT /debug/jobs/{job_id}/start - 200, 404, 409, 422

- [x] **200** — ready; `JOB_STARTED`. [`test_admin.py::test_put_job_start_200_ready`](test_admin.py).
- [x] **409** — missing input; `JOB_NOT_READY`. [`test_admin.py::test_put_job_start_409_missing_input`](test_admin.py).
- [x] **409** — already started; `JOB_ALREADY_STARTED`. [`test_admin.py::test_put_job_start_409_already_started`](test_admin.py).
- [x] **422** — invalid id; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_put_job_start_422_invalid_id`](test_admin.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_admin.py::test_put_job_start_404_job_not_found`](test_admin.py).

#### GET /v1/engines as ADMIN - 200

- [x] **200** — admin filters. [`test_admin.py::test_get_engines_200_admin_filters`](test_admin.py).

### Image-name validation

- [x] job image extension options. [`test_image_name_validation.py::test_job_image_extension_options`](test_image_name_validation.py).
- [x] image extension defaults. [`test_image_name_validation.py::test_image_extension_defaults`](test_image_name_validation.py).
- [x] extension error response; `REQUEST_VALIDATION_ERROR`. [`test_image_name_validation.py::test_extension_error_response`](test_image_name_validation.py).

### Input cleanup and lifecycle

- [x] cleanup only terminal inputs. [`test_input_cleanup.py::test_cleanup_only_terminal_inputs`](test_input_cleanup.py).
- [x] disabled cleanup keeps inputs. [`test_input_cleanup.py::test_disabled_cleanup_keeps_inputs`](test_input_cleanup.py).
- [x] cleanup preserves unknown directories and symlink targets. [`test_input_cleanup.py::test_cleanup_preserves_unknown_directories_and_symlink_targets`](test_input_cleanup.py).
- [x] cleanup retries failed deletion. [`test_input_cleanup.py::test_cleanup_retries_failed_deletion`](test_input_cleanup.py).
- [x] input cleanup disabled by default. [`test_input_cleanup.py::test_input_cleanup_disabled_by_default`](test_input_cleanup.py).
- [x] image record cleanup disabled by default. [`test_input_cleanup.py::test_image_record_cleanup_disabled_by_default`](test_input_cleanup.py).
- [x] input retention configuration. [`test_input_cleanup.py::test_input_retention_configuration`](test_input_cleanup.py).
- [x] invalid input retention. [`test_input_cleanup.py::test_invalid_input_retention`](test_input_cleanup.py).
- [x] input retention. [`test_input_cleanup.py::test_input_retention`](test_input_cleanup.py).
- [x] optional image record cleanup. [`test_input_cleanup.py::test_optional_image_record_cleanup`](test_input_cleanup.py).
- [x] image records retained when removal fails. [`test_input_cleanup.py::test_image_records_retained_when_removal_fails`](test_input_cleanup.py).
- [x] image record cleanup transaction recovery. [`test_input_cleanup.py::test_image_record_cleanup_transaction_recovery`](test_input_cleanup.py).
- [x] cleanup interval. [`test_input_cleanup.py::test_cleanup_interval`](test_input_cleanup.py).
- [x] invalid cleanup interval. [`test_input_cleanup.py::test_invalid_cleanup_interval`](test_input_cleanup.py).
- [x] cleanup skips locked and requeued jobs. [`test_input_cleanup.py::test_cleanup_skips_locked_and_requeued_jobs`](test_input_cleanup.py).

### Output cleanup

- [x] output retention. [`test_output_cleanup.py::test_output_retention`](test_output_cleanup.py).
- [x] configured output targets. [`test_output_cleanup.py::test_configured_output_targets`](test_output_cleanup.py).
- [x] output cleanup disabled. [`test_output_cleanup.py::test_output_cleanup_disabled`](test_output_cleanup.py).
- [x] output cleanup retries errors. [`test_output_cleanup.py::test_output_cleanup_retries_errors`](test_output_cleanup.py).
- [x] output cleanup preserves unknown files and symlinks. [`test_output_cleanup.py::test_output_cleanup_preserves_unknown_files_and_symlinks`](test_output_cleanup.py).
- [x] output cleanup defaults. [`test_output_cleanup.py::test_output_cleanup_defaults`](test_output_cleanup.py).
- [x] output target configuration. [`test_output_cleanup.py::test_output_target_configuration`](test_output_cleanup.py).
- [x] invalid output configuration. [`test_output_cleanup.py::test_invalid_output_configuration`](test_output_cleanup.py).
- [x] zero output retention configuration. [`test_output_cleanup.py::test_zero_output_retention_configuration`](test_output_cleanup.py).
- [x] immediate output cleanup skips time check. [`test_output_cleanup.py::test_immediate_output_cleanup_skips_time_check`](test_output_cleanup.py).

### Cleanup transaction recovery

- [x] recovers after files removed but commit fails. [`test_deletion_tracking.py::test_recovers_after_files_removed_but_commit_fails`](test_deletion_tracking.py).

### Alembic migrations

- [x] upgrade existing jobs. [`test_migrations.py::test_upgrade_existing_jobs`](test_migrations.py).

### Test isolation and documentation consistency

- [x] failed tests drop databases. [`test_test_isolation.py::test_failed_tests_drop_databases`](test_test_isolation.py).
- [x] readme references current tests. [`test_test_isolation.py::test_readme_references_current_tests`](test_test_isolation.py).

### Worker adapter

- [x] adapter failure state. [`test_adapter.py::test_adapter_failure_state`](test_adapter.py).

### File-transfer failure behavior

Temporary siblings use a single naming convention beside their official file:
`<official-filename>.docapi-tmp-<purpose>-<random-id><extension>`. The purposes are
`upload-staging` for new data being written/validated and `rollback-backup` for
an original retained until a DB update commits. The final extension is repeated
because image writers determine the format from the filename. For example:

- `meta.json.docapi-tmp-upload-staging-<random-id>.json`
- `<job-id>.zip.docapi-tmp-upload-staging-<random-id>.zip`
- `meta.json.docapi-tmp-rollback-backup-<random-id>.json`

All seven file-saving endpoints (image, ALTO, PAGE XML, metadata, result ZIP,
artifact ZIP, and engine ZIP) use `api/file_saving.py`. Its context manager makes
the lifecycle explicit: prepare staging, let the route write/validate and install
the file and update metadata, commit or recover, then clean up. A return inside
the route's `async with` still commits before returning the response. ZIP writing
and validation share `write_zip_to_staging`; format-specific input validation
stays in the user routes. Routes use `file_save.db` explicitly for their metadata
updates, keeping the save transaction distinct from preliminary request lookups.

Staging names are unique for each upload, including ZIPs. A replacement renames
the old official file into a rollback backup, then renames staging into the
official path. The backup remains until the metadata transaction commits. On a
known failure the DB rolls back and the original is renamed back; a failed first
upload removes the new official file. Success removes the backup. Handled errors
and cancellation remove staging files. At most two data file paths for a
destination coexist during this flow, excluding the empty lock file; renaming
does not copy the ZIP. FastAPI may also spool the incoming multipart request in
system temporary storage, outside this destination-directory limit.

Nonblocking filesystem locks coordinate saves across API processes sharing the
same data directories. A second save to the same destination immediately returns
409 `FILE_SAVE_IN_PROGRESS`; the caller can retry after the first finishes.
Different images, ALTO, PAGE XML, metadata, and ZIP destinations can stage
concurrently. Each lock uses an empty hidden sibling named
`.<official-filename>.docapi-save.lock`. The file remains after release so all
processes lock the same inode; its presence does not mean a save is active. The
OS releases the lock when its descriptor closes or the process exits. Do not
manually remove lock files while saves may be running.

Staging preparation, writing, and validation hold no DB connection. Directory
scans and staging preparation run in a worker thread; cancellation waits for that
worker before cleanup and unlocking. The save session borrows a pooled connection
only at final installation to check/lock the job or engine, rename files, update
metadata, and commit or roll back. The row lock prevents queuing/cancellation or
cleanup from changing the record during that final step. Backup cleanup happens
after commit has returned the connection to the pool.

An abandoned staging file can be discarded under the upload lock if there is no
rollback backup. A retained backup requires recovery before further replacement:
its contents must not be discarded or promoted purely by the filename. If commit
acknowledgement is lost, the server preserves the backup instead of blindly
restoring old bytes over potentially committed metadata. If deleting a backup
fails after a known successful commit, the upload stays successful and logs the
retained backup. These cases need operator recovery. There is no durable recovery
journal or guarantee of atomicity across a process/system crash. Cancellation
before commit rolls back; once commit starts it is shielded and resolved before
unlocking, retaining the new data if that commit succeeds.

Interrupted request bodies fail before route storage writes. Upload read/write,
validation, replacement, metadata update, and real DB commit failures
preserve the prior official data and DB records, remove staging, and permit a
retry. Input upload flags and image hashes share the file-save transaction;
engine ZIP replacement shares the transaction recording
`files_updated`. CRUD operations join this transaction instead of committing
individual steps. Result/artifact ZIP uploads still have no uploaded flag in the
DB, but use the same replacement/backup flow. Tests inspect the backup while
commit is pending, verify that overlapping saves return 409 and allow retry,
assert that staging holds no pooled connections, and verify progress requests
continue while a ZIP is still being staged.

Input routes leave jobs in NEW after saving their inputs. A separate lifespan
background task checks at startup and every `JOB_READINESS_CHECK_INTERVAL_SECONDS`
(default 5 seconds), then queues NEW jobs with every required upload flag set.
The task uses batches and skips locked job rows; multiple API processes can run
it safely. Final file installation briefly locks the job row and rechecks access,
so queuing or cancellation during staging prevents a user from installing that
file afterward. Admin state overrides retain their existing behavior.
Route tests advance readiness explicitly; task tests cover startup recovery,
interval configuration, retry, cancellation, incomplete inputs, and locked jobs.

Download tests inspect raw ASGI messages for file-open/read errors and client
disconnects. Once a `200` header has been sent, a stream failure cannot turn it
into a JSON `500`: the body is incomplete and the stream aborts. Tests verify
that no complete body or second response is sent, that stored data is unchanged,
and that a subsequent complete download succeeds. Buffered HTTPX responses
alone would hide these failures and could misleadingly appear successful.

### Cleanup concurrency and API responsiveness

- [x] input/result/artifact deletion is held while API reads, file downloads, and
  updates to active jobs complete. A real thread watchdog detects event-loop
  blocking. Updates to another completed job in the selected cleanup batch wait
  for deletion and commit, then succeed. [`test_cleanup_concurrency.py::test_api_remains_responsive_during_cleanup`](test_cleanup_concurrency.py).

Cleanup runs as an asyncio task; filesystem removal runs in a thread. It holds
row locks on up to 100 selected jobs through file removal and commit, so writes
to those jobs may wait for slow deletion of another job in the same batch. Reads
and writes outside that batch remain responsive in these tests. This is a
concurrency regression check, not a disk-load or API throughput benchmark.

### Application startup and launcher

- [x] partial development credentials create only missing keys. [`test_startup.py::test_startup_creates_only_missing_test_credentials`](test_startup.py).
- [x] real uniqueness conflicts roll back credential inserts and prevent serving. [`test_startup.py::test_startup_key_conflict_rolls_back`](test_startup.py).
- [x] startup creates credentials. [`test_startup.py::test_startup_creates_credentials`](test_startup.py).
- [x] startup preserves existing credentials. [`test_startup.py::test_startup_preserves_existing_credentials`](test_startup.py).
- [x] startup rotates configured admin. [`test_startup.py::test_startup_rotates_configured_admin`](test_startup.py).
- [x] startup without admin setting preserves admin. [`test_startup.py::test_startup_without_admin_setting_preserves_admin`](test_startup.py).
- [x] startup invalid credentials. [`test_startup.py::test_startup_invalid_credentials`](test_startup.py).
- [x] startup database failure rolls back. [`test_startup.py::test_startup_database_failure_rolls_back`](test_startup.py).
- [x] cleanup task lifecycle. [`test_startup.py::test_cleanup_task_lifecycle`](test_startup.py).
- [x] production configuration. [`test_run.py::test_production_configuration`](test_run.py).
- [x] runner reload and database setup. [`test_run.py::test_runner_reload_and_database_setup`](test_run.py).
- [x] runner database failure prevents serving. [`test_run.py::test_runner_database_failure_prevents_serving`](test_run.py).


### Job readiness and concurrent input saves

- [x] Failed replacement restores the original before releasing the job row lock. [`test_user.py::test_put_image_500_restore_before_readiness_unlock`](test_user.py).

- [x] queue ready jobs batches. [`test_job_readiness.py::test_queue_ready_jobs_batches`](test_job_readiness.py).
- [x] queue ready jobs only new. [`test_job_readiness.py::test_queue_ready_jobs_only_new`](test_job_readiness.py).
- [x] queue ready jobs required inputs. [`test_job_readiness.py::test_queue_ready_jobs_required_inputs`](test_job_readiness.py).
- [x] queue ready jobs skips locked. [`test_job_readiness.py::test_queue_ready_jobs_skips_locked`](test_job_readiness.py).
- [x] readiness interval configuration. [`test_job_readiness.py::test_readiness_interval_configuration`](test_job_readiness.py).
- [x] readiness loop interval and retry. [`test_job_readiness.py::test_readiness_loop_interval_and_retry`](test_job_readiness.py).
- [x] readiness recovers jobs at startup. [`test_startup.py::test_readiness_recovers_jobs_at_startup`](test_startup.py).
- [x] readiness task lifecycle. [`test_startup.py::test_readiness_task_lifecycle`](test_startup.py).
- [x] put inputs 409 conflicting saves; staging holds no DB connection. [`test_user.py::test_put_input_409_file_save_in_progress`](test_user.py).
- [x] put image 201 parallel input formats. [`test_user.py::test_put_image_201_parallel_input_formats`](test_user.py).
- [x] put image 409 state changed during staging. [`test_user.py::test_put_image_409_state_changed_during_staging`](test_user.py).

### Remaining depth and edge cases

The endpoint/outcome gaps from #19 are covered above, including the two key
creation/rotation failure branches that were previously marked done without
actual tests. The remaining work concerns deeper combinations and fault paths:

- [x] Expand successful readonly cases: shared read tests run as user, readonly,
  and admin for job listing, job details, results, filters, and data visibility.
  Own-job listing and access to known jobs belonging to other owners are covered.
- [x] Expand date-filter tests: inclusive boundaries for all four fields in UTC,
  positive/negative offsets, and naive UTC; nullable start/finish dates; combined
  date/state/engine/result filters; and descending creation-date ordering.
- [x] Extend OpenAPI visibility tests to all engine/upload toggles, every
  User/Worker/Admin section combination, empty/unknown/General/Debug sections,
  method-level filtering, cached schemas, and fresh-import environment binding.
  Successful calls and authentication are checked independently of visibility.
- [x] Expand startup coverage to production/development credentials, admin-key
  creation and rotation, preserved/disabled existing keys, malformed credentials,
  DB read/commit failures, cleanup-task cancellation, and launcher reload/DB setup.
  These cases live in `test_startup.py` and `test_run.py`.
- [x] Migration coverage is scoped to the last two migrations: cleanup timestamps
  and image-count backfill, including existing data, downgrade, and re-upgrade.
  This coverage is sufficient for the current scope; older transitions and
  additional migration-failure scenarios are not planned here.
- [x] Add upload parsing/read/write/replacement and DB failure tests, preserved
  ZIP replacements, retry recovery, and download open/read/client-disconnect
  failures. These cases stay in the corresponding user/worker/admin route files.
- [ ] Add races with cleanup actually removing files during requests. Current
  concurrency tests hold deletion while requests run; cleanup tests also cover
  failed deletion, failed commits, recovery, and locked/requeued jobs.
- [ ] Add deeper validator combinations and large or unusual multipart/XML/ZIP
  payloads. Current tests cover each configured XML check, ZIP-validation bypass,
  extension policy, missing files, and selected malformed payloads.
- [ ] Add stress coverage for multiple workers leasing many jobs, retry/error
  races, and reconnect/cancellation behavior. Current tests cover one-job
  concurrent assignment, timeout/grace boundaries, retry exhaustion, and release
  attempt restoration.
- [ ] Review and test defensive invariant failures caused by DB races or
  unexpected CRUD results. Generic internal-error serialization has a focused
  failure-injection test; arbitrary server faults are not enumerated per route.
