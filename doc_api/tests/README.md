# Running tests

Install the service and test dependencies in your Python environment:

```sh
python -m pip install -e '.[server,dev]'
export TEST_DATABASE_URL='postgresql+asyncpg://test_user:password@localhost:5432/postgres'
python -m pytest doc_api/tests doc_worker/tests
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
pytest run configuration targeting `doc_api/tests` (or both `doc_api/tests` and
`doc_worker/tests` for the complete suite), set the working directory to
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
`@pytest.mark.parametrize("user_role", ["user", "admin", "manager"])` or
`@pytest.mark.parametrize("worker_role", ["worker", "admin", "manager"])` to run the same
case under each role. The corresponding header fixture and workflow
fixtures use that role; unrelated actors retain their normal role. Each run has
its own temporary database and upload directories. Without parametrization,
`user_role` defaults to `user` and `worker_role` to `worker`.

Role-specific ownership/state denials, engine restrictions, and worker-only
update validation retain their own cases: admins intentionally bypass these
workflow restrictions. Where a shared successful response exposes more data to an
admin, the test explicitly checks the expected fields for each role. This keeps
admin coverage alongside the operation it exercises without duplicating tests.

Read endpoints use `reader_role` with `user`, `readonly`, `admin`, and `manager`, and explicit
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

## Test coverage

This inventory documents implemented tests for the current endpoints and response
assertions, incorporating the coverage originally tracked in
[issue #19](https://github.com/DCGM/DocAPI/issues/19). Entries reference test
functions, including their parameterized cases, rather than a fixed test count.
The sections describe covered behavior; they do not imply that every combination
of inputs and configuration has been exhausted.

`test_readme_references_current_tests` checks that every current test function is
listed and that no reference points to a removed function. Update this inventory
in the same change as adding, renaming, moving, or removing tests.
`test_registered_routes_have_test_cases` checks the explicit route inventory in
`helpers.py` against all registered application routes, including root and debug
routes. Every protected route has missing/malformed/unknown/wrong-secret/inactive
key checks, plus checks for each disallowed role. These authentication matrices
are listed once below instead of repeated under each endpoint.

### Cross-role scenarios

Successful user and worker route tests also run with admin credentials where
applicable. Shared read tests run as user, readonly, and admin for job listing,
details, results, filters, and data visibility. Readonly coverage includes
own-job listing and access to known jobs belonging to other owners.

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

- [x] post job 201 mixed input links. [`test_user.py::test_post_job_201_mixed_input_links`](test_user.py).
- [x] post job 422 invalid input link. [`test_user.py::test_post_job_422_invalid_input_link`](test_user.py).
- [x] post job 201 input links metadata requirement. [`test_user.py::test_post_job_201_input_links_metadata_requirement`](test_user.py).
- [x] post job 201 preserves input link. [`test_user.py::test_post_job_201_preserves_input_link`](test_user.py).

#### GET /v1/jobs - 200, 403, 422

Filters combine before pagination. `owner_label` (admin/manager only) and `worker_label`
filter exact, case-sensitive API key labels; users and readonly keys remain scoped to their own jobs.
`limit` defaults to unrestricted and `offset` to 0. `sort_by` accepts
`created_date` (default), `started_date`, `finished_date`, or `last_change`;
`sort_order` accepts `desc` (default) or `asc`. Missing timestamps sort last,
with UUID breaking ties in the selected direction. `X-Total-Count` reports the
filtered total and `X-Has-More` indicates more results. Result availability means DONE with no `results_deleted_at` timestamp; the filter
is applied in SQL before pagination, without checking files.

- [x] **200** — owner/worker filters, role scoping, pagination, empty pages and response headers. [`test_user.py::test_get_jobs_200_pagination_and_principals`](test_user.py).
- [x] **403** — owner filtering restricted to admins/managers. [`test_user.py::test_get_jobs_403_owner_filter`](test_user.py).
- [x] **200** — all timestamp sort modes/directions, nulls last and stable UUID ties across pages. [`test_user.py::test_get_jobs_200_sorting`](test_user.py).
- [x] **200** — repeated state filters combine states before pagination. [`test_user.py::test_get_jobs_200_multiple_states`](test_user.py).
- [x] **200** — result availability filtering precedes pagination and total counting. [`test_user.py::test_get_jobs_200_result_filter_before_pagination`](test_user.py).
- [x] **422** — invalid pagination, sorting options. [`test_user.py::test_get_jobs_422_listing_options`](test_user.py).

Owner and worker key labels are included only for admins and managers; an unassigned worker has a null label.

- [x] **200** — key labels visible only to admins/managers, including unassigned workers. [`test_user.py::test_get_jobs_200_key_labels`](test_user.py).

Logs are omitted by default (including null log fields) and their database columns
are not loaded. Set `include_logs=true` to return permitted processing and final
logs with the existing role, attempt-history, and secret-masking rules. Single-job
detail responses retain their existing log behavior.

Engine settings are also omitted by default. Set `include_engine_settings=true`
to include them with existing masking rules. The two inclusion flags work
independently. When engine settings are omitted, the query skips the job definition
column containing them. Single-job detail responses keep their engine settings.

- [x] **200** — optional engine settings, independent log inclusion, masked secrets, null settings, and database projection. [`test_user.py::test_get_jobs_200_optional_engine_settings`](test_user.py).

- [x] **200** — optional logs, null values, role visibility, and database projection. [`test_user.py::test_get_jobs_200_optional_logs`](test_user.py).
- [x] **422** — invalid log/settings inclusion flags. [`test_user.py::test_get_jobs_422_invalid_include_logs`](test_user.py).

Date-filter tests cover inclusive boundaries for all four date fields, UTC and
positive/negative offsets, naive UTC values, and nullable start/finish dates.
They also cover combined date/state/engine/result filters and descending
creation-date ordering.

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

- [x] **200** — URL fields are enriched symmetrically for all images and roles. [`test_user.py::test_get_job_200_symmetric_input_links`](test_user.py).

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

Visibility tests cover all engine/upload toggles, User/Worker/Admin section
combinations, empty/unknown/General/Debug sections, method-level filtering,
cached schemas, and fresh-import environment binding. Successful calls and
authentication are checked independently of documentation visibility.

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
- [x] get image 410 link is not upload. [`test_worker.py::test_get_image_410_link_is_not_upload`](test_worker.py).


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
- [x] get alto 410 link is not upload. [`test_worker.py::test_get_alto_410_link_is_not_upload`](test_worker.py).


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
- [x] get page 410 link is not upload. [`test_worker.py::test_get_page_410_link_is_not_upload`](test_worker.py).


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
- [x] **200** — progress in range; `JOB_UPDATED`. [`test_worker.py::test_patch_job_200_progress_in_range`](test_worker.py).


### Admin and debug routes

#### GET /v1/admin/keys - 200

- [x] **200** — default case; `KEYS_RETRIEVED`. [`test_admin.py::test_get_keys_200`](test_admin.py).
- [x] **200** — optional role filter for every role, admin and manager access, including empty matches. [`test_admin.py::test_get_keys_200_role_filter`](test_admin.py).
- [x] **422** — invalid, uppercase or empty role rejected. [`test_admin.py::test_get_keys_422_role_filter`](test_admin.py).

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

#### PUT /debug/jobs/{job_id}/start - 200, 404, 409, 422

- [x] **200** — ready; `JOB_STARTED`. [`test_admin.py::test_put_job_start_200_ready`](test_admin.py).
- [x] **409** — missing input; `JOB_NOT_READY`. [`test_admin.py::test_put_job_start_409_missing_input`](test_admin.py).
- [x] **409** — already started; `JOB_ALREADY_STARTED`. [`test_admin.py::test_put_job_start_409_already_started`](test_admin.py).
- [x] **422** — invalid id; `REQUEST_VALIDATION_ERROR`. [`test_admin.py::test_put_job_start_422_invalid_id`](test_admin.py).
- [x] **404** — job not found; `JOB_NOT_FOUND`. [`test_admin.py::test_put_job_start_404_job_not_found`](test_admin.py).

#### GET /v1/engines as ADMIN - 200

- [x] **200** — admin filters. [`test_admin.py::test_get_engines_200_admin_filters`](test_admin.py).

### Input download links

Job creation tests cover all 27 combinations of linked, uploaded, and missing
image/ALTO/PAGE inputs across two images, under user and admin credentials.
They verify that links stay in the job definition, image records hold only
presence booleans, links do not set upload flags, and missing uploads keep jobs NEW,
and workers receive the URLs after leasing. Other cases cover HTTP(S) URL
validation, long signed URLs, metadata requirements, migration downgrade/re-upgrade,
and input cleanup with or without deleting image records. Upload-only file
endpoints still return GONE for inputs that have only a URL. External worker
requests omit DocAPI credentials and do not parse source data as an API envelope.

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

- [x] cleanup linked inputs. [`test_input_cleanup.py::test_cleanup_linked_inputs`](test_input_cleanup.py).

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

### Per-attempt job logs

Process logs and separate final reports contain sparse JSON objects keyed by the
zero-based attempt number. Missing attempts have no entry; responses select the
current attempt explicitly and history is ordered numerically.
Worker updates append to the current entry; admin replacement affects only that
entry. Leasing clears a reused entry, adds a technical-only retry header, and
leaves a quiet first attempt's logs null. Job detail/list responses keep string
fields: user/readonly/worker responses show the current attempt; admins and
managers see joined history. A quiet current attempt does not expose an earlier public message.

- [x] worker progress/finalization logs. [`test_worker.py::test_patch_job_200_attempt_logs`](test_worker.py).
- [x] reused attempt entries. [`test_worker.py::test_post_job_lease_200_reused_attempt_logs`](test_worker.py).
- [x] quiet first attempt. [`test_worker.py::test_patch_job_200_quiet_first_attempt_logs`](test_worker.py).
- [x] admin append/replace current entry. [`test_user.py::test_patch_job_200_current_attempt_logs`](test_user.py).
- [x] role-specific detail/list text. [`test_user.py::test_get_job_200_attempt_logs`](test_user.py).
- [x] Convert all four log arrays to sparse objects, preserving indices, empty strings, SQL nulls and text; downgrade/re-upgrade with current-attempt padding across migration batches. [`test_migrations.py::test_upgrade_sparse_attempt_logs`](test_migrations.py).
- [x] Send final reports only when the current attempt has nonblank text; order technical history numerically. [`test_job_mail.py::test_sparse_final_reports_use_current_attempt`](test_job_mail.py).

### Final reports and email notifications

Workers send process logs through progress updates and final reports through
`WorkerResponse.ok/error/fail(final_log=..., final_log_user=...)`. Final reports are stored
separately in `final_log` and `final_log_user`, with no
fallback between fields. Exception/API diagnostics are added only to the technical
report. Final reports replace the current attempt’s report and are cleared when an attempt
index is reused. Legacy process logs are not copied into final reports.
Final technical reports are restricted to worker/admin readers.

Keys have an optional validated email address and an `email_notifications` flag,
disabled by default. On a transition to DONE or FAILED, non-empty final user
reports go to opted-in owners through EXTERNAL_MAIL settings; final technical
reports go to INTERNAL_MAIL_RECEIVER_MAILS through INTERNAL_MAIL settings.
Subjects use USER_JOB_MAIL_SUBJECT_TEMPLATE and ADMIN_JOB_MAIL_SUBJECT_TEMPLATE with optional public/admin server names, uppercase `{status}` (DONE/FAILED), full/short
job ID, image count, attempt, engine name/version, and owner-key label placeholders.
The user default is `[{server_name}] {status} job {job_id_short}`;
the admin default is `[{admin_server_name}] {status} job {job_id_short} ({owner_label})`.
Server-error emails use `INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE`, defaulting to
`[{admin_server_name}] INTERNAL SERVER ERROR`, with `{server_name}` and
`{admin_server_name}` placeholders available.
Both templates support the same placeholders. Unknown or malformed placeholders are rejected at configuration startup.
Admins receive final-report history; owners receive the latest user report.
Retryable ERROR states are handled by the independent lease checker, which
requeues jobs or marks exhausted attempts FAILED without needing a lease request.

A separate mail checker persists one notification per job/channel/recipient.
It snapshots verified final reports, commits a delivery claim, releases the DB
connection, and performs SMTP in a thread. Failed sends retry up to
JOB_MAIL_MAX_ATTEMPTS with exponential backoff from JOB_MAIL_RETRY_DELAY_SECONDS
(capped at one day). Sent/failed records prevent ordinary duplicate delivery across
checker runs and server restarts. A five-minute claim timeout recovers interrupted
processes. SMTP has no transactional acknowledgement with PostgreSQL: a hard crash
after SMTP acceptance but before recording success can still cause a duplicate.
Graceful cancellation waits for an in-flight SMTP attempt and records its result.
Flood limits count SMTP attempts per channel/recipient/subject/hour within each
server process; suppressed deliveries wait until the next hour without consuming
an SMTP attempt. All tests mock SMTP.

- [x] post keys 201 email settings. [`test_admin.py::test_post_keys_201_email_settings`](test_admin.py).
- [x] patch key 200 email settings. [`test_admin.py::test_patch_key_200_email_settings`](test_admin.py).
- [x] key 422 invalid email. [`test_admin.py::test_key_422_invalid_email`](test_admin.py).
- [x] patch job 200 final report. [`test_user.py::test_patch_job_200_final_report`](test_user.py).
- [x] patch job 200 final reports. [`test_worker.py::test_patch_job_200_final_reports`](test_worker.py).
- [x] patch job 200 owner mail conditions. [`test_worker.py::test_patch_job_200_owner_mail_conditions`](test_worker.py).
- [x] patch job 422 final report without final state. [`test_worker.py::test_patch_job_422_final_report_without_final_state`](test_worker.py).
- [x] post job lease 200 exhausted attempt mail. [`test_worker.py::test_post_job_lease_200_exhausted_attempt_mail`](test_worker.py).
- [x] patch job 200 smtp failure. [`test_worker.py::test_patch_job_200_smtp_failure`](test_worker.py).
- [x] patch job 500 no mail on failed commit. [`test_worker.py::test_patch_job_500_no_mail_on_failed_commit`](test_worker.py).
- [x] patch job 200 api responsive during mail. [`test_worker.py::test_patch_job_200_api_responsive_during_mail`](test_worker.py).
- [x] get job 200 final reports. [`test_user.py::test_get_job_200_final_reports`](test_user.py).
- [x] adapter final reports. [`test_adapter.py::test_adapter_final_reports`](test_adapter.py).
- [x] upgrade email and final reports. [`test_migrations.py::test_upgrade_email_and_final_reports`](test_migrations.py).
- [x] recipient flood limits. [`test_job_mail.py::test_recipient_flood_limits`](test_job_mail.py).
- [x] invalid mail configuration is logged. [`test_job_mail.py::test_invalid_mail_configuration_is_logged`](test_job_mail.py).

- [x] worker writes pad missing attempt positions in all four logs. [`test_worker.py::test_patch_job_200_logs_sparse_attempts`](test_worker.py).
- [x] admin append/replace writes pad missing attempt positions. [`test_user.py::test_patch_job_200_logs_sparse_attempts`](test_user.py).

- [x] configurable internal-error subjects. [`test_job_mail.py::test_internal_error_subject_template`](test_job_mail.py).
- [x] admin subjects use the admin server name by default. [`test_job_mail.py::test_admin_subject_default_uses_admin_server_name`](test_job_mail.py).
- [x] configurable subject placeholders and subsets. [`test_job_mail.py::test_subject_template`](test_job_mail.py).
- [x] absent optional subject values. [`test_job_mail.py::test_subject_template_missing_optional_values`](test_job_mail.py).
- [x] invalid subject configuration. [`test_job_mail.py::test_subject_template_invalid_configuration`](test_job_mail.py).

- [x] mailing defaults and settings match the environment example. [`test_job_mail.py::test_mail_configuration_matches_env_example`](test_job_mail.py).

- [x] concurrent admin finalization does not duplicate mail. [`test_user.py::test_patch_job_200_concurrent_final_state_no_duplicate_mail`](test_user.py).
- [x] notification preparation failure preserves a successful job update. [`test_worker.py::test_patch_job_200_mail_preparation_failure`](test_worker.py).

### Periodic lease and mail checkers

Both tasks run once at startup and then at JOB_LEASE_CHECK_INTERVAL_SECONDS and
JOB_MAIL_CHECK_INTERVAL_SECONDS (five seconds by default). Route tests advance
them explicitly; dedicated loop and lifespan tests exercise automatic scheduling
and cancellation. Lease requests only claim QUEUED jobs and never recover leases.
`JOB_TIMEOUT_SECONDS=0` is the default and disables PROCESSING lease expiration;
lease responses contain `lease_expire_at: null`. Explicit ERROR jobs still retry
or fail at the attempt limit. Positive timeouts include the configured grace period.
Worker progress/log updates still reach the API; empty heartbeats are skipped for non-expiring leases.

- [x] non-expiring lease API responses and progress updates. [`test_worker.py::test_post_job_lease_200_non_expiring`](test_worker.py).
- [x] lease timeout defaults and validation. [`test_job_leases.py::test_lease_timeout_configuration`](test_job_leases.py).
- [x] disabled expiration preserves PROCESSING jobs and recovers ERROR jobs. [`test_job_leases.py::test_non_expiring_leases_recover_only_errors`](test_job_leases.py).
- [x] lease recovery without requests. [`test_job_leases.py::test_lease_recovery_without_requests`](test_job_leases.py).
- [x] lease recovery skips locked jobs. [`test_job_leases.py::test_lease_recovery_skips_locked_jobs`](test_job_leases.py).
- [x] lease checker runs periodically. [`test_job_leases.py::test_lease_checker_runs_periodically`](test_job_leases.py).
- [x] lease checker recovers after failure. [`test_job_leases.py::test_lease_checker_recovers_after_failure`](test_job_leases.py).
- [x] notification preparation and restart are idempotent. [`test_job_mail.py::test_notification_preparation_and_restart_are_idempotent`](test_job_mail.py).
- [x] concurrent mail checkers claim once. [`test_job_mail.py::test_concurrent_mail_checkers_claim_once`](test_job_mail.py).
- [x] mail retry limit and backoff. [`test_job_mail.py::test_mail_retry_limit_and_backoff`](test_job_mail.py).
- [x] mail flood deferral does not consume attempt. [`test_job_mail.py::test_mail_flood_deferral_does_not_consume_attempt`](test_job_mail.py).
- [x] mail recovers expired claim and rejects stale result. [`test_job_mail.py::test_mail_recovers_expired_claim_and_rejects_stale_result`](test_job_mail.py).
- [x] mail shutdown finishes inflight delivery. [`test_job_mail.py::test_mail_shutdown_finishes_inflight_delivery`](test_job_mail.py).
- [x] mail checker runs without requests. [`test_job_mail.py::test_mail_checker_runs_without_requests`](test_job_mail.py).

- [x] mail waits while smtp is disabled. [`test_job_mail.py::test_mail_waits_while_smtp_is_disabled`](test_job_mail.py).
- [x] expired mail claims respect attempt limit. [`test_job_mail.py::test_expired_mail_claims_respect_attempt_limit`](test_job_mail.py).
- [x] mail checker recovers after failure. [`test_job_mail.py::test_mail_checker_recovers_after_failure`](test_job_mail.py).
- [x] upgrade job notifications. [`test_migrations.py::test_upgrade_job_notifications`](test_migrations.py).

### Secret masking

`doc_api.tools.secrets` masks values under secret-looking keys at any depth and
known secret patterns in free text. User/readonly responses show masked engine
settings and job logs; admin and worker responses keep the real values, for jobs
and debugging. Logged exceptions are masked too. Masking happens in the server
routes and checkers, not the shared schemas. Stored data is never changed.

- `SECRET_MASKING` turns masking off.
- `SECRET_MASKING_ALLOWED_KEYS` (empty by default) lists key names that only look secret.
- `SECRET_MASKING_MASK` (`*****` by default) replaces each secret.
- `SECRET_MASKING_PARTIAL_KEYS` (empty by default) lists keys whose values keep
  their start and end visible, overriding everything else.
- `SECRET_MASKING_INTERNAL_MAIL` (on: server errors, admin job reports) and
  `SECRET_MASKING_EXTERNAL_MAIL` (off: user job reports) choose which emails are
  masked before sending.

- [x] nested keys, subtrees and unchanged input. [`test_secret_masking.py::test_mask_secrets_in_struct_nested_keys_and_subtrees`](test_secret_masking.py).
- [x] allowlist, empty by default. [`test_secret_masking.py::test_mask_secrets_in_struct_allowlist`](test_secret_masking.py).
- [x] token shapes, URL credentials and key/value pairs in free text. [`test_secret_masking.py::test_mask_secrets_in_text_patterns`](test_secret_masking.py).
- [x] DocAPI keys are recognized by the configured `KEY_PREFIX`. [`test_secret_masking.py::test_mask_secrets_in_text_uses_configured_key_prefix`](test_secret_masking.py).
- [x] partial keys reveal the start and end of a value in structures. [`test_secret_masking.py::test_partial_keys_in_struct`](test_secret_masking.py).
- [x] partial keys in free text. [`test_secret_masking.py::test_partial_keys_in_text`](test_secret_masking.py).
- [x] partial keys apply with masking turned off. [`test_secret_masking.py::test_partial_keys_apply_with_masking_disabled`](test_secret_masking.py).
- [x] configurable mask. [`test_secret_masking.py::test_custom_mask`](test_secret_masking.py).
- [x] optional high-entropy switch. [`test_secret_masking.py::test_mask_secrets_in_text_high_entropy_switch`](test_secret_masking.py).
- [x] server log handlers carry the masking filter. [`test_secret_masking.py::test_server_log_handlers_mask_secrets`](test_secret_masking.py).
- [x] only exception records are masked, traceback included. [`test_secret_masking.py::test_logging_masks_only_exceptions`](test_secret_masking.py).
- [x] unhandled errors mask the logged URL and the error mail. [`test_secret_masking.py::test_unhandled_error_masks_url_and_mail`](test_secret_masking.py).
- [x] masking can be disabled. [`test_secret_masking.py::test_masking_can_be_disabled`](test_secret_masking.py).
- [x] settings from the environment. [`test_secret_masking.py::test_masking_configuration`](test_secret_masking.py).
- [x] job report mail masking by default: internal on, external off. [`test_secret_masking.py::test_job_report_mail_masking_defaults_per_channel`](test_secret_masking.py).
- [x] job report mail masking switches per channel. [`test_secret_masking.py::test_job_report_mail_masking_per_channel_switch`](test_secret_masking.py).
- [x] server error mail unmasked with internal mail masking off; the log stays masked. [`test_secret_masking.py::test_unhandled_error_mail_unmasked_when_internal_mail_masking_off`](test_secret_masking.py).
- [x] job report mail subject and body. [`test_secret_masking.py::test_job_report_mail_is_masked`](test_secret_masking.py).
- [x] engine settings and job logs by role; storage intact. [`test_secret_masking.py::test_get_job_200_engine_settings_masked_for_readers`](test_secret_masking.py).
- [x] engine settings and job logs with partial keys by role. [`test_secret_masking.py::test_get_job_200_partial_keys_revealed_partly`](test_secret_masking.py).
- [x] validation errors echoing input. [`test_secret_masking.py::test_post_job_422_masks_echoed_input`](test_secret_masking.py).
- [x] workers and clients never load masking. [`test_secret_masking.py::test_workers_and_clients_do_not_mask`](test_secret_masking.py).
- [x] the database password is never logged. [`test_secret_masking.py::test_database_password_is_never_logged`](test_secret_masking.py).

### Alembic migrations

Migration coverage includes cleanup timestamps, image-count backfill, and the
image/ALTO/PAGE URL-presence booleans, per-attempt JSON logs and final reports,
key email settings, and the persistent notification table. Log migration
cases include null/empty logs, timestamps in retry headers, missing attempt entries,
reused indexes, and text that mentions ATTEMPT without being a separator.
Tests use existing data and exercise upgrade, downgrade, and re-upgrade. Older
migration transitions and additional migration-failure scenarios are outside the
current coverage scope.

- [x] upgrade existing jobs. [`test_migrations.py::test_upgrade_existing_jobs`](test_migrations.py).

- [x] upgrade image input urls. [`test_migrations.py::test_upgrade_image_input_url_flags`](test_migrations.py).

- [x] upgrade job attempt logs. [`test_migrations.py::test_upgrade_job_attempt_logs`](test_migrations.py).

### Build identity and migration metadata

`APP_GIT_COMMIT` respects an explicit build hash; local Git checkouts resolve/export
it at startup. Outside a checkout it stays unknown unless supplied. Docker preserves
the `COMMIT_SHA` build argument in this variable; for local builds pass
`--build-arg COMMIT_SHA="$(git rev-parse HEAD)"`. Uncommitted edits are not represented
by the hash.

The DB stores the running build's Git commit and the last successful migration's
UTC timestamp in `database_metadata`; `alembic_version` identifies the schema.

Startup has four independent controls:

| Setting | Default |
| --- | --- |
| `DATABASE_REQUIRE_MATCHING_SCHEMA` | `true` |
| `DATABASE_ALLOW_SCHEMA_UPDATE` | `false` |
| `DATABASE_REQUIRE_MATCHING_COMMIT` | `false` |
| `DATABASE_ALLOW_COMMIT_UPDATE` | `true` |

Guards check the original state and reject mismatches before any updates, even
when the corresponding update flag is enabled. To upgrade an existing schema,
disable its guard and enable schema updates. Disabling both schema flags leaves
the schema unchanged without requiring a match. An unrecognized or newer revision
cannot be automatically upgraded. The commit guard requires a known, matching
commit; missing metadata cannot establish a match. Unknown running commits are
stored as NULL when commit updates are enabled and the guard is off. Disabling
commit updates preserves the previous value, including during migrations.
Code-only updates never change `migrated_at`.

Fresh databases initialize their schema and code identity regardless of update
flags; the commit guard still requires a known build commit. Setting all four
flags to false skips database preparation, replacing `DATABASE_FORCE`.

Online Alembic migrations are explicitly requested schema changes rather than
automatic startup updates, so the startup schema flags do not prevent them. They
honor the commit guard and commit update flag. Successful schema changes record
the migration timestamp in the same transaction. No-op Alembic commands leave
metadata unchanged. Concurrent initialization uses a PostgreSQL advisory lock.
Keep all instances sharing a DB on the same code revision; the stored commit
represents the latest accepted code identity, not all concurrently running builds.
DocAPI does not create backups; see the [migration guide](../README.md) for backup
and recovery instructions.

- [x] explicit build commit is preserved. [`test_build_info.py::test_explicit_build_commit_is_preserved`](test_build_info.py).
- [x] invalid build commit is rejected. [`test_build_info.py::test_invalid_build_commit_is_rejected`](test_build_info.py).
- [x] checkout commit is exported once. [`test_build_info.py::test_checkout_commit_is_exported_once`](test_build_info.py).
- [x] git failure leaves commit unknown. [`test_build_info.py::test_git_failure_leaves_commit_unknown`](test_build_info.py).
- [x] installed package does not use unrelated checkout. [`test_build_info.py::test_installed_package_does_not_use_unrelated_checkout`](test_build_info.py).
- [x] initialization and code updates preserve migration time. [`test_db_update.py::test_initialization_and_code_update_preserve_migration_time`](test_db_update.py).
- [x] startup upgrades preserve existing data. [`test_db_update.py::test_startup_upgrade_preserves_data`](test_db_update.py).
- [x] disallowed upgrades preserve the schema. [`test_db_update.py::test_disallowed_upgrade_preserves_schema`](test_db_update.py).
- [x] failed migrations roll back metadata and release the lock. [`test_db_update.py::test_failed_migration_rolls_back_metadata_and_releases_lock`](test_db_update.py).
- [x] concurrent startups apply one migration. [`test_db_update.py::test_concurrent_startups_apply_one_migration`](test_db_update.py).
- [x] incompatible schemas reject startup with or without automatic updates. [`test_db_update.py::test_incompatible_schema_rejects_startup`](test_db_update.py).
- [x] unversioned databases reject startup. [`test_db_update.py::test_unversioned_database_rejects_startup`](test_db_update.py).
- [x] online Alembic commands record migration metadata. [`test_db_update.py::test_online_alembic_records_migration_metadata`](test_db_update.py).
- [x] database policy defaults and overrides. [`test_db_update.py::test_database_policy_configuration`](test_db_update.py).
- [x] startup updates commits or rejects mismatches according to policy. [`test_db_update.py::test_startup_commit_policy`](test_db_update.py).
- [x] strict initialization requires a known build commit. [`test_db_update.py::test_strict_commit_initialization`](test_db_update.py).
- [x] strict commit checks precede startup and direct Alembic migrations. [`test_db_update.py::test_strict_commit_check_precedes_migration`](test_db_update.py).
- [x] schema guard takes precedence over automatic updates. [`test_db_update.py::test_schema_guard_precedes_updates`](test_db_update.py).
- [x] migrations preserve the commit when commit updates are disabled. [`test_db_update.py::test_migration_preserves_commit_when_updates_disabled`](test_db_update.py).
- [x] disabled schema policies leave unchecked databases unchanged. [`test_db_update.py::test_disabled_schema_policy_preserves_unchecked_database`](test_db_update.py).
- [x] fresh databases initialize without update permissions and pass strict restarts. [`test_db_update.py::test_fresh_database_initializes_without_update_permissions`](test_db_update.py).
- [x] database metadata migration and no-op preservation. [`test_migrations.py::test_upgrade_database_metadata`](test_migrations.py).

### Test isolation and documentation consistency

- [x] failed tests drop databases. [`test_test_isolation.py::test_failed_tests_drop_databases`](test_test_isolation.py).
- [x] readme references current tests. [`test_test_isolation.py::test_readme_references_current_tests`](test_test_isolation.py).

### Worker adapter

- [x] adapter failure state. [`test_adapter.py::test_adapter_failure_state`](test_adapter.py).

- [x] adapter uploaded input uses docapi. [`test_adapter.py::test_adapter_uploaded_input_uses_docapi`](test_adapter.py).

File downloads skip API-code parsing unless the response has a JSON content type.
Workers always stream engine downloads; result downloads offer `get_result_stream`
for saving directly to disk. Streaming checks HTTP status before opening the target,
writes chunks, and closes the response on success, read failure, or write failure.
Existing bytes-returning download methods remain available.

- [x] non-JSON responses are not parsed. [`test_adapter.py::test_adapter_response_skips_non_json`](test_adapter.py).
- [x] JSON content types preserve API codes. [`test_adapter.py::test_adapter_response_extracts_json_code`](test_adapter.py).
- [x] non-envelope JSON has no API code. [`test_adapter.py::test_adapter_response_json_without_code`](test_adapter.py).
- [x] malformed JSON is tolerated; unexpected errors propagate. [`test_adapter.py::test_adapter_response_json_errors`](test_adapter.py).
- [x] explicit codes skip parsing. [`test_adapter.py::test_adapter_response_explicit_code_skips_json`](test_adapter.py).
- [x] buffered file downloads avoid JSON parsing. [`test_adapter.py::test_adapter_download_does_not_parse_body`](test_adapter.py).
- [x] engine/result streaming handles HTTP, read and write failures without buffering the body. [`test_adapter.py::test_adapter_stream_download`](test_adapter.py).

### Worker input source preferences

Worker client tests live in `doc_worker/tests`, separate from the worker API
endpoint tests in `doc_api/tests/test_worker.py`. Run them without a database:

```sh
python -m pytest doc_worker/tests
```

The wrapper prepares linked and uploaded inputs using independent
`prefer_image_url`, `prefer_alto_url`, and `prefer_page_url` flags, all false by
default. Tests cover every flag combination, single-source inputs, mixed
downloads, and failed source requests.

- [x] worker input source policy. [`test_worker_input_source_policy`](../../doc_worker/tests/test_doc_worker_wrapper.py).
- [x] worker mixed source downloads. [`test_worker_mixed_source_downloads`](../../doc_worker/tests/test_doc_worker_wrapper.py).

- [x] worker external input download. [`test_worker_external_input_download`](../../doc_worker/tests/test_doc_worker_wrapper.py).
- [x] worker external request has no docapi credentials. [`test_worker_external_request_has_no_docapi_credentials`](../../doc_worker/tests/test_doc_worker_wrapper.py).
- [x] worker external input failure. [`test_worker_external_input_failure`](../../doc_worker/tests/test_doc_worker_wrapper.py).

### Optional worker health mail

The wrapper accepts `health_config=WorkerHealthConfig(...)` from
`doc_worker.doc_worker_wrapper`, together with a `mail_sender`. Constructor arguments `identity` (class name by default), `hostname` (automatic
by default), and `mail_subject` apply to both alive and failure emails. The subject
default is `[{identity}] {status} ({hostname})`; supported placeholders are
`identity`, `hostname`, `status`, and `time` (UTC datetime, e.g. `{time:%Y-%m-%d}`).
Startup uses the same
alive report as periodic checks. Configuring health without a mail sender logs
a warning and rejects initialization before creating directories or polling. Reports use short aligned rows and separate
time-window sections for mobile reading. Lifecycle mail is
opt-in and independent of job final reports. Defaults are an immediate alive report at startup, daily
alive mail; an alive interval of zero disables
periodic mail. Alive messages report process uptime and the last pipeline
return, rather than guaranteeing that processing is progressing.

- [x] Startup, periodic alive mail, disabling mail, masking, invalid intervals, and SMTP failures. [`test_worker_health.py`](../../doc_worker/tests/test_worker_health.py).
- [x] Wrapper lifecycle integration, graceful interruption, unexpected termination, and failures that cannot be reported to DocAPI. [`test_worker_health.py`](../../doc_worker/tests/test_worker_health.py).
- [x] Clear job/lease state after every pipeline cycle, preserve it through reporting and cleanup, and use a newly acquired lease when fetching job details fails. [`test_worker_health.py`](../../doc_worker/tests/test_worker_health.py).

Explicit worker failures are mailed whenever a mail sender is configured, even
without health reports. Each failure sends an alert with available exception
details and tracebacks; secrets are masked and SMTP failures are contained.

### Persistent worker job history

`job_history_path` enables per-worker JSONL history independently of mail settings.
It is disabled by default in the general wrapper. Each completed local attempt
records the job ID, attempt number, UTC start/finish times, duration, outcome,
engine identity and image count. Retried job IDs produce separate entries; idle
polls produce none. Logs, exceptions, input links and engine definitions are not
stored. Outcomes describe the worker attempt, including interrupted/crashed
attempts observed by Python; a hard kill cannot write a completion record.

Each attempt appends one JSON record per line without reading or rewriting
previous entries. History survives job-directory cleanup and restarts. A sibling
`.stats.json` file holds cumulative totals, known job IDs for unique-job counts,
and compact recent records for the exact 7-day and 24-hour windows.

Normal appends and alive reports use the cache. All-time counters are updated
incrementally; rolling windows inspect only the recent cached records. A stale
cache reads only the missing JSONL tail. Missing/invalid caches rebuild from
history once, and the cache is atomically replaced. Use separate paths per worker.
Malformed history lines are preserved, skipped with a warning, and counted in
the health report. History/cache write failures are logged without changing the
processing outcome. Rolling windows use completion timestamps.

- [x] Persistence, retry counts, time windows, append-only writes, cache reconstruction/tail recovery, corrupt records and concurrent statistics reads. [`test_job_history.py`](../../doc_worker/tests/test_job_history.py).
- [x] Pipeline outcomes, interruptions, crashes, idle polls and history failures. [`test_job_finished.py`](../../doc_worker/tests/test_job_finished.py).
- [x] Identical startup/periodic alive reports and graceful handling of unavailable statistics. [`test_worker_health.py`](../../doc_worker/tests/test_worker_health.py).

### Worker final reports and automatic error messages

`DocWorkerWrapper._report_job_response()` handles OK, ERROR and FAIL reporting
and failure emails when a mail sender is configured. Success is sent only after uploading results.
Reporting returns a boolean and never replaces the original pipeline response
or reports a different state as a fallback. Reporting failures send a worker failure email,
retain local job files, and are recorded separately as `reported_to_api`
in history and `report fail` in alive summaries. Callbacks receive the original
pipeline response.
Implementations return their final logs directly through
`WorkerResponse.ok/error/fail(final_log=..., final_log_user=...)`. The wrapper
sends those reports, adding exception and API diagnostics only to the technical
log. Failures outside processing use the wrapper's configurable error messages.

The `error_messages` constructor argument overrides selected entries from
[`DEFAULT_WORKER_ERRORS`](../../doc_worker/doc_worker_wrapper.py), for example:

```python
error_messages={
    "download_job_data": WorkerErrorMessage(
        final_log="Input preparation failed",
        final_log_user="Could not download the inputs. Please try again later.",
    ),
}
```

Import `WorkerErrorMessage` from `doc_worker.doc_worker_wrapper`. Omitted entries keep
the existing English defaults. Either field may be `None` to omit its message;
technical diagnostics are retained. `download_input` supports `{kind}` and
`{filename}` in both messages. Invalid names or templates fail at construction.
Explicit responses returned by `process_job` retain their own messages.

- [x] Separate technical/user overrides, unchanged defaults, original outcomes and technical-only exception diagnostics. [`test_job_finished.py`](../../doc_worker/tests/test_job_finished.py).
- [x] Custom input download messages for image, ALTO and PAGE inputs from both DocAPI and external links. [`test_doc_worker_wrapper.py`](../../doc_worker/tests/test_doc_worker_wrapper.py).

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

### Defensive CRUD results and disappearing records

Route tests inject unexpected CRUD result codes. They assert that the logged
exception is `RouteInvariantError`, rather than accepting an unrelated crash as sufficient evidence of a 500. Responses
retain the generic `INTERNAL_ERROR` envelope without internal diagnostic text.
CRUD success codes are trusted to guarantee their corresponding result; routes
do not recheck successful payloads. Coverage includes job creation/listing/details,
referenced engine and image lookups, lease/progress operations, input metadata updates, engine-file metadata,
and admin key/engine/job operations. Lists and input saves check their CRUD result
codes instead of returning success unconditionally. Unexpected CRUD results fail explicitly instead of falling through to another
update path.

Other tests delete actual records in the isolated database after existence
checks, before image retrieval, before updates, or after staging but before file
installation. Missing jobs/images/engines return the appropriate 404 response;
staged data is discarded and previous files remain intact. Input update failures
roll back the file installation and retain the previous upload flags. The debug
start route rechecks an unsuccessful conditional start, distinguishing missing
jobs, incomplete inputs, and a job queued by another operation. Job lookups
refresh cached ORM records so repeated reads observe committed state changes.

User/worker cases also run under admin authentication where applicable; read
cases include readonly credentials. These tests cover explicit route contracts
and selected races; arbitrary malformed return structures and every possible
server fault are not enumerated.

- [x] post job 500 unexpected crud result. [`test_user.py::test_post_job_500_unexpected_crud_result`](test_user.py).
- [x] get jobs 500 unexpected crud result. [`test_user.py::test_get_jobs_500_unexpected_crud_result`](test_user.py).
- [x] get job 500 unexpected crud result. [`test_user.py::test_get_job_500_unexpected_crud_result`](test_user.py).
- [x] get job 404 disappeared after check. [`test_user.py::test_get_job_404_disappeared_after_check`](test_user.py).
- [x] get job 404 disappeared before images. [`test_user.py::test_get_job_404_disappeared_before_images`](test_user.py).
- [x] put image 500 unexpected crud result. [`test_user.py::test_put_image_500_unexpected_crud_result`](test_user.py).
- [x] put image 404 disappeared before install. [`test_user.py::test_put_image_404_disappeared_before_install`](test_user.py).
- [x] put alto 500 unexpected crud result. [`test_user.py::test_put_alto_500_unexpected_crud_result`](test_user.py).
- [x] put alto 404 disappeared before install. [`test_user.py::test_put_alto_404_disappeared_before_install`](test_user.py).
- [x] put page 500 unexpected crud result. [`test_user.py::test_put_page_500_unexpected_crud_result`](test_user.py).
- [x] put page 404 disappeared before install. [`test_user.py::test_put_page_404_disappeared_before_install`](test_user.py).
- [x] put metadata 500 unexpected crud result. [`test_user.py::test_put_metadata_500_unexpected_crud_result`](test_user.py).
- [x] put metadata 404 disappeared after check. [`test_user.py::test_put_metadata_404_disappeared_after_check`](test_user.py).
- [x] put metadata 404 disappeared before install. [`test_user.py::test_put_metadata_404_disappeared_before_install`](test_user.py).
- [x] patch job 404 disappeared after check. [`test_user.py::test_patch_job_404_disappeared_after_check`](test_user.py).
- [x] patch job 500 unexpected cancel result. [`test_user.py::test_patch_job_500_unexpected_cancel_result`](test_user.py).
- [x] patch job 404 disappeared before update. [`test_user.py::test_patch_job_404_disappeared_before_update_admin`](test_user.py).
- [x] get job result 404 disappeared after check. [`test_user.py::test_get_job_result_404_disappeared_after_check`](test_user.py).
- [x] get engines 500 unexpected crud result. [`test_user.py::test_get_engines_500_unexpected_crud_result`](test_user.py).
- [x] post job lease 500 unexpected crud result. [`test_worker.py::test_post_job_lease_500_unexpected_crud_result`](test_worker.py).
- [x] patch job lease 500 unexpected crud result. [`test_worker.py::test_patch_job_lease_500_unexpected_crud_result`](test_worker.py).
- [x] patch job lease 404 disappeared after check. [`test_worker.py::test_patch_job_lease_404_disappeared_after_check`](test_worker.py).
- [x] patch job lease 404 disappeared before update. [`test_worker.py::test_patch_job_lease_404_disappeared_before_update`](test_worker.py).
- [x] delete job lease 500 unexpected crud result. [`test_worker.py::test_delete_job_lease_500_unexpected_crud_result`](test_worker.py).
- [x] delete job lease 404 disappeared after check. [`test_worker.py::test_delete_job_lease_404_disappeared_after_check`](test_worker.py).
- [x] delete job lease 404 disappeared before update. [`test_worker.py::test_delete_job_lease_404_disappeared_before_update`](test_worker.py).
- [x] get image 500 unexpected crud result. [`test_worker.py::test_get_image_500_unexpected_crud_result`](test_worker.py).
- [x] get alto 500 unexpected crud result. [`test_worker.py::test_get_alto_500_unexpected_crud_result`](test_worker.py).
- [x] get page 500 unexpected crud result. [`test_worker.py::test_get_page_500_unexpected_crud_result`](test_worker.py).
- [x] get metadata 404 disappeared after check. [`test_worker.py::test_get_metadata_404_disappeared_after_check`](test_worker.py).
- [x] get engine files 500 unexpected crud result. [`test_worker.py::test_get_engine_files_500_unexpected_crud_result`](test_worker.py).
- [x] post job result 404 disappeared before install. [`test_worker.py::test_post_job_result_404_disappeared_before_install`](test_worker.py).
- [x] post job artifacts 404 disappeared before install. [`test_worker.py::test_post_job_artifacts_404_disappeared_before_install`](test_worker.py).
- [x] patch job 500 unexpected crud result. [`test_worker.py::test_patch_job_500_unexpected_crud_result`](test_worker.py).
- [x] patch job 404 disappeared before update. [`test_worker.py::test_patch_job_404_disappeared_before_update`](test_worker.py).
- [x] get keys 500 unexpected crud result. [`test_admin.py::test_get_keys_500_unexpected_crud_result`](test_admin.py).
- [x] post keys 500 unexpected crud result. [`test_admin.py::test_post_keys_500_unexpected_crud_result`](test_admin.py).
- [x] post key secret 500 unexpected crud result. [`test_admin.py::test_post_key_secret_500_unexpected_crud_result`](test_admin.py).
- [x] patch key 500 unexpected crud result. [`test_admin.py::test_patch_key_500_unexpected_crud_result`](test_admin.py).
- [x] post engine 500 unexpected crud result. [`test_admin.py::test_post_engine_500_unexpected_crud_result`](test_admin.py).
- [x] patch engine 500 unexpected crud result. [`test_admin.py::test_patch_engine_500_unexpected_crud_result`](test_admin.py).
- [x] put engine files 404 disappeared before install. [`test_admin.py::test_put_engine_files_404_disappeared_before_install`](test_admin.py).
- [x] put engine files 500 unexpected metadata result. [`test_admin.py::test_put_engine_files_500_unexpected_metadata_result`](test_admin.py).
- [x] patch job 404 disappeared before update. [`test_user.py::test_patch_job_404_disappeared_before_update_admin`](test_user.py).
- [x] patch job 500 unexpected crud result. [`test_user.py::test_patch_job_500_unexpected_crud_result`](test_user.py).
- [x] put job start 500 unexpected crud result. [`test_admin.py::test_put_job_start_500_unexpected_crud_result`](test_admin.py).
- [x] put job start 404 disappeared before update. [`test_admin.py::test_put_job_start_404_disappeared_before_update`](test_admin.py).
- [x] put job start 409 queued during request. [`test_admin.py::test_put_job_start_409_queued_during_request`](test_admin.py).

### Concurrent workers, retries, and interrupted requests

Four independent API identities send 48 concurrent lease requests against 32
eligible jobs plus exhausted and terminal jobs. The test checks unique
assignments, matching worker IDs, exactly one attempt increment, preserved retry
start times/logs, and exhaustion without leasing terminal jobs. It polls in
bounded waves because `SKIP LOCKED` can return an empty queue while another
transaction holds eligible jobs. This is bounded concurrency coverage, rather
than a throughput benchmark. The same scenario runs with admin identities.

Controlled pauses exercise reassignment and user cancellation between route
checks and the final database write. Lease renewal/release and job updates
refresh the job and recheck ownership/state under its row lock: old workers
receive 403 after reassignment; cancelled jobs reject updates with 409. Admins
retain their ownership override. Ownership distinguishes API keys, rather than
separate worker processes sharing the same key.

Cancellation before lease commit rolls back the assignment. Cancellation after
commit, or a disconnect simulated during ASGI response sending, retains the
assignment. A subsequent POST may lease another job; it does not recover the
lost response. The tests release the known original lease and verify it can be
leased again without consuming an extra attempt. These are application-level
request tests, rather than TCP reconnect tests.

An uncommitted ERROR cannot be leased. After it commits, the lease checker either requeues
it or permanently fails it at the attempt limit; lease requests only claim QUEUED jobs. Concurrent ERROR and FAILED
reports must leave the job permanently failed; an ERROR arriving after failure
is rejected without appending its log.

- [x] post job lease 200 many workers and retries. [`test_worker.py::test_post_job_lease_200_many_workers_and_retries`](test_worker.py).
- [x] post job lease cancelled before commit. [`test_worker.py::test_post_job_lease_cancelled_before_commit`](test_worker.py).
- [x] post job lease 200 lost response. [`test_worker.py::test_post_job_lease_200_lost_response`](test_worker.py).
- [x] patch job lease 403 reassignment during request. [`test_worker.py::test_patch_job_lease_403_reassignment_during_request`](test_worker.py).
- [x] patch job lease 409 cancellation during request. [`test_worker.py::test_patch_job_lease_409_cancellation_during_request`](test_worker.py).
- [x] delete job lease 403 reassignment during request. [`test_worker.py::test_delete_job_lease_403_reassignment_during_request`](test_worker.py).
- [x] delete job lease 409 cancellation during request. [`test_worker.py::test_delete_job_lease_409_cancellation_during_request`](test_worker.py).
- [x] patch job 403 reassignment during request. [`test_worker.py::test_patch_job_403_reassignment_during_request`](test_worker.py).
- [x] patch job 409 cancellation during request. [`test_worker.py::test_patch_job_409_cancellation_during_request`](test_worker.py).
- [x] patch job 200 error during lease poll. [`test_worker.py::test_patch_job_200_error_during_lease_poll`](test_worker.py).
- [x] patch job 200 409 concurrent error and failure. [`test_worker.py::test_patch_job_200_409_concurrent_error_and_failure`](test_worker.py).

### Generated payloads and validation combinations

Payloads are constructed at test runtime in `dummy_data.py`; no large binary
fixtures are stored in the repository. ALTO and PAGE cases cover Unicode,
UTF-16 declarations, 64 nested groups, XML above 1 MiB, malformed encodings,
truncated/empty XML, and rejected entity declarations. Every validation-toggle
subset is exercised against a document with an unsupported namespace and no
text structures; the enabled checks determine acceptance. These are basic
structural checks, rather than full XSD validation.

Real ZIP archives cover empty archives, 400 entries, Unicode entry names, an
empty directory, a deflated text entry, and an uncompressed 3 MiB entry.
Successful saves are downloaded and compared byte for byte. Truncated or damaged central directories are rejected
without changing previous files or DB records, followed by a successful retry.
Multipart cases cover missing boundaries, invalid bodies, wrong field names,
empty filenames, and text fields supplied instead of files. User and worker
cases also run with admin authentication. Invalid file fields retain their
validation messages and return 422; exception objects in validation details are
encoded as strings to avoid an internal serialization error.

- [x] put image 400 multipart. [`test_user.py::test_put_image_400_multipart`](test_user.py).
- [x] put image 422 multipart. [`test_user.py::test_put_image_422_multipart`](test_user.py).
- [x] put alto 201 generated xml. [`test_user.py::test_put_alto_201_generated_xml`](test_user.py).
- [x] put alto 400 generated xml. [`test_user.py::test_put_alto_400_generated_xml`](test_user.py).
- [x] put alto 201 validation combinations. [`test_user.py::test_put_alto_201_validation_combinations`](test_user.py).
- [x] put alto 422 validation combinations. [`test_user.py::test_put_alto_422_validation_combinations`](test_user.py).
- [x] put alto 400 multipart. [`test_user.py::test_put_alto_400_multipart`](test_user.py).
- [x] put alto 422 multipart. [`test_user.py::test_put_alto_422_multipart`](test_user.py).
- [x] put page 201 generated xml. [`test_user.py::test_put_page_201_generated_xml`](test_user.py).
- [x] put page 400 generated xml. [`test_user.py::test_put_page_400_generated_xml`](test_user.py).
- [x] put page 201 validation combinations. [`test_user.py::test_put_page_201_validation_combinations`](test_user.py).
- [x] put page 422 validation combinations. [`test_user.py::test_put_page_422_validation_combinations`](test_user.py).
- [x] put page 400 multipart. [`test_user.py::test_put_page_400_multipart`](test_user.py).
- [x] put page 422 multipart. [`test_user.py::test_put_page_422_multipart`](test_user.py).
- [x] post job result 200 generated zip. [`test_worker.py::test_post_job_result_200_generated_zip`](test_worker.py).
- [x] post job result 415 generated zip. [`test_worker.py::test_post_job_result_415_generated_zip`](test_worker.py).
- [x] post job result 400 multipart. [`test_worker.py::test_post_job_result_400_multipart`](test_worker.py).
- [x] post job result 422 multipart. [`test_worker.py::test_post_job_result_422_multipart`](test_worker.py).
- [x] post job artifacts 200 generated zip. [`test_worker.py::test_post_job_artifacts_200_generated_zip`](test_worker.py).
- [x] post job artifacts 415 generated zip. [`test_worker.py::test_post_job_artifacts_415_generated_zip`](test_worker.py).
- [x] post job artifacts 400 multipart. [`test_worker.py::test_post_job_artifacts_400_multipart`](test_worker.py).
- [x] post job artifacts 422 multipart. [`test_worker.py::test_post_job_artifacts_422_multipart`](test_worker.py).
- [x] put engine files 200 generated zip. [`test_admin.py::test_put_engine_files_200_generated_zip`](test_admin.py).
- [x] put engine files 415 generated zip. [`test_admin.py::test_put_engine_files_415_generated_zip`](test_admin.py).
- [x] put engine files 400 multipart. [`test_admin.py::test_put_engine_files_400_multipart`](test_admin.py).
- [x] put engine files 422 multipart. [`test_admin.py::test_put_engine_files_422_multipart`](test_admin.py).

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

- [x] Cleanup removes inputs, results, and artifacts before a download, after
  response headers, or after the first chunk. Before opening, deletion produces
  the usual missing-data response or aborts an already-started stream; an open
  file remains readable through completion on Linux. Subsequent requests report
  missing data, and deletion timestamps are committed. Input cases also cover
  optional image-record deletion. [`test_cleanup_concurrency.py::test_download_during_cleanup`](test_cleanup_concurrency.py).
- [x] Cleanup skips a terminal job while any of its six job-file save endpoints
  holds the final installation row lock, then removes its files and records the
  timestamp after the save commits. [`test_cleanup_concurrency.py::test_cleanup_skips_installing_file`](test_cleanup_concurrency.py).

### Application startup and launcher

Startup tests cover production/development credential settings, admin-key
creation and rotation, preserved/disabled existing keys, partial and malformed
credentials, and database read/commit failures. Task lifecycle tests verify
cleanup cancellation; launcher tests cover reload, resource settings and database setup failures.
These cases live in `test_startup.py` and `test_run.py`.

Resource defaults allow parallel transfers on a shared host: 64 concurrent HTTP connections/tasks,
64 pending connections, 60-second HTTP keep-alive, 30-second graceful shutdown,
and a database pool of 10 plus 10 overflow connections with a 10-second wait.
Deployers expecting higher load should configure these values accordingly.

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
- [x] HTTP keep-alive timeout defaults to 60 seconds, accepts nonnegative integer overrides, rejects invalid values and is passed to Uvicorn. [`test_run.py`](test_run.py).
- [x] HTTP concurrency/backlog/shutdown and database pool configuration defaults, overrides and validation; HTTP settings reach Uvicorn. [`test_run.py`](test_run.py).
- [x] Database pool size/overflow limits, waiting timeout and recovery after releasing a connection, using disposable PostgreSQL databases. [`test_startup.py::test_database_pool_limits_and_recovers_after_exhaustion`](test_startup.py).


### Job readiness and concurrent input saves

- [x] Failed replacement restores the original before releasing the job row lock. [`test_user.py::test_put_image_500_restore_before_readiness_unlock`](test_user.py).

- [x] queue ready jobs batches. [`test_job_readiness.py::test_queue_ready_jobs_batches`](test_job_readiness.py).
- [x] queue ready jobs only new. [`test_job_readiness.py::test_queue_ready_jobs_only_new`](test_job_readiness.py).
- [x] queue ready jobs required inputs. [`test_job_readiness.py::test_queue_ready_jobs_required_inputs`](test_job_readiness.py).
- [x] Queue only when image records match the retained image count, including reopened jobs after cleanup. [`test_job_readiness.py::test_queue_ready_jobs_requires_matching_image_count`](test_job_readiness.py).
- [x] queue ready jobs skips locked. [`test_job_readiness.py::test_queue_ready_jobs_skips_locked`](test_job_readiness.py).
- [x] readiness interval configuration. [`test_job_readiness.py::test_readiness_interval_configuration`](test_job_readiness.py).
- [x] readiness loop interval and retry. [`test_job_readiness.py::test_readiness_loop_interval_and_retry`](test_job_readiness.py).
- [x] readiness recovers jobs at startup. [`test_startup.py::test_readiness_recovers_jobs_at_startup`](test_startup.py).
- [x] readiness task lifecycle. [`test_startup.py::test_background_checker_lifecycle`](test_startup.py).
- [x] put inputs 409 conflicting saves; staging holds no DB connection. [`test_user.py::test_put_input_409_file_save_in_progress`](test_user.py).
- [x] put image 201 parallel input formats. [`test_user.py::test_put_image_201_parallel_input_formats`](test_user.py).
- [x] put image 409 state changed during staging. [`test_user.py::test_put_image_409_state_changed_during_staging`](test_user.py).

### Admin workflow bypasses

All authenticated endpoints accept active admin keys. Admins bypass ownership,
worker assignment, job state, input requirements, and stored upload
flags when downloading an existing file. Valid requests/files, existing records,
database integrity, and concurrency protection remain mandatory. The debug start
route remains a manual invocation of the normal readiness check; use PATCH to
force a state change.

`PATCH /v1/jobs/{job_id}` replaces the removed admin job PATCH route. Admins may
edit the full `JobUpdate` schema, including attempts, input flags, dates, progress,
and process/final logs. State updates return the corresponding operation success
code. Progress/log updates without a state change return `JOB_UPDATED` and renewed
lease data for both workers and admins. Other admin edits return `JOB_UPDATED`
without lease data. Progress must be a finite number in `[0, 1]` for every role; invalid values are rejected with 422. Final reports can be edited independently
of state. Process logs append by default; `append_logs=false` replaces only the
current attempt. Final logs always replace that attempt. Normal activity and
finalization defaults apply, while explicit admin field values take precedence.
User/worker bodies retain the narrower `JobProgressUpdate` validation, and workers
retain their existing transition checks, return codes, and lease payloads.

- [x] Every source/target state combination and final-report editing outside finalization; [`test_user.py::test_patch_job_200_admin_any_transition`](test_user.py).
- [x] Former admin job-edit tests moved to the shared route in [`test_user.py`](test_user.py), including explicit field overrides, sparse attempt logs, log replacement, deletion races, and notification behavior.
- [x] Optional input uploads and existing result downloads in every state, with another owner's job; [`test_user.py`](test_user.py).
- [x] Admin-only fields remain rejected for ordinary user/worker requests, and the old admin PATCH path is absent from routing and OpenAPI; [`test_user.py`](test_user.py).
- [x] Inactive engine selection by name/version/default, including names with multiple inactive versions; [`test_user.py::test_post_job_201_admin_inactive_engine`](test_user.py).
- [x] Lease operations, optional input downloads, and output uploads in every state without an assigned worker; [`test_worker.py`](test_worker.py).
- [x] Existing engine ZIP download without an upload timestamp; [`test_worker.py::test_get_engine_files_200_admin_without_upload_timestamp`](test_worker.py).

Workflow denial tests use ordinary roles only; corresponding admin bypass cases
are tested separately. Shared successful cases still run under admin credentials
where their behavior matches, with explicit assertions for role-specific responses.

Additional individual cases for the consolidated behavior:

- [x] adapter progress update admin and worker payload. [`test_adapter.py::test_adapter_progress_update_admin_and_worker_payload`](test_adapter.py).
- [x] get job result 200 admin any state. [`test_user.py::test_get_job_result_200_admin_any_state`](test_user.py).
- [x] patch job 200 force update. [`test_user.py::test_patch_job_200_force_update`](test_user.py).
- [x] patch job 404 disappeared before update. [`test_user.py::test_patch_job_404_disappeared_before_update`](test_user.py).
- [x] patch job 404 job not found admin. [`test_user.py::test_patch_job_404_job_not_found_admin`](test_user.py).
- [x] patch job 404 removed admin route. [`test_user.py::test_patch_job_404_removed_admin_route`](test_user.py).
- [x] patch job 422 admin fields forbidden to other roles. [`test_user.py::test_patch_job_422_admin_fields_forbidden_to_other_roles`](test_user.py).
- [x] patch job 422 invalid state. [`test_user.py::test_patch_job_422_invalid_state`](test_user.py).
- [x] put file 201 admin any state optional input. [`test_user.py::test_put_file_201_admin_any_state_optional_input`](test_user.py).
- [x] delete job lease 204 admin any state. [`test_worker.py::test_delete_job_lease_204_admin_any_state`](test_worker.py).
- [x] get engine files 410 admin missing file without timestamp. [`test_worker.py::test_get_engine_files_410_admin_missing_file_without_timestamp`](test_worker.py).
- [x] get input 200 admin any state optional input. [`test_worker.py::test_get_input_200_admin_any_state_optional_input`](test_worker.py).
- [x] patch job lease 200 admin any state. [`test_worker.py::test_patch_job_lease_200_admin_any_state`](test_worker.py).
- [x] post output 201 admin any state. [`test_worker.py::test_post_output_201_admin_any_state`](test_worker.py).

- [x] Admin progress updates return a lease consistent with the saved activity timestamp in every job state, with finite and infinite expiration. [`test_worker.py::test_patch_job_200_admin_progress_lease_any_state`](test_worker.py).
- [x] Admin process-log updates return lease data and respect explicit activity timestamps. [`test_worker.py::test_patch_job_200_admin_log_lease_explicit_timestamp`](test_worker.py).

### Explicit nulls in updates

Omitted nullable fields stay unchanged. Admin job edits can clear the attempt
counter, start/finish dates, and the current attempt’s four log entries using
`null`. Clearing logs preserves other attempts; an entirely empty history remains
SQL NULL. Worker progress logs keep their append behavior and worker final logs
keep their finalization rules: null messages do not clear existing text.
Engine definition and key email updates also distinguish omission from null.
The attempt counter accepts only null or an integer in `[0, 1000]`; booleans,
floats, strings, negative values, and values above 1000 are rejected before any writes.

- [x] patch engine 200 null definition. [`test_admin.py::test_patch_engine_200_null_definition`](test_admin.py).
- [x] patch key 200 null email and omission. [`test_admin.py::test_patch_key_200_null_email_and_omission`](test_admin.py).
- [x] patch job 200 admin attempt counter null and integer. [`test_user.py::test_patch_job_200_admin_attempt_counter_null_and_integer`](test_user.py).
- [x] patch job 200 admin null counter restarts first attempt. [`test_user.py::test_patch_job_200_admin_null_counter_restarts_first_attempt`](test_user.py).
- [x] patch job 200 admin null finished date overrides default. [`test_user.py::test_patch_job_200_admin_null_finished_date_overrides_default`](test_user.py).
- [x] patch job 200 admin null log current attempt. [`test_user.py::test_patch_job_200_admin_null_log_current_attempt`](test_user.py).
- [x] patch job 200 admin nullable dates. [`test_user.py::test_patch_job_200_admin_nullable_dates`](test_user.py).
- [x] patch job 422 admin invalid attempt counter. [`test_user.py::test_patch_job_422_admin_invalid_attempt_counter`](test_user.py).
- [x] patch job 200 worker null logs do not clear. [`test_worker.py::test_patch_job_200_worker_null_logs_do_not_clear`](test_worker.py).

### Numeric request validation

Request schemas validate progress as a finite number in `[0, 1]`, the attempt
counter as null or an integer in `[0, 1000]`, and image order as an integer in
`[0, 2147483647]`. Invalid values return 422 before writes. There is no silent
clamping. Validation errors containing NaN/infinity are rendered as valid JSON.
Arbitrary engine settings/definitions and metadata retain their own JSON data.

- [x] post job 201 image order bounds. [`test_user.py::test_post_job_201_image_order_bounds`](test_user.py).
- [x] post job 422 image order out of range or type. [`test_user.py::test_post_job_422_image_order_out_of_range_or_type`](test_user.py).
- [x] patch job 422 non finite progress. [`test_worker.py::test_patch_job_422_non_finite_progress`](test_worker.py).
- [x] patch job 422 progress out of range or type. [`test_worker.py::test_patch_job_422_progress_out_of_range_or_type`](test_worker.py).

### Date range validation

Administrative date edits and job-listing date filters share validation and UTC
normalization. Timezone-free dates mean UTC; explicit offsets preserve the instant.
Dates must remain representable in UTC. Activity timestamps in edits
must also leave room for the configured finite lease duration. Invalid dates
return 422 before writes. Exact Python minimum/maximum dates are rejected because
the PostgreSQL driver reserves them for infinity. Finite and infinite lease boundaries are covered.

- [x] UTC conversion overflow leaves the job unchanged. [`test_user.py::test_patch_job_422_admin_date_utc_overflow`](test_user.py).
- [x] Lease expiry overflow is rejected even for edits without progress. [`test_user.py::test_patch_job_422_admin_lease_date_overflow`](test_user.py).
- [x] Supported finite and infinite lease date boundaries. [`test_user.py::test_patch_job_200_admin_lease_date_boundary`](test_user.py).
- [x] Unsafe listing dates return 422 for all reader roles. [`test_user.py::test_get_jobs_422_date_range`](test_user.py).
- [x] Listing date filters do not impose lease-duration limits. [`test_user.py::test_get_jobs_200_date_filter_independent_of_lease`](test_user.py).
- [x] Administrative dates with offsets or without timezones are stored and returned in UTC. [`test_user.py::test_patch_job_200_admin_dates_normalized_to_utc`](test_user.py).


### Manager role

Dedicated management route tests run under both admin and manager authentication;
debug routes are tested as admin and reject managers.
Shared user/worker workflow tests also run as manager where applicable. Managers
retain cross-owner visibility and management access, while state, readiness,
input requirement, and lease-state checks remain enforced. Managers bypass
owner and worker assignment restrictions across all routes.

- [x] Role identity, access to management routes, and denial of debug routes. [`test_authentication.py::test_get_me_200_manager`](test_authentication.py), [`test_authentication.py::test_route_manager_authorized`](test_authentication.py).
- [x] Cross-owner job and full log visibility. [`test_user.py::test_get_jobs_200_manager_visibility`](test_user.py).
- [x] Input uploads respect job state even for another owner’s job. [`test_user.py::test_put_image_201_manager_other_owner`](test_user.py), [`test_user.py::test_put_image_409_manager_job_not_new`](test_user.py).
- [x] Administrative job fields remain unavailable. [`test_user.py::test_patch_job_422_manager_admin_fields`](test_user.py).
- [x] Cross-owner cancellation respects terminal states. [`test_user.py::test_patch_job_200_manager_cancel_other_owner`](test_user.py), [`test_user.py::test_patch_job_409_manager_cancel_terminal_job`](test_user.py).
- [x] Explicit versions still require active engines. [`test_user.py::test_post_job_201_manager_explicit_engine_version`](test_user.py), [`test_user.py::test_post_job_409_manager_inactive_engine_version`](test_user.py).
- [x] Worker operations can manage other workers’ jobs while respecting job state. [`test_worker.py::test_patch_job_200_manager_other_worker`](test_worker.py), [`test_worker.py::test_post_job_output_201_manager_other_worker`](test_worker.py), [`test_worker.py::test_delete_job_lease_204_manager_other_worker`](test_worker.py), [`test_worker.py::test_get_input_200_manager_other_worker`](test_worker.py).
- [x] Upgrade supports persisted manager keys; downgrade refuses until they are reassigned and then preserves keys. [`test_migrations.py::test_upgrade_manager_role`](test_migrations.py).
- [x] Null log values do not clear stored manager progress/final reports. [`test_worker.py::test_patch_job_200_manager_null_logs_preserved`](test_worker.py).

## Lightweight monitoring

`/monitor` serves a plain HTML/CSS/JavaScript login shell. Monitoring data requires
an active API key sent in `X-API-Key`; the page remembers it in browser
local storage until sign-out or authentication rejection. User and read-only keys see only
Processing, Finished and Settings, fixed to their own jobs. Admin and manager keys
retain full monitoring. Worker keys remain unsupported. Ownership is enforced before aggregation and pagination;
identity filters require management access. User responses omit key identities, and
only User processing and User final log tabs are available. Saved browser section
and log preferences cannot restore restricted content when switching accounts.
No frontend build, CDN, additional Python dependency or database migration is
required. The responsive layout supports automatic, desktop and mobile modes.

The Monitor Swagger section contains `/v1/monitor/info`, `/jobs/statistics`,
`/jobs/processing`, `/workers` and `/database`. Filter options reuse the existing
`/v1/admin/keys` and `/v1/engines` endpoints, including inactive keys and engines for management keys.
User and read-only engine/version options instead come from their unfiltered, owner-scoped
statistics, so engines referenced only by other users never appear. Refresh timing
is embedded in the public page shell, avoiding access to server information.
Routes authenticate and validate inputs;
`monitor_cruds.py` performs read-only queries without authentication decisions.
Summary queries exclude job definitions, logs and credentials. Processing shows all
PROCESSING jobs followed by all QUEUED jobs, oldest creation first in each group,
then NEW and recently finished jobs, newest creation first. The limit applies to the whole list, with standard limit/offset pagination (omit limit for all matching jobs). The response contains jobs plus PROCESSING/QUEUED/NEW totals across all matching records, and X-Total-Count/X-Has-More headers, from one database snapshot. DONE, FAILED and ERROR jobs also remain visible for a configurable finish-time window (60 seconds by default; zero disables it), independently of the page size. The browser provides a persistent Keep finished selector. Jobs uses
the standard `/v1/jobs` endpoint with label filters and supports server-side pagination and timestamp sorting, with total counts in response headers.
Finished combines totals above a paginated jobs table. Both requests share owner/worker label,
engine/version, multiple states and inclusive date filters. The default state selection is DONE and FAILED;
the selector can narrow it to either state. Period offers 24h (default), 7d, one calendar month or all time,
translated to from_finished_date by the browser. Totals cover every matching job regardless of the selected
page or sort order. The table sorts by finish time and shows finish timestamps and elapsed runtime.
Runtime spans first start to finish, including retry waits; missing or reversed timestamps show Unavailable.
Processing retains independent filters. Worker keys are excluded from owner options.

The default section order is Server, Processing, Workers, Finished, Database, Settings. Every section supports moving and collapsing; browser local
storage retains its position and collapsed state. Collapsed headers retain useful
summaries. When Finished is collapsed, aggregates continue to refresh the header; the jobs table and its logs skip automatic requests.
Settings contains the layout selector. Mobile uses a header drawer for links and
sign-out, while the global collapse/expand control stays in the header.

Job UUIDs show eight characters with the full value on hover. Opening a row's log
icon separately requests the existing job-details endpoint, then refreshes that
viewer's logs while open and visible. Closing it or removing its job from the table
stops those requests. Four tabs show User processing, Processing, User final and
Final; Final is the initial default and the browser remembers subsequent tab choices.
Empty, loading and failure messages are separate from actual log content. Dates
use year/month/day and 24-hour time.

The separate Monitoring configuration section defines:

- `MONITOR_REFRESH_INTERVAL_SECONDS=5`: browser refresh period; hidden tabs pause.
- `MONITOR_WORKER_OFFLINE_AFTER_SECONDS=60`: with no PROCESSING job, a recent
  `last_used` means idle, otherwise off. A PROCESSING assignment always means active,
  including disabled keys; key enablement is displayed separately. Multiple worker
  processes sharing a key appear as one worker. Both settings require positive integers.
- `MONITOR_HOSTNAME=`: optional display-only hostname override.
- `MONITOR_IP_ADDRESSES=`: optional comma-separated IPv4/IPv6 display addresses.
  Empty or unset overrides retain automatic process detection independently.
  An overridden hostname is not resolved to derive the addresses. In Docker,
  the deployer must explicitly supply and maintain outer host information if
  desired. These settings do not affect networking, binding, or public URLs.

Statistics aggregate every matching job before pagination using the standard jobs filters except result availability.
The four date lower bounds are inclusive and normalize timestamps to UTC; there is no
separate backend period parameter. Aggregate and table queries use the same filter selection.
Both endpoints accept repeated state parameters, for example state=done&state=failed.
Result availability uses DONE state and an unset results removal timestamp, without checking files; its filter is available only on the standard jobs endpoint.
DONE/FAILED totals count jobs, retained image counts and elapsed first-start-to-finish
time, including retry waits. Missing or reversed timestamps do not contribute runtime.
Attempts count all outcomes and in-progress attempts, excluding released leases;
image estimates multiply the job's image count by attempts. Worker filtering uses
the latest assignment; per-attempt runtime and historical attribution are unavailable.
Hostname, addresses and uptime refer to the serving API process/environment by default.
Only hostname and addresses can be overridden; uptime remains process uptime.

The Database section reads PostgreSQL's existing statistics in one SQL
statement per refresh: database size, client connection counts by state, longest
active query duration, connections waiting on locks, and deadlocks with their
statistics reset time. Only the application database is included, and the statistics
request's own connection is excluded. Idle-in-transaction counts include aborted
transactions. An absent active query displays as None; an unknown reset time as
Unavailable. No query text or connection identities are returned. Activity visibility
depends on PostgreSQL permissions and tracking settings; cross-role activity details
require a monitoring role such as `pg_read_all_stats`. No extension or migration is needed.

`test_monitor.py` covers rendering/static assets, admin/manager responses,
PostgreSQL statistics and filters, recent-job ordering/refresh, worker activity
thresholds and processing precedence, schema validation, configuration alignment,
and Monitor Swagger documentation. Owner-scope tests also cover empty data,
filters, totals, grouped identities, pagination and job-log access. The shared authentication matrix also covers
all eight JSON endpoints. Database checks include an actual lock wait and idle
transaction in a disposable database. Browser verification checks login rejection/success,
filters, desktop/mobile layouts without horizontal overflow, and clearing data
on sign-out.

Automated monitoring and presentation checks:

- [x] [`test_monitor.py::test_get_monitor_200`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_info_200`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_info_200_identity_overrides`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_database_200_idle`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_database_200_lock_wait`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_stats_200`](test_monitor.py).
- [x] Statistics match the standard jobs filters, including result files and pagination independence. [`test_monitor.py::test_get_monitor_stats_matches_jobs`](test_monitor.py).
- [x] Matching OpenAPI filter contracts. [`test_monitor.py::test_get_monitor_stats_filter_contract`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_stats_200_filters`](test_monitor.py).
- [x] [`test_monitor.py::test_monitor_attempts_exclude_released_leases`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_engine_filters`](test_monitor.py).
- [x] [`test_monitor.py::test_jobs_list_200_limit_and_refresh`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_jobs_removed`](test_monitor.py).
- [x] [`test_monitor.py::test_recent_activity_includes_all_pending`](test_monitor.py).
- [x] [`test_monitor.py::test_processing_finished_grace_period`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_jobs_owner_worker_filters`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_workers_200_activity`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_workers_excludes_management_assignments`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_workers_enabled_last_seen_order`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_workers_200_processing_always_active`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_422`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_page_200`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_openapi_200`](test_monitor.py).
- [x] [`test_monitor.py::test_monitor_configuration_invalid`](test_monitor.py).
- [x] [`test_monitor.py::test_monitor_configuration_matches_env`](test_monitor.py).
- [x] [`test_monitor.py::test_monitor_identity_configuration`](test_monitor.py).
- [x] [`test_monitor.py::test_monitor_ip_configuration_invalid`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_stats_200_group_partitions`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_stats_200_group_ranking`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_stats_422_group_options`](test_monitor.py).
- [x] [`test_dummy_database_seed.py::test_seed_dummy_database_finished_dataset`](test_dummy_database_seed.py).
- [x] [`test_dummy_database_seed.py::test_seed_dummy_database_credentials`](test_dummy_database_seed.py).
- [x] [`test_monitor.py::test_get_monitor_stats_200_finished_date_filter`](test_monitor.py).
- [x] [`test_monitor.py::test_get_monitor_stats_200_unavailable_runtime`](test_monitor.py).
- [x] [`test_presentation.py::test_monitor_commit_badge`](test_presentation.py).
- [x] [`test_presentation.py::test_monitor_version_release_link`](test_presentation.py).
- [x] [`test_presentation.py::test_swagger_root_path_and_version`](test_presentation.py).
- [x] [`test_presentation.py::test_native_swagger_description`](test_presentation.py).
- [x] [`test_presentation.py::test_native_swagger_contact`](test_presentation.py).

Additional existing configuration checks:

- [x] [`test_job_mail.py::test_default_subjects`](test_job_mail.py).
- [x] [`test_job_mail.py::test_job_mail_passes_smtp_security_and_username`](test_job_mail.py).
- [x] [`test_job_mail.py::test_mail_logger_passes_smtp_security_and_username`](test_job_mail.py).
- [x] [`test_job_mail.py::test_mail_timeout_must_be_finite_positive`](test_job_mail.py).
- [x] [`test_run.py::test_http_keep_alive_timeout_configuration`](test_run.py).
- [x] [`test_run.py::test_http_keep_alive_timeout_rejects_invalid_values`](test_run.py).
- [x] [`test_run.py::test_integer_resource_settings_reject_fractions`](test_run.py).
- [x] [`test_run.py::test_positive_resource_settings_reject_zero`](test_run.py).
- [x] [`test_run.py::test_resource_setting_defaults_and_overrides`](test_run.py).
- [x] [`test_run.py::test_resource_settings_accept_explicit_zero`](test_run.py).
- [x] [`test_run.py::test_resource_settings_reject_invalid_values`](test_run.py).
- [x] [`test_run.py::test_runner_passes_http_keep_alive_timeout`](test_run.py).
- [x] [`test_run.py::test_runner_passes_resource_limits`](test_run.py).

Processing provides Jobs/Images measures and an optional PROCESSING, QUEUED or NEW state filter.
The default All relevant selection includes the usual pending jobs and recently finished jobs.
Both job and image totals cover all filtered matches before pagination. Finished also labels its
default DONE/FAILED selection All relevant. These controls persist across browser refresh.
