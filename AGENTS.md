# Working on DocAPI

Rules for coding agents and contributors. CI only builds releases; nothing
checks these rules or runs the tests except whoever makes the change.

## Workflow

- Every change starts from a GitHub issue with the problem, a `## Change` and a
  `## Done when` section. Keep the issue up to date when the plan changes.
- Work on a branch `issue-<number>-<short-slug>` made from an up-to-date `main`.
- Commit, push and open pull requests only when the maintainer asks.
- A pull request starts with `Closes #<number>`, then `## Changes` and
  `## Tests`. The Tests section says what was run and what was not.

## Commits

- An imperative subject line, then a wrapped body that says what changed and why.
- End with `Closes #<number>`, or `Part of #<number>` for one of several commits.
- No AI-attribution trailers such as `Co-Authored-By` or session links.
- Stage files by name. Never commit `.env`, `compose.yml` or other local setup.

## Tests

Use Python 3.12 or newer, as the package and the Docker image do. Install the
pinned server versions, then the package and its test tools:

```sh
python -m pip install -r doc_api/requirements.txt -e '.[server,dev]'
```

This repository develops and tests with the versions in
`doc_api/requirements.txt`; see Dependencies below.

Database tests need `TEST_DATABASE_URL`: a disposable PostgreSQL whose user may
create databases. Each test makes and drops its own database. Without it, those
tests are skipped. See `doc_api/tests/README.md` for details.

```sh
export TEST_DATABASE_URL='postgresql+asyncpg://test_user:password@localhost:5432/postgres'
python -m pytest doc_worker/tests doc_api/tests/test_job_mail.py   # the files a change affects
python -m pytest -n 8 doc_api/tests doc_worker/tests               # before a release
```

- For a change and its pull request, run only the test files the change
  affects. Run the full suite before every new release, or when asked.
- Every test in `doc_api/tests` is listed in `doc_api/tests/README.md`, and
  `test_readme_references_current_tests` fails otherwise. Update the README
  together with the tests.
- `doc_api/config.py` and `doc_api/.env_example` mirror each other: the same
  sections with the same headings, the same settings in the same order, under
  the same names and with the same defaults. Add, rename, move or remove a
  setting in both at once. `doc_api/tests/test_config.py` checks the names,
  order and defaults.
- Report test results as they are, including failures that `main` has as well.

## Dependencies

DocAPI is first of all the API, and its versions are set in two places with
different jobs:

- `doc_api/requirements.txt` pins every package the server runs, exactly. The
  Docker image installs it, and this repository develops and tests with it.
  Updating a dependency means changing it here and running the full suite.
- `pyproject.toml` names the packages without versions. Other projects use
  DocAPI's worker and client (`doc-api[worker]`, `doc-api[client]`), often as
  a Git submodule, next to their own pinned libraries; choosing the versions
  is up to them, and a version here could make their install unresolvable.
  Only `requires-python` is set, to what the shared code needs and the image
  runs.
- OpenCV is a dependency of the extras, not of the base: the `server` extra
  takes `opencv-python-headless`, as the image does, and the `worker` and
  `client` extras take `opencv-python`. Both builds install the same `cv2`
  module, so one environment must not get both; `all` therefore takes only the
  server's.

`test_dependency_versions.py` checks that `pyproject.toml` pins no versions
and that the image runs the Python that `requires-python` names.

## No real side effects

- Tests never send email. The repository-root `conftest.py` removes `*_MAIL_*`
  and `MAIL_*` settings and fails any test that opens an SMTP connection. Use
  the `local_smtp` fixture for a real exchange with a local server.
- Do not connect to production servers, SMTP relays or deployments without the
  maintainer's explicit go-ahead.

## Code boundaries

- `doc_api.tools` holds general tools that DocAPI uses and that other projects
  may use on their own, such as `mail_sender` and `secrets`. Write them for any
  caller: they import nothing from the rest of `doc_api`, take everything they
  need as arguments, and know nothing of DocAPI's jobs, routes, settings or
  names. Code that only makes sense inside DocAPI belongs elsewhere.
- Routes and their cruds have separate jobs, and the two are never mixed.
  - Routes (`doc_api/api/routes/*_routes.py`, with the access guards in
    `doc_api/api/guards`) handle authentication, roles and the request's
    higher-level logic. A route may query the database itself where that fits.
  - Cruds (`doc_api/api/cruds/*_cruds.py`, one beside each routes file) carry
    out the database operations: queries, locks and transactions. They never
    decide who may do what or look at the caller's role. Where an operation
    has to hold under a lock, the route passes the precondition in, such as
    the key that must own the job, and the crud only checks it.
- All Pydantic schemas live in `doc_api/api/schemas/base_objects.py`. The only
  exception is `doc_api/api/schemas/responses.py`, which holds the response
  envelopes and `AppCode`.
  - `base_objects.py` is shared with the worker and the client, so a schema
    never checks anything against the server settings; the route does that.
  - Place a new class among the related schemas, not at the end of the file.
  - Place a new attribute by its significance among the existing ones, not
    necessarily last.
- Work that runs in the background, independent of API requests, is a checker
  in `doc_api/api/checkers/`, like the existing lease, readiness, job mail and
  cleanup checkers. Any new background work becomes another checker, not a task
  started from a route or a loop elsewhere.
  - A `check_*` function does one pass. It locks the rows it changes with
    `skip_locked`, so several API processes can run it side by side.
  - A `run_*` loop repeats the pass at an interval from a `*_INTERVAL_SECONDS`
    setting. It logs a failed pass and tries again at the next interval.
  - The loop is started in the `lifespan` of `doc_api/api/main.py` and is
    cancelled at shutdown.
- `doc_api.tools`, `doc_worker` and `doc_client` must not import the server
  configuration (`doc_api.config`) or its masking settings; the server passes
  what they need.
- Server settings are read from the environment in `doc_api/config.py`,
  validated at startup and documented in `doc_api/.env_example` (see Tests).
  The API documentation texts are documented in `doc_api/.env_docs_example`,
  which follows `.env_example` in `config.py`'s order.
  `doc_api/.env_docker_example` holds only the extra settings Compose reads for
  `doc_api/compose.yml`, the database settings; Docker deployments append it
  to `.env`.
  `config.py` holds only values and imports only the standard library. Objects
  built from the settings and shared by the server, such as the secret masker
  and the mail senders, are built once at startup in `doc_api/api/services.py`.

## Documenting routes

Every route documents each response it can return, in the same style as the
existing routes (e.g. `post_lease` in `doc_api/api/routes/worker_routes.py`):

- **A response dict:** directly above the route, a dict named after it (e.g.
  `POST_LEASE_RESPONSES`) has one entry per `AppCode` the route returns. Each
  entry has:
  - the `status` and a `description` for the API docs;
  - the response `model`, plus `model_data` when the response carries data;
  - the `detail` text the route sends;
  - for file responses, the `content_type` and an `example_value`.
- **Shared codes:** a code that means the same in several routes is documented
  once in `GENERAL_RESPONSES` (`doc_api/api/schemas/responses.py`). The route's
  dict refers to that entry (`AppCode.JOB_NOT_FOUND:
  GENERAL_RESPONSES[AppCode.JOB_NOT_FOUND]`) instead of repeating it. Add a
  code there only when several routes share it.
- **The decorator** sets `summary`, `description`, `tags`, `response_model`,
  the `x-order` in `openapi_extra`, and `responses=make_responses(<dict>)`.
- **The route** answers with the `detail` from its dict for each code. A code
  that is not in its dict is raised as `RouteInvariantError`.
- **Building the response:** use the prepared response types and functions
  from `doc_api/api/schemas/responses.py`, as the existing routes do.
  - **Success:** build a `DocAPIResponseOK[...]` with the status, the code, the
    `detail` from the route's dict and any data. Return it as it is for a 200,
    which FastAPI checks against `response_model`. Pass it through
    `validate_ok_response()` for other statuses or to leave fields out.
  - **Client errors:** raise `DocAPIClientErrorException(status=..., code=...,
    detail=...)`. The app's handler renders it with
    `validate_client_error_response()`. Never build an error `JSONResponse` by
    hand.
  - **Unexpected outcomes:** a code the route does not expect raises
    `RouteInvariantError`, and unhandled errors reach the generic 500 handler
    (`validate_server_error_response()`).
  - **Non-JSON content** (files, the monitor page, redirects) is the only place
    for `FileResponse` and similar classes.
- **Tell the maintainer first:**
  - when a response cannot be built with these helpers, describing the
    workaround, before writing it;
  - when a change adds a new `AppCode`, giving its name, status and meaning.
    Reuse an existing code where one fits.
- **Generated responses:** the authentication and role responses, the access
  guards' responses and request validation (422) are added to the OpenAPI
  automatically from the route's dependencies and guards. Do not repeat them in
  the dict.
- **Tests:** every route also has its cases in `ROUTE_CASES`
  (`doc_api/tests/helpers.py`), and `test_registered_routes_have_test_cases`
  fails otherwise.

## Names and writing

- Examples and tests use neutral names such as `example.org` and `myapp`.
  Issues, pull requests, code and docs do not name downstream projects or
  specific deployments.
- Issues, pull requests and docstrings say what the code does and why, in plain
  language. Call out behaviour changes and renamed or breaking settings in the
  pull request.
