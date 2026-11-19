# Writing a DocAPI worker

A worker is a subclass of `DocWorkerWrapper` (`doc_worker_wrapper.py`). The
wrapper does everything around a job:

- asks DocAPI for a job and leases it;
- downloads its images, ALTO, PAGE, meta JSON and engine files;
- zips and uploads the results, and reports the outcome;
- mails its own failures, DocAPI outages and ALIVE reports, when mail is set up.

The subclass implements only the processing. `dummy_worker.py` is a complete
example, which copies the inputs to the results.

## Processing a job

Implement `process_job`. The wrapper calls it with keyword arguments, once per
job:

```python
from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse

class MyWorker(DocWorkerWrapper):
    def process_job(self, job, job_log_file_handler, images_dir, result_dir,
                    alto_dir=None, page_xml_dir=None, meta_file=None, engine_dir=None):
        ...  # read the inputs, write the results into result_dir
        return WorkerResponse.ok()
```

- **Inputs and results:**
  - `images_dir` holds the job's images.
  - `alto_dir`, `page_xml_dir`, `meta_file` and `engine_dir` are `None` when the
    job has no such input.
  - Everything written to `result_dir` is zipped and uploaded as the result.
- **Logging:** `job_log_file_handler` writes the job's `worker.log`. Your own
  logging gets there only as described in
  [Logging into the job's log](#logging-into-the-jobs-log).
- **The outcome:** return one of these.
  - `WorkerResponse.ok()`: the job is done.
  - `WorkerResponse.error(final_log, final_log_user)`: a retryable error.
  - `WorkerResponse.fail(final_log, final_log_user)`: a permanent failure.

  `final_log` is the technical report for admins, and `final_log_user` the
  report for the job's owner.

  If `process_job` raises an exception, the wrapper reports a retryable error
  for you. The job gets the texts of the `ERROR_PROCESS_JOB_LOG` and
  `ERROR_PROCESS_JOB_LOG_USER` settings, by default "Processing failed due to
  an internal error". The exception and its traceback are logged and appended
  to the technical report, never to the owner's. Return `fail` yourself when
  retrying cannot help, and give your own reports. Pass `exception=` to
  `error` or `fail` to get its traceback into the technical report too.
- **After each job:** to act once every leased job is done (log or mail a
  summary, say), override `on_job_finished`. It is called whether the job
  succeeded or failed.

## Logging into the job's log

Each job has its own log file, `worker.log` in the job's directory. The wrapper
writes only its own messages there. To add those of your code and its
libraries, attach `job_log_file_handler` to the root logger while the job is
processed:

```python
import logging


class ThumbnailWorker(DocWorkerWrapper):
    def process_job(self, job, job_log_file_handler, images_dir, result_dir, **other_inputs):
        root_logger = logging.getLogger()
        root_logger.addHandler(job_log_file_handler)
        try:
            ...  # everything logged here goes into the job's log
            return WorkerResponse.ok()
        finally:
            root_logger.removeHandler(job_log_file_handler)
```

- **Always remove it in the `finally`.** The wrapper closes the handler after
  the job, but a closed handler that is still attached opens the file again,
  and later messages would land in this job's log.
- **Set the root level** to what the log should hold, e.g.
  `logging.basicConfig(level=logging.INFO)`. The default, WARNING, drops INFO
  and DEBUG messages before any handler sees them.

Everything logged while the job is processed goes into its log, including the
messages of other threads, such as the rare ones of the health mail. Messages
the wrapper itself logs in that time appear twice, because they reach the
handler directly and through the root.

## Configuring and starting it

Settings come from the environment through `config.py`. `.env_example` lists
every setting with its default. `from_config` builds the worker from a
checked `Config`, including the connector, the mail sender and the health
mail:

```python
from doc_worker.config import Config

MyWorker.from_config(Config()).start()
```

- **Overriding settings:** `Config({"POLLING_INTERVAL": "1"})` replaces the
  environment variable, e.g. from a command-line argument. Overrides are text,
  as in the environment, and an empty value means unset. An unknown name is an
  error. See `build_config` in `dummy_worker.py`.
- **Building it without the config:** you can also pass the constructor's
  arguments to the class directly.

## Settings of your own

Extend `Config` with your settings. Read them in `read()` and check them in
`check()`, each calling `super()` first. Your settings then take overrides and
are checked like the wrapper's.

`from_config` passes each of your settings to the constructor parameter of the
same name in lower case: `THUMBNAIL_MODEL_PATH` to `thumbnail_model_path`. A
setting with no such parameter is an error. The settings of `Config` itself go
only to the wrapper.

## Example

A worker that makes a thumbnail of each image of a job with a model.
`my_models.ThumbnailModel` stands for your model library. The settings of its
own are the model file, `THUMBNAIL_MODEL_PATH`, and the size and format of the
thumbnails, `THUMBNAIL_SIZE` and `THUMBNAIL_FORMAT`. `from_config` passes the
path to the constructor, which loads the model:

```python
import os

import cv2
from my_models import ThumbnailModel  # your model library

from doc_worker.config import Config
from doc_worker.doc_worker_wrapper import DocWorkerWrapper, WorkerResponse


class ThumbnailConfig(Config):
    def read(self):
        super().read()
        # The model file that makes the thumbnails. Required.
        self.THUMBNAIL_MODEL_PATH = self._str("THUMBNAIL_MODEL_PATH", None)
        # The longest side of a thumbnail, in pixels.
        self.THUMBNAIL_SIZE = self._nonnegative_int("THUMBNAIL_SIZE", 256)
        # The file format of the thumbnails: jpg or png.
        self.THUMBNAIL_FORMAT = self._str("THUMBNAIL_FORMAT", "jpg")

    def check(self):
        super().check()
        if not self.THUMBNAIL_MODEL_PATH or not os.path.isfile(self.THUMBNAIL_MODEL_PATH):
            raise ValueError("THUMBNAIL_MODEL_PATH must name an existing model file")
        if self.THUMBNAIL_SIZE < 16:
            raise ValueError("THUMBNAIL_SIZE must be at least 16")
        if self.THUMBNAIL_FORMAT not in ("jpg", "png"):
            raise ValueError("THUMBNAIL_FORMAT must be jpg or png")


class ThumbnailWorker(DocWorkerWrapper):
    """Makes a thumbnail of each image of a job with a model."""

    def __init__(self, *args, thumbnail_model_path, thumbnail_size, thumbnail_format, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = ThumbnailModel.load(thumbnail_model_path)
        self.thumbnail_size = thumbnail_size
        self.thumbnail_format = thumbnail_format

    def process_job(self, job, job_log_file_handler, images_dir, result_dir, **other_inputs):
        for name in sorted(os.listdir(images_dir)):
            image = cv2.imread(os.path.join(images_dir, name))
            if image is None:
                return WorkerResponse.fail(f"cv2 cannot read {name}", f"The image {name} cannot be read")
            thumbnail = self.model.thumbnail(image, self.thumbnail_size)
            stem = os.path.splitext(name)[0]
            cv2.imwrite(os.path.join(result_dir, f"{stem}.{self.thumbnail_format}"), thumbnail)
        return WorkerResponse.ok()


if __name__ == "__main__":
    ThumbnailWorker.from_config(ThumbnailConfig()).start()
```

```sh
API_URL=https://docapi.example.org WORKER_KEY_FILE=worker.key BASE_DIR=data \
THUMBNAIL_MODEL_PATH=thumbnails.model THUMBNAIL_SIZE=512 python thumbnail_worker.py
```

## Building something before the worker exists

The constructor can instead take a model that is already loaded. Other code
and tests can then pass a model of their own, such as a small or fake one,
without a model file. `from_config` then has to load the model first: override
`subclass_args` to build it from the settings. The override returns all of
your own arguments itself, as nothing is passed automatically any more, and the
wrapper's arguments are still built as usual.

The config and `process_job` stay as above:

```python
class ThumbnailWorker(DocWorkerWrapper):
    """Makes a thumbnail of each image of a job with a model it is given."""

    def __init__(self, *args, model, thumbnail_size, thumbnail_format, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = model
        self.thumbnail_size = thumbnail_size
        self.thumbnail_format = thumbnail_format

    @classmethod
    def subclass_args(cls, config):
        return {"model": ThumbnailModel.load(config.THUMBNAIL_MODEL_PATH),
                "thumbnail_size": config.THUMBNAIL_SIZE,
                "thumbnail_format": config.THUMBNAIL_FORMAT}
```

## Tests

Tests never send mail: the repository's `conftest.py` removes the `MAIL_*`
settings and blocks SMTP connections (see `AGENTS.md`).
