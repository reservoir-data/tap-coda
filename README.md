# `tap-coda`

Coda tap class.

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets.

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`

## Settings

| Setting    | Required | Default | Description |
|:-----------|:--------:|:-------:|:------------|
| auth_token | True     | None    | The token to authenticate against the API service. |

A full list of supported settings and capabilities is available by running: `tap-coda --about`

### Source Authentication and Authorization

See the documentation: https://coda.io/developers/apis/v1#section/Authentication.

## Usage

You can easily run `tap-coda` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-coda --version
tap-coda --help
tap-coda --config CONFIG --discover > ./catalog.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_coda/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-coda` CLI interface directly using `poetry run`:

```bash
poetry run tap-coda --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-coda
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-coda --version
# OR run a test `elt` pipeline:
meltano elt tap-coda target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
