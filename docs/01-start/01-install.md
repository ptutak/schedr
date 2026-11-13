# Install

schedr needs **Python 3.14** or newer. Install it into a virtual environment in a project
directory of its own; schedr finds that environment by itself later.

```sh
mkdir my-schedr && cd my-schedr
python3.14 -m venv .venv
.venv/bin/pip install schedr          # add [pydantic] for Pydantic models
```

With [uv](https://docs.astral.sh/uv/) the same is `uv venv --python 3.14` and
`uv pip install schedr`.

The package carries the `schedr` binary, so there is nothing to compile. Wheels exist for
Linux x86_64 (glibc and musl), Linux aarch64 and macOS arm64. Check it:

```sh
.venv/bin/schedr --version
```

## Images

For Kubernetes or Docker, the images are on `ghcr.io`, x86_64:

| Image | What it has | For |
|---|---|---|
| `ghcr.io/ptutak/schedr` | Python, schedr, the task libraries and connection clients | standalone, workers, the base for your own image |
| `ghcr.io/ptutak/schedr-slim` | the same without the task libraries and connection clients | a smaller base |
| `ghcr.io/ptutak/schedr-server` | the binary alone | the master, the webserver and the UI |

Each is built on Debian (glibc), tagged with the version (`1.0.0`) and its series (`1.0`);
the same on Alpine adds `-alpine` (`1.0.0-alpine`). There is no `latest`: a deployment names the
version it runs. [Deployment](../03-guide/10-deployment.md) covers the Helm chart.
