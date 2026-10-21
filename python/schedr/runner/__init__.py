"""The Python runner: ``<interpreter> -m schedr.runner {parse,task}``.

The worker agent starts it as a subprocess and talks to it over the Unix socket named by
``SCHEDR_AGENT_SOCKET`` (see PROJECT.md, Rust/Python integration). The runner never decides
retries, enforces timeouts, watches memory or writes log files: the agent does all that from
outside.
"""
