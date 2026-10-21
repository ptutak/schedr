"""The Python runner: ``<interpreter> -m schedr.runner {parse,task}``.

The worker agent starts it as a subprocess and talks to it over a socket it hands over
already connected (its descriptor in ``SCHEDR_AGENT_FD``; see PROJECT.md, Rust/Python
integration). The runner never decides
retries, enforces timeouts, watches memory or writes log files: the agent does all that from
outside.
"""
