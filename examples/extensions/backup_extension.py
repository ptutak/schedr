"""A shell script as an extension's plugin (M5.2i6, decision 519): the backup extension.

Its plugin is ``examples/extensions/backup/backup.sh``, run by ``schedr extension shell``;
``make standalone`` configures it (``[extensions.backup]``, its metric and panel declared there)
and sets its secret, ``backup/target``. The script reads the payload on its stdin and reports
through ``schedr task-ctx``: a status, a link, a metric point, a cleanup handle, its output.
"""

from schedr import Dag, ExtensionTask

with Dag("extension_shell_backup", description="A backup run by a shell script's plugin."):
    ExtensionTask(
        "nightly",
        name="backup",
        version="1.0",
        payload={"source": "/data"},
        secrets=["backup/target"],
        cleanup=True,
    )

with Dag("extension_shell_failing", description="The shell plugin failing: exit 3."):
    ExtensionTask(
        "nightly",
        name="backup",
        version="1.0",
        payload={"fail": True},
        secrets=["backup/target"],
        cleanup=True,
    )
