"""Secrets in the DAG body: ``secrets['etl/db#password']`` and ``SecretPrefix(...)``.

A secret is named, never read, in the DAG file: ``secrets['path#key']`` is a placeholder,
filled in by the worker agent that runs the task (in an argument, a t-string or ``env``), and
``secrets=[...]`` declares the secrets a task reads on demand (``schedr task-ctx secret get``,
``task_ctx.secret(...)``). See PROJECT.md, Secret storage.
"""

from collections.abc import Iterable
from typing import Final

from schedr._model import SecretDecl, SecretPrefixDecl, SecretRef
from schedr._model.values import storage_label

_VALUE = (
    "is a placeholder, filled in by the worker that runs the task: pass it to a task (an "
    "argument, a t-string, `env=`), or declare it in `secrets=` and read it inside the task "
    "with `task_ctx.secret(...)`"
)


def parse_secret(spec: object, storage: str | None) -> SecretRef:
    """``path#key`` (or ``path``, the whole secret) as a secret of ``storage``.

    Raises:
        TypeError: For a spec that isn't text.
        ValueError: For a bad path or key, or a path ending in ``/`` (a prefix).
    """
    if not isinstance(spec, str):
        raise TypeError(
            f"`{storage_label(storage)}[{spec!r}]`: a secret is named by text, `'path#key'`"
        )
    path, sep, key = spec.partition("#")
    if path.endswith("/") and len(path) > 1:
        raise ValueError(
            f"`{storage_label(storage)}[{spec!r}]`: a path doesn't end with `/`; for every "
            f"secret under it, declare `SecretPrefix({path!r})` in `secrets=`"
        )
    return SecretRef(path, key if sep else None, storage)


class Secret:
    """A secret named in the DAG body, filled in at run time: ``secrets['etl/db#password']``."""

    __slots__ = ("_ref",)

    def __init__(self, ref: SecretRef) -> None:
        """Wraps the secret's name.

        Args:
            ref: Which secret.
        """
        self._ref = ref

    def _schedr_reference(self) -> SecretRef:
        return self._ref

    def __repr__(self) -> str:
        """As written: ``secrets['etl/db#password']`` (never a value: it has none here)."""
        return self._ref.label()

    def __str__(self) -> str:
        """Refuses to be formatted into a plain string.

        Raises:
            TypeError: Always.
        """
        raise TypeError(f"`{self._ref.label()}` {_VALUE}")

    def __bool__(self) -> bool:
        """Refuses to be used as a value.

        Raises:
            TypeError: Always.
        """
        raise TypeError(f"`{self._ref.label()}` {_VALUE}")

    def __eq__(self, other: object) -> bool:
        """Two placeholders for the same secret are equal; nothing else compares.

        Raises:
            TypeError: Against anything but a placeholder (the value is known only at run
                time).
        """
        if isinstance(other, Secret):
            return self._ref == other._ref
        raise TypeError(f"`{self._ref.label()}` {_VALUE}")

    def __hash__(self) -> int:
        """Hashed by the secret it names."""
        return hash(self._ref)


class Storage:
    """One secret storage: ``secrets.other['path#key']``."""

    __slots__ = ("_name",)

    def __init__(self, name: str | None) -> None:
        """A storage by name; ``None`` for the default one.

        Args:
            name: Its ``[secret_storage.<name>]`` key.
        """
        self._name = name

    def __getitem__(self, spec: str) -> Secret:
        """The secret ``path#key`` (``path``: the whole secret, every key).

        Args:
            spec: ``path#key`` or ``path``.

        Returns:
            Its placeholder.
        """
        return Secret(parse_secret(spec, self._name))

    def __repr__(self) -> str:
        """As written."""
        return storage_label(self._name)


class Secrets(Storage):
    """The default storage, ``secrets['path#key']``, and the others by name."""

    __slots__ = ()

    def __init__(self) -> None:
        """The default storage."""
        super().__init__(None)

    def __getattr__(self, name: str) -> Storage:
        """``secrets.other``: the storage ``[secret_storage.other]``.

        Raises:
            AttributeError: For a dunder or private name (copy, pickle and type checks ask).
        """
        if name.startswith("_"):
            raise AttributeError(name)
        return self.storage(name)

    def storage(self, name: str) -> Storage:
        """A storage by name, also one that isn't a Python name: ``secrets.storage('v-2')``.

        Args:
            name: Its ``[secret_storage.<name>]`` key.

        Returns:
            The storage.

        Raises:
            ValueError: For a name a storage can't have.
        """
        SecretRef("x", None, name)  # checks the name
        return Storage(name)


secrets: Final = Secrets()
"""Secrets from secret storage: ``secrets['etl/db#password']`` (the default storage),
``secrets.other['etl/db#password']`` (the storage ``[secret_storage.other]``)."""


class SecretPrefix:
    """Every secret under a prefix, for ``secrets=``: ``SecretPrefix("connections/")``.

    Secrets under it are fetched when the task reads them, so a task can choose them at run
    time (e.g. one per tenant).
    """

    __slots__ = ("_decl",)

    def __init__(self, prefix: str, *, storage: str | None = None) -> None:
        """A prefix of a storage.

        Args:
            prefix: The prefix; a missing last ``/`` is added (``"tenants"`` is
                ``"tenants/"``, never ``"tenants_old/..."``).
            storage: The storage; ``None`` for the default one.

        Raises:
            TypeError: For a prefix that isn't text.
            ValueError: For a bad prefix or storage name.
        """
        if not isinstance(prefix, str):
            raise TypeError(f"SecretPrefix({prefix!r}): a prefix is text")
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        self._decl = SecretPrefixDecl(prefix, storage)

    def __repr__(self) -> str:
        """As written."""
        return self._decl.label()

    def __eq__(self, other: object) -> bool:
        """The same prefix of the same storage."""
        return isinstance(other, SecretPrefix) and self._decl == other._decl

    def __hash__(self) -> int:
        """Hashed by the prefix and storage."""
        return hash(self._decl)


type SecretsArg = (
    Secret
    | SecretPrefix
    | str
    | list[Secret | SecretPrefix | str]
    | tuple[Secret | SecretPrefix | str, ...]
    | None
)
"""What ``secrets=`` takes."""


def declared(value: object, where: str) -> tuple[SecretDecl, ...]:
    """The secrets a task declares in ``secrets=``, each once, in the order written.

    Args:
        value: A list (or one item) of placeholders (``secrets['path#key']``), plain strings
            (``"path#key"``, ``"path"``: the default storage) and ``SecretPrefix(...)``;
            ``None`` for none.
        where: The task, for messages.

    Returns:
        The declarations.

    Raises:
        TypeError: For anything else.
        ValueError: For a bad name, or a plain string ending in ``/``.
    """
    if value is None:
        return ()
    items: Iterable[object] = (
        [value] if isinstance(value, str | Secret | SecretPrefix) else _iterable(value, where)
    )
    found: dict[SecretDecl, None] = {}
    for item in items:
        if isinstance(item, Secret):
            decl: SecretDecl = item._schedr_reference()
        elif isinstance(item, SecretPrefix):
            decl = item._decl
        elif isinstance(item, str):
            try:
                decl = parse_secret(item, None)
            except ValueError as e:
                raise ValueError(f"{where}: secrets=: {e}") from None
        else:
            raise TypeError(
                f"{where}: secrets= takes `secrets['path#key']`, `'path#key'` or "
                f"`SecretPrefix(...)`, got {type(item).__name__}"
            )
        found[decl] = None
    return tuple(found)


def _iterable(value: object, where: str) -> Iterable[object]:
    if isinstance(value, list | tuple):
        return value
    raise TypeError(
        f"{where}: secrets= takes a list of `secrets['path#key']`, `'path#key'` or "
        f"`SecretPrefix(...)`, got {type(value).__name__}"
    )
