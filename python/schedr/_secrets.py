"""Secrets in the DAG body: ``secrets['etl/db']['password']`` and ``SecretPrefix(...)``.

A secret is named, never read, in the DAG file: ``secrets['etl/db']['password']`` is a
placeholder, filled in by the worker agent that runs the task (in an argument, a t-string or
``env``), and ``secrets=[...]`` declares the secrets a task reads on demand (``schedr
task-ctx secret get``, ``task_ctx.secret(...)``). A placeholder is typed as the value it
stands for (decision 176): ``secrets['etl/db']`` as the whole secret, ``dict[str, str]``;
``['password']``, ``.get('password', '')`` and ``secrets.value('etl/db#password')`` as one
value, ``str``. Misuse in the DAG body is refused when the file loads. See PROJECT.md, Secret
storage.
"""

from collections.abc import Iterable, Iterator
from typing import Final, cast

from schedr._model import SecretDecl, SecretPrefixDecl, SecretRef
from schedr._model.values import storage_label

_VALUE = (
    "is a placeholder, filled in by the worker that runs the task: pass it to a task (an "
    "argument, a t-string, `env=`), or declare it in `secrets=` and read it inside the task "
    "with `task_ctx.secret(...)`"
)


def parse_secret(spec: object, storage: str | None) -> SecretRef:
    """``path#key`` (or ``path``, the whole secret) as a secret of ``storage``.

    The form of a secret written as one string: ``secrets=``, TOML, the CLI, test values.

    Raises:
        TypeError: For a spec that isn't text.
        ValueError: For a bad path or key, or a path ending in ``/`` (a prefix).
    """
    if not isinstance(spec, str):
        raise TypeError(f"`{storage_label(storage)}`: a secret is named by text, `'path#key'`")
    path, sep, key = spec.partition("#")
    if path.endswith("/") and len(path) > 1:
        raise ValueError(
            f"`{spec}`: a path doesn't end with `/`; for every secret under it, declare "
            f"`SecretPrefix({path!r})` in `secrets=`"
        )
    return SecretRef(path, key if sep else None, storage)


class Secret:
    """A secret named in the DAG body, filled in at run time: ``secrets['etl/db']['password']``."""

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
        """As written: ``secrets['etl/db']['password']`` (never a value: it has none here)."""
        return self._ref.label()

    def _refusal(self) -> TypeError:
        return TypeError(f"`{self._ref.label()}` {_VALUE}")

    def __str__(self) -> str:
        """Refuses to be formatted into a plain string.

        Raises:
            TypeError: Always.
        """
        raise self._refusal()

    def __bool__(self) -> bool:
        """Refuses to be used as a value.

        Raises:
            TypeError: Always.
        """
        raise self._refusal()

    def __iter__(self) -> Iterator[str]:
        """Refuses to be iterated (the keys are known only at run time).

        Raises:
            TypeError: Always.
        """
        raise self._refusal()

    def __len__(self) -> int:
        """Refuses to be measured.

        Raises:
            TypeError: Always.
        """
        raise self._refusal()

    def __eq__(self, other: object) -> bool:
        """Two placeholders for the same secret are equal; nothing else compares.

        Raises:
            TypeError: Against anything but a placeholder (the value is known only at run
                time).
        """
        if isinstance(other, Secret):
            return self._ref == other._ref
        raise self._refusal()

    def __hash__(self) -> int:
        """Hashed by the secret it names."""
        return hash(self._ref)


class WholeSecret(Secret):
    """A whole secret, every key: ``secrets['etl/db']``; ``['password']`` is one of them."""

    __slots__ = ()

    def __getitem__(self, key: str) -> Secret:
        """One key of the secret: ``secrets['etl/db']['password']``.

        Args:
            key: The key.

        Returns:
            Its placeholder.

        Raises:
            TypeError: For a key that isn't text.
            ValueError: For a bad key.
        """
        return Secret(self._key(key, None))

    def get(self, key: str, default: str | None = None) -> Secret:
        """One key of the secret, or ``default`` when the secret or the key is missing.

        A default is written in the DAG, so it is shown as it is (in the DAG's JSON, the API,
        the UI) and never masked: never a real secret.

        Args:
            key: The key.
            default: The text read when it is missing.

        Returns:
            Its placeholder.

        Raises:
            TypeError: For a key or a default that isn't text, or no default.
            ValueError: For a bad key.
        """
        if not isinstance(default, str):
            raise TypeError(
                f"`{self._ref.label()}.get({key!r})`: give the default as text, `.get({key!r}, '')`"
            )
        return Secret(self._key(key, default))

    def _key(self, key: object, default: str | None) -> SecretRef:
        ref = self._ref
        if not isinstance(key, str):
            raise TypeError(f"`{ref.label()}`: a key is text, `{ref.label()}['password']`")
        return SecretRef(ref.path, key, ref.storage, default)


def _whole(path: object, storage: str | None) -> WholeSecret:
    label = storage_label(storage)
    if not isinstance(path, str):
        raise TypeError(f"`{label}`: a secret is named by its path, `{label}['etl/db']`")
    if "#" in path:
        whole, _, key = path.partition("#")
        raise ValueError(
            f"`{label}[{path!r}]`: index by the path, then the key, "
            f"`{label}[{whole!r}][{key!r}]`, or write `{label}.value({path!r})`"
        )
    return WholeSecret(parse_secret(path, storage))


class Storage:
    """One secret storage: ``secrets.other['etl/db']['password']``."""

    __slots__ = ("_name",)

    def __init__(self, name: str | None) -> None:
        """A storage by name; ``None`` for the default one.

        Args:
            name: Its ``[secret_storage.<name>]`` key.
        """
        self._name = name

    def __getitem__(self, path: str) -> dict[str, str]:
        """The whole secret at ``path``, every key; ``[key]`` on it is one value.

        Typed as the value a task receives (decision 176): the object is a placeholder.

        Args:
            path: The secret's path, ``etl/db``.

        Returns:
            Its placeholder.
        """
        # A placeholder typed as its value, as an output is: a checker reads what the task
        # gets; loading the file refuses using it as a value.
        return cast("dict[str, str]", _whole(path, self._name))

    def value(self, spec: str, default: str | None = None) -> str:
        """One value, written ``path#key``: ``secrets.value('etl/db#password')``.

        Args:
            spec: ``path#key``.
            default: The text read when the secret or the key is missing; written in the
                DAG, shown as it is, never masked: never a real secret.

        Returns:
            Its placeholder (typed as the value).

        Raises:
            TypeError: For a spec or default that isn't text.
            ValueError: For a spec without ``#key`` (the whole secret is indexed) or a bad
                one.
        """
        label = storage_label(self._name)
        ref = parse_secret(spec, self._name)
        if ref.key is None:
            raise ValueError(
                f"`{label}.value({spec!r})`: one value is `path#key`; the whole secret is "
                f"`{label}[{spec!r}]`"
            )
        if default is not None and not isinstance(default, str):
            raise TypeError(f"`{label}.value({spec!r})`: a default is text")
        placeholder = Secret(SecretRef(ref.path, ref.key, ref.storage, default))
        return cast("str", placeholder)

    def __repr__(self) -> str:
        """As written."""
        return storage_label(self._name)


class Secrets(Storage):
    """The default storage, ``secrets['etl/db']['password']``, and the others by name."""

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
"""Secrets from secret storage: ``secrets['etl/db']['password']`` (the default storage),
``secrets.other['etl/db']['password']`` (the storage ``[secret_storage.other]``)."""


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
    SecretPrefix
    | str
    | dict[str, str]
    | list[SecretPrefix | str | dict[str, str]]
    | tuple[SecretPrefix | str | dict[str, str], ...]
    | None
)
"""What ``secrets=`` takes: placeholders are typed as their values (``str`` for one key,
``dict[str, str]`` for a whole secret), plain strings ``'path#key'``, ``SecretPrefix``."""


def declared(value: object, where: str) -> tuple[SecretDecl, ...]:
    """The secrets a task declares in ``secrets=``, each once, in the order written.

    Args:
        value: A list (or one item) of placeholders (``secrets['path']['key']``), plain strings
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
            ref = item._schedr_reference()
            if ref.default is not None:
                raise ValueError(
                    f"{where}: secrets=: `{ref.label()}`: a default is for a value passed to "
                    "a task, not a declaration"
                )
            decl: SecretDecl = ref
        elif isinstance(item, SecretPrefix):
            decl = item._decl
        elif isinstance(item, str):
            try:
                decl = parse_secret(item, None)
            except ValueError as e:
                raise ValueError(f"{where}: secrets=: {e}") from None
        else:
            raise TypeError(
                f"{where}: secrets= takes `secrets['path']['key']`, `'path#key'` or "
                f"`SecretPrefix(...)`, got {type(item).__name__}"
            )
        found[decl] = None
    return tuple(found)


def _iterable(value: object, where: str) -> Iterable[object]:
    if isinstance(value, list | tuple):
        return value
    raise TypeError(
        f"{where}: secrets= takes a list of `secrets['path']['key']`, `'path#key'` or "
        f"`SecretPrefix(...)`, got {type(value).__name__}"
    )
