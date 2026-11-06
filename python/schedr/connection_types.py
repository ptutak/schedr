"""The built-in connection types: one definition for every kind of task (decisions 174, 183).

``billing_api = HTTP("billing_api", base_url=..., token=secrets["billing"]["token"])`` is a
connection as ``@connection`` makes one, its name the first argument: tasks declare it
(``connections=[billing_api]``) and open it (``with billing_api() as client:``), getting the
type's client. Each client's library is an extra (``schedr[http]``, ``schedr[ssh]``,
``schedr[mongo]``, ``schedr[postgres]``, ``schedr[mysql]``), imported only when a connection
is opened, so ``import schedr`` needs none of them. Every type has a check
(``schedr connections test NAME``): HTTP a request, SSH connecting, Mongo a ``ping``, SQL a
``SELECT 1``.
"""

import contextlib
import os
import ssl
import tempfile
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from pathlib import Path
from string.templatelib import Template
from typing import TYPE_CHECKING, Any, Protocol, cast
from urllib.parse import parse_qsl, unquote, urlsplit

from schedr import _build
from schedr.connections import Connection, TwoWayConnection, defined_values

# The clients' types, for type checkers; at run time `object`, so the annotations resolve
# (schedr reads them) without the extras installed.
if TYPE_CHECKING:
    from httpx import AsyncClient as AsyncHttpClient
    from httpx import Client as HttpClient
    from paramiko import PKey, SSHClient
    from psycopg import AsyncConnection as AsyncPostgres
    from pymongo.asynchronous.database import AsyncDatabase
    from pymongo.database import Database

    MongoDatabase = Database[dict[str, object]]
    AsyncMongoDatabase = AsyncDatabase[dict[str, object]]
    AsyncSqlConnection = AsyncPostgres[tuple[object, ...]]
else:
    HttpClient = AsyncHttpClient = SSHClient = MongoDatabase = object
    AsyncMongoDatabase = AsyncSqlConnection = object

__all__ = ["HTTP", "SQL", "SSH", "DbApiConnection", "DbApiCursor", "Generic", "Mongo"]


def _missing(module: str, extra: str, kind: str) -> ImportError:
    """The error for a client library that isn't installed, naming the extra bringing it."""
    return ImportError(f"a {kind} connection needs `{module}`: install `schedr[{extra}]`")


@contextlib.contextmanager
def _private_file(text: str) -> Iterator[Path]:
    """``text`` in a file only this process reads, gone on leaving.

    For a library taking only a path (a key, a certificate). On Linux an anonymous memory
    file (``/proc/self/fd/N``), never on a disk: a task killed while the library reads it
    leaves nothing behind. Elsewhere a temporary file, mode 0600, removed on leaving.
    """
    if hasattr(os, "memfd_create"):
        descriptor = os.memfd_create("schedr", os.MFD_CLOEXEC)
        try:
            data = text.encode()
            while data:
                data = data[os.write(descriptor, data) :]
            yield Path(f"/proc/self/fd/{descriptor}")
        finally:
            os.close(descriptor)
        return
    descriptor, name = tempfile.mkstemp(prefix="schedr-")
    path = Path(name)
    try:
        with os.fdopen(descriptor, "w") as file:
            file.write(text)
        yield path
    finally:
        path.unlink(missing_ok=True)


# --- HTTP -----------------------------------------------------------------------------------


def _http(
    base_url: str,
    token: str | None = None,
    user: str | None = None,
    password: str | None = None,
    headers: dict[str, str] | None = None,
    ca_cert: str | None = None,
    client_cert: str | None = None,
    client_key: str | None = None,
    timeout: float = 30.0,
    check_path: str | None = None,
) -> Iterator[HttpClient]:
    try:
        import httpx  # noqa: PLC0415  # an extra, imported when a connection is opened
    except ImportError:
        raise _missing("httpx", "http", "HTTP") from None
    sent, auth, context = _http_settings(
        token, user, password, headers, ca_cert, client_cert, client_key
    )
    client = httpx.Client(
        base_url=base_url, headers=sent, auth=auth, verify=context, timeout=timeout
    )
    try:
        yield client
    finally:
        client.close()


async def _ahttp(  # the same values as `_http`
    base_url: str,
    token: str | None = None,
    user: str | None = None,
    password: str | None = None,
    headers: dict[str, str] | None = None,
    ca_cert: str | None = None,
    client_cert: str | None = None,
    client_key: str | None = None,
    timeout: float = 30.0,
    check_path: str | None = None,
) -> AsyncIterator[AsyncHttpClient]:
    del check_path  # a check runs with the sync client
    try:
        import httpx  # noqa: PLC0415  # an extra, imported when a connection is opened
    except ImportError:
        raise _missing("httpx", "http", "HTTP") from None
    sent, auth, context = _http_settings(
        token, user, password, headers, ca_cert, client_cert, client_key
    )
    client = httpx.AsyncClient(
        base_url=base_url, headers=sent, auth=auth, verify=context, timeout=timeout
    )
    try:
        yield client
    finally:
        await client.aclose()


def _http_settings(
    token: str | None,
    user: str | None,
    password: str | None,
    headers: dict[str, str] | None,
    ca_cert: str | None,
    client_cert: str | None,
    client_key: str | None,
) -> tuple[dict[str, str], tuple[str, str] | None, ssl.SSLContext]:
    """What both clients take: the headers, the authentication and the TLS context."""
    sent = dict(headers or {})
    if token is not None:
        sent["Authorization"] = f"Bearer {token}"
    context = (
        ssl.create_default_context(cadata=ca_cert) if ca_cert else ssl.create_default_context()
    )
    if client_cert is not None:
        # Read once into the context; the files are gone before the task's code runs.
        with _private_file(client_cert) as cert, _private_file(client_key or "") as key:
            context.load_cert_chain(cert, key if client_key is not None else None)
    auth = (user, password or "") if user is not None else None
    return sent, auth, context


def _path_only(path: str) -> None:
    """Refuses a ``check_path`` that names a host of its own (``https://...``, ``//...``).

    httpx sends an absolute URL as it is, with the client's headers: the token would go
    there instead of to ``base_url``.

    Raises:
        ValueError: When it does.
    """
    parts = urlsplit(path)
    if parts.scheme or parts.netloc:
        raise ValueError(f"`check_path` is a path under `base_url`, not a URL: {path!r}")


def _check_http(client: HttpClient, values: Mapping[str, object]) -> None:
    """A request to ``check_path``, which must succeed; without one, any answer to ``/``."""
    path = values.get("check_path")
    if isinstance(path, str):
        _path_only(path)
        client.get(path).raise_for_status()
    else:
        client.head("/")


class HTTP(TwoWayConnection[HttpClient, AsyncHttpClient]):
    """An HTTP API: an ``httpx.Client`` with its base URL, authentication and TLS settings.

    ``with billing_api() as client: client.get("/invoices")``; ``async with`` gives an
    ``httpx.AsyncClient``.
    """

    def __init__(  # the type's shape
        self,
        name: str,
        /,
        *,
        base_url: str,
        token: str | None = None,
        user: str | None = None,
        password: str | None = None,
        headers: dict[str, str] | None = None,
        ca_cert: str | None = None,
        client_cert: str | None = None,
        client_key: str | None = None,
        timeout: float = 30.0,
        check_path: str | None = None,
    ) -> None:
        """An HTTP connection named ``name``.

        Args:
            name: Its name, unique in the DAG folder.
            base_url: The URL requests are relative to.
            token: A bearer token (``Authorization: Bearer ...``).
            user: A user for basic authentication, with ``password``.
            password: Its password.
            headers: Headers sent with every request.
            ca_cert: The CAs to trust, as PEM text, instead of the system's.
            client_cert: A client certificate, as PEM text (its key in it, or ``client_key``).
            client_key: The client certificate's key, as PEM text.
            timeout: Seconds for each request.
            check_path: What its check requests (it must succeed); else any answer to ``/``.

        Raises:
            TypeError: For both a token and a user, or a client key without a certificate.
            ValueError: For a ``check_path`` that is a URL.
        """
        if token is not None and user is not None:
            raise TypeError(f"connection `{name}`: a token or a user, not both")
        if client_key is not None and client_cert is None:
            raise TypeError(f"connection `{name}`: `client_key` without `client_cert`")
        if isinstance(check_path, str):
            try:
                _path_only(check_path)
            except ValueError as e:
                raise ValueError(f"connection `{name}`: {e}") from None
        given = {
            "base_url": base_url,
            "token": token,
            "user": user,
            "password": password,
            "headers": headers,
            "ca_cert": ca_cert,
            "client_cert": client_cert,
            "client_key": client_key,
            "timeout": timeout,
            "check_path": check_path,
        }
        super().__init__(
            _http,
            _ahttp,
            defined_values(_http, given, name, "http", _build.caller_file()),
            name=name,
            kind="http",
            checker=_check_http,
        )


# --- SSH ------------------------------------------------------------------------------------


def _known_host(number: int, line: str) -> list[tuple[str, str, PKey]]:
    """The host keys a ``known_hosts`` line trusts: none for a blank line or a comment.

    Raises:
        ValueError: For a line that can't be read, or a marker (``@cert-authority``,
            ``@revoked``), which paramiko doesn't support: refused rather than skipped, so
            no key is trusted, or a revoked one kept, by mistake.
    """
    from paramiko import SSHException  # noqa: PLC0415  # an extra, imported by `_ssh` already
    from paramiko.hostkeys import HostKeyEntry, InvalidHostKey  # noqa: PLC0415  # the same

    line = line.strip()
    if not line or line.startswith("#"):
        return []
    if line.startswith("@"):
        marker = line.split()[0]
        raise ValueError(f"known_hosts line {number}: markers (`{marker}`) aren't supported")
    try:
        entry = HostKeyEntry.from_line(line, number)
    except (SSHException, InvalidHostKey) as e:
        raise ValueError(f"known_hosts line {number} can't be read: {e}") from None
    if entry is None or entry.key is None:
        raise ValueError(f"known_hosts line {number} can't be read")
    return [(host, entry.key.get_name(), entry.key) for host in entry.hostnames]


def _ssh(  # the type's shape
    host: str,
    user: str,
    port: int = 22,
    private_key: str | None = None,
    passphrase: str | None = None,
    password: str | None = None,
    known_hosts: str | None = None,
    insecure_skip_host_check: bool = False,  # a value, given by name
    timeout: float = 30.0,
) -> Iterator[SSHClient]:
    try:
        import paramiko  # noqa: PLC0415  # an extra, imported when a connection is opened
    except ImportError:
        raise _missing("paramiko", "ssh", "SSH") from None
    client = paramiko.SSHClient()
    try:
        if known_hosts is not None:
            keys = client.get_host_keys()
            for number, line in enumerate(known_hosts.splitlines(), 1):
                for host_name, key_type, host_key in _known_host(number, line):
                    keys.add(host_name, key_type, host_key)
            client.set_missing_host_key_policy(paramiko.RejectPolicy())
        elif insecure_skip_host_check:
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = None
        if private_key is not None:
            with _private_file(private_key) as path:
                key = paramiko.PKey.from_path(path, password=passphrase)
        client.connect(
            host,
            port=port,
            username=user,
            pkey=key,
            password=password,
            timeout=timeout,
            look_for_keys=False,
            allow_agent=False,
        )
        yield client
    finally:
        client.close()


def _check_ssh(client: SSHClient, _values: Mapping[str, object]) -> None:
    """Connected when opened: the session must still be up."""
    transport = client.get_transport()
    if transport is None or not transport.is_active():
        raise ConnectionError("the SSH session isn't active")


class SSH(Connection[SSHClient]):
    """An SSH host (network devices too): a connected paramiko ``SSHClient``.

    ``with router() as client: client.exec_command("show version")``. The host's key must be
    in ``known_hosts``; ``insecure_skip_host_check=True`` accepts any, loudly.
    """

    def __init__(  # the type's shape
        self,
        name: str,
        /,
        *,
        host: str,
        user: str,
        port: int = 22,
        private_key: str | None = None,
        passphrase: str | None = None,
        password: str | None = None,
        known_hosts: str | None = None,
        insecure_skip_host_check: bool = False,
        timeout: float = 30.0,
    ) -> None:
        """An SSH connection named ``name``.

        Args:
            name: Its name, unique in the DAG folder.
            host: The host.
            user: The user to log in as.
            port: The port.
            private_key: A private key, as text (OpenSSH or PEM).
            passphrase: The private key's passphrase.
            password: A password, instead of a key or besides it.
            known_hosts: The host's keys, as ``known_hosts`` lines (``[host]:port`` for
                another port than 22): any other key is refused.
            insecure_skip_host_check: Accept whatever key the host shows (an attacker's
                too); only where ``known_hosts`` can't be had.
            timeout: Seconds to connect.

        Raises:
            TypeError: Without ``known_hosts`` or ``insecure_skip_host_check=True``, or with
                both.
        """
        if known_hosts is None and not insecure_skip_host_check:
            raise TypeError(
                f"connection `{name}`: `known_hosts` (the host's keys) is needed to trust the "
                "host; `insecure_skip_host_check=True` accepts any key, an attacker's too"
            )
        if known_hosts is not None and insecure_skip_host_check:
            raise TypeError(
                f"connection `{name}`: `known_hosts` or `insecure_skip_host_check`, not both"
            )
        given = {
            "host": host,
            "user": user,
            "port": port,
            "private_key": private_key,
            "passphrase": passphrase,
            "password": password,
            "known_hosts": known_hosts,
            "insecure_skip_host_check": insecure_skip_host_check,
            "timeout": timeout,
        }
        super().__init__(
            _ssh,
            defined_values(_ssh, given, name, "ssh", _build.caller_file()),
            name=name,
            kind="ssh",
            checker=_check_ssh,
        )


# --- Mongo ----------------------------------------------------------------------------------


def _credentials(url: str, user: str | None, password: str | None) -> None:
    """Refuses a user or a password given twice: in the URL and as a value of its own.

    Raises:
        ValueError: When one is.
    """
    parts = urlsplit(url)
    for what, in_url, given in (
        ("user", parts.username, user),
        ("password", parts.password, password),
    ):
        if in_url is not None and given is not None:
            raise ValueError(f"a {what} in the URL and `{what}=` both: give it once")


def _template_text(url: object) -> str | None:
    """A URL's text, a t-string's values left out (each written ``0``): its structure.

    ``None`` for a URL that is a secret as a whole: known when the task opens it.
    """
    if isinstance(url, Template):
        return "0".join(url.strings)
    return url if isinstance(url, str) else None


def _mongo(
    uri: str,
    database: str,
    user: str | None = None,
    password: str | None = None,
    ca_cert: str | None = None,
    timeout: float = 30.0,
) -> Iterator[MongoDatabase]:
    try:
        import pymongo  # noqa: PLC0415  # an extra, imported when a connection is opened
    except ImportError:
        raise _missing("pymongo", "mongo", "Mongo") from None
    with _mongo_tls(uri, user, password, ca_cert) as (tls, ca_file):
        client: pymongo.MongoClient[dict[str, object]] = pymongo.MongoClient(
            uri,
            document_class=dict,
            serverSelectionTimeoutMS=round(timeout * 1000),
            username=user,
            password=password,
            tls=tls,
            tlsCAFile=ca_file,
        )
        # The CA file stays open with it: a client resolving `mongodb+srv://` at its first
        # command (the async one always, the sync one with `connect=false`) reads it then.
        try:
            yield client[database]
        finally:
            client.close()


async def _amongo(  # the same values as `_mongo`
    uri: str,
    database: str,
    user: str | None = None,
    password: str | None = None,
    ca_cert: str | None = None,
    timeout: float = 30.0,
) -> AsyncIterator[AsyncMongoDatabase]:
    try:
        import pymongo  # noqa: PLC0415  # an extra, imported when a connection is opened
    except ImportError:
        raise _missing("pymongo", "mongo", "Mongo") from None
    with _mongo_tls(uri, user, password, ca_cert) as (tls, ca_file):
        client: pymongo.AsyncMongoClient[dict[str, object]] = pymongo.AsyncMongoClient(
            uri,
            document_class=dict,
            serverSelectionTimeoutMS=round(timeout * 1000),
            username=user,
            password=password,
            tls=tls,
            tlsCAFile=ca_file,
        )
        # As the sync client: the CA file is read again at the first command for SRV.
        try:
            yield client[database]
        finally:
            await client.close()


@contextlib.contextmanager
def _mongo_tls(
    uri: str, user: str | None, password: str | None, ca_cert: str | None
) -> Iterator[tuple[bool, str | None]]:
    """Whether both clients use TLS, and their CA file, which exists while the block runs."""
    _credentials(uri, user, password)
    options = {
        key.lower(): value for key, value in parse_qsl(urlsplit(uri).query, keep_blank_values=True)
    }
    # TLS, verified, unless the URI says otherwise (decision 189); a keyword overrides the
    # URI's own, so the URI's is passed on as it is.
    written = options.get("tls", options.get("ssl"))
    tls = written.lower() == "true" if written is not None else True
    with contextlib.ExitStack() as files:
        ca_file = str(files.enter_context(_private_file(ca_cert))) if ca_cert is not None else None
        yield tls, ca_file


def _check_mongo(db: MongoDatabase, _values: Mapping[str, object]) -> None:
    db.command("ping")


class Mongo(TwoWayConnection[MongoDatabase, AsyncMongoDatabase]):
    """A MongoDB database: a pymongo ``Database``, its client closed when the block ends.

    ``with orders_db() as db: db.orders.count_documents({})``. TLS is used and verified
    unless the URI says ``tls=false`` (decision 189): against ``ca_cert``, else the system's
    CAs; the URI's own TLS options (``tlsAllowInvalidHostnames=true``, ...) as written.
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        uri: str,
        database: str,
        user: str | None = None,
        password: str | None = None,
        ca_cert: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        """A Mongo connection named ``name``.

        Args:
            name: Its name, unique in the DAG folder.
            uri: The MongoDB URI (``mongodb://host/``); a value a t-string fills in is
                percent-encoded (decision 188).
            database: The database the task gets (not where the user logs in).
            user: The user, if not in the URI. It logs in, as with every Mongo client, on
                the URI's ``authSource``, else the database in its path, else ``admin``:
                ``mongodb://host/orders`` for a user made in ``orders``.
            password: Its password, if not in the URI.
            ca_cert: The CA certificates (PEM text) to check the server's against, instead of
                the system's.
            timeout: Seconds to find a server.

        Raises:
            ValueError: For a user or a password both in the URI and given.
        """
        try:
            written = _template_text(uri)
            if written is not None:
                _credentials(written, user, password)
        except ValueError as e:
            raise ValueError(f"connection `{name}`: {e}") from None
        given = {
            "uri": uri,
            "database": database,
            "user": user,
            "password": password,
            "ca_cert": ca_cert,
            "timeout": timeout,
        }
        super().__init__(
            _mongo,
            _amongo,
            defined_values(_mongo, given, name, "mongo", _build.caller_file(), urls=("uri",)),
            name=name,
            kind="mongo",
            checker=_check_mongo,
        )


# --- SQL ------------------------------------------------------------------------------------


class DbApiCursor(Protocol):
    """A DB-API cursor, as psycopg's and PyMySQL's are."""

    def execute(self, query: str, params: Sequence[object] | None = ..., /) -> object:
        """Runs a query."""
        ...

    def fetchone(self) -> Sequence[object] | None:
        """The next row."""
        ...

    def fetchall(self) -> Sequence[Sequence[object]]:
        """The remaining rows."""
        ...

    def close(self) -> None:
        """Closes it."""
        ...


class DbApiConnection(Protocol):
    """A DB-API connection, as psycopg's and PyMySQL's are."""

    def cursor(self) -> DbApiCursor:
        """A new cursor."""
        ...

    def commit(self) -> None:
        """Commits the transaction."""
        ...

    def rollback(self) -> None:
        """Rolls it back."""
        ...

    def close(self) -> None:
        """Closes the connection."""
        ...


_POSTGRES = ("postgresql", "postgres")
# MariaDB speaks MySQL's protocol: the same driver (decision 196).
_MYSQL = ("mysql", "mariadb")


def _scheme(url: str) -> str:
    return urlsplit(url).scheme.split("+", 1)[0].lower()


_SSLMODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")


def _sslmode(url: str, scheme: str, *, mode_written: bool = True) -> str:
    """The URL's ``sslmode`` (libpq's, for both databases); ``verify-full`` without one.

    Decision 187.

    Args:
        url: The URL, or a t-string's text (its values left out: ``mode_written=False``).
        scheme: Its scheme.
        mode_written: Whether ``sslmode``'s value is the one used (not a t-string's
            placeholder).

    Raises:
        ValueError: For another mode, or a query key a mysql:// URL doesn't take: PyMySQL
            would ignore it, a TLS setting among them.
    """
    query = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    mode = query.get("sslmode", "verify-full")
    if mode_written and mode not in _SSLMODES:
        raise ValueError(f"`sslmode={mode}`: one of {', '.join(_SSLMODES)}")
    other = sorted(set(query) - {"sslmode"})
    if scheme in _MYSQL and other:
        raise ValueError(
            f"a {scheme}:// URL takes only `sslmode`, not {', '.join(f'`{key}`' for key in other)}"
        )
    return mode


def _postgres(
    url: str, user: str | None, password: str | None, ca_cert: str | None, timeout: float
) -> DbApiConnection:
    try:
        import psycopg  # noqa: PLC0415  # an extra, imported when a connection is opened
    except ImportError:
        raise _missing("psycopg", "postgres", "Postgres") from None
    with _postgres_settings(url, ca_cert) as (sslmode, sslrootcert):
        # libpq reads the CA file while connecting: it can go once connected.
        return psycopg.connect(
            url,
            connect_timeout=max(1, round(timeout)),
            user=user,
            password=password,
            sslmode=sslmode,
            sslrootcert=sslrootcert,
        )


async def _apostgres(
    url: str, user: str | None, password: str | None, ca_cert: str | None, timeout: float
) -> AsyncSqlConnection:
    try:
        import psycopg  # noqa: PLC0415  # an extra, imported when a connection is opened
    except ImportError:
        raise _missing("psycopg", "postgres", "Postgres") from None
    with _postgres_settings(url, ca_cert) as (sslmode, sslrootcert):
        # As the sync one: its CA file goes once connected.
        return await psycopg.AsyncConnection.connect(
            url,
            connect_timeout=max(1, round(timeout)),
            user=user,
            password=password,
            sslmode=sslmode,
            sslrootcert=sslrootcert,
        )


@contextlib.contextmanager
def _postgres_settings(url: str, ca_cert: str | None) -> Iterator[tuple[str | None, str | None]]:
    """``sslmode`` and ``sslrootcert`` for both connections; the CA file exists in the block."""
    query = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    mode = _sslmode(url, "postgresql")
    # A keyword overrides the URL's own; psycopg leaves out one that is `None`.
    sslmode = None if "sslmode" in query else mode
    with contextlib.ExitStack() as files:
        sslrootcert = None
        if ca_cert is not None:
            sslrootcert = str(files.enter_context(_private_file(ca_cert)))
        elif mode == "verify-full" and "sslrootcert" not in query:
            sslrootcert = "system"  # the system's CAs, as HTTP's (libpq 16+)
        yield sslmode, sslrootcert


# WORKAROUND(pymysql==1.2.3): PyMySQL's MySQL 8 login without TLS returns no packet; fixed
# upstream in PyMySQL #1275 (commit 5e4ac551), not in a release yet. Remove this class and its
# `auth_plugin_map` use in `_mysql` once the extra's PyMySQL has the fix.
class _CachingSha2Password:
    """PyMySQL's own ``caching_sha2_password`` login, as a handler of one's own.

    PyMySQL 1.2.3's full authentication without TLS (the password sent RSA-encrypted) reads
    the server's answer and doesn't return it (fixed upstream, PyMySQL #1275, unreleased), so
    its login loop fails on ``None``. The loop takes ``None`` from a handler of one's own as
    the login's end: the answer was an OK, an error being raised when read. Remove once a
    release has the fix.
    """

    def __init__(self, connection: object) -> None:
        self._connection = connection

    def authenticate(self, packet: object) -> object:
        """One step of the login: PyMySQL's own."""
        from pymysql import _auth  # noqa: PLC0415  # an extra, imported when one is opened

        return _auth.caching_sha2_password_auth(self._connection, packet)


def _mysql(
    url: str, user: str | None, password: str | None, ca_cert: str | None, timeout: float
) -> DbApiConnection:
    try:
        import pymysql  # noqa: PLC0415  # an extra, imported when a connection is opened
    except ImportError:
        raise _missing("pymysql", "mysql", "MySQL") from None
    mode = _sslmode(url, "mysql")
    context = None  # PyMySQL's own default: TLS when the server offers it
    if mode in ("require", "verify-ca", "verify-full"):
        # A context makes PyMySQL refuse a server without TLS, before authenticating.
        context = (
            ssl.create_default_context(cadata=ca_cert) if ca_cert else ssl.create_default_context()
        )
        # As PyMySQL's own contexts: not RFC 5280's strict profile, which refuses the
        # certificates MySQL makes itself (no Authority Key Identifier); the chain, the
        # signatures and the dates are still checked.
        context.verify_flags &= ~ssl.VERIFY_X509_STRICT
        if mode != "verify-full":
            context.check_hostname = False
        if mode == "require":
            context.verify_mode = ssl.CERT_NONE
    parts = urlsplit(url)
    mysql = pymysql.connect(
        # WORKAROUND(pymysql==1.2.3): its own handling loses the packet ending MySQL 8's first
        # login without TLS; a handler of one's own may end it so (`_CachingSha2Password`).
        auth_plugin_map={"caching_sha2_password": _CachingSha2Password},
        host=parts.hostname or "localhost",
        port=parts.port or 3306,
        user=user if user is not None else unquote(parts.username or ""),
        password=password if password is not None else unquote(parts.password or ""),
        database=unquote(parts.path.lstrip("/")) or None,
        connect_timeout=max(1, round(timeout)),
        ssl=context,
        ssl_disabled=mode == "disable",
    )
    # A DB-API connection; its stubs' `cursor` overloads (a cursor class argument) don't
    # match the protocol's plain `cursor()`, which it has.
    return cast("DbApiConnection", mysql)


def _sql(
    url: str,
    user: str | None = None,
    password: str | None = None,
    ca_cert: str | None = None,
    timeout: float = 30.0,
) -> Iterator[DbApiConnection]:
    scheme = _scheme(url)
    _credentials(url, user, password)
    if scheme in _POSTGRES:
        connection = _postgres(url, user, password, ca_cert, timeout)
    elif scheme in _MYSQL:
        connection = _mysql(url, user, password, ca_cert, timeout)
    else:
        raise ValueError(
            f"an SQL connection's URL is postgresql://, mysql:// or mariadb://, not {scheme}://"
        )
    try:
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


async def _asql(  # the same values as `_sql`
    url: str,
    user: str | None = None,
    password: str | None = None,
    ca_cert: str | None = None,
    timeout: float = 30.0,
) -> AsyncIterator[AsyncSqlConnection]:
    scheme = _scheme(url)
    if scheme in _MYSQL:
        raise TypeError(
            f"`async with` on an SQL connection is for Postgres; a {scheme}:// one has no "
            "async driver here: use `with` (decision 317)"
        )
    if scheme not in _POSTGRES:
        raise ValueError(
            f"an SQL connection's URL is postgresql://, mysql:// or mariadb://, not {scheme}://"
        )
    _credentials(url, user, password)
    connection = await _apostgres(url, user, password, ca_cert, timeout)
    try:
        yield connection
        await connection.commit()
    except BaseException:
        await connection.rollback()
        raise
    finally:
        await connection.close()


def _check_sql(connection: DbApiConnection, _values: Mapping[str, object]) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT 1")
        if cursor.fetchone() is None:
            raise ConnectionError("`SELECT 1` gave no row")
    finally:
        cursor.close()


def _scheme_written(url: object) -> str | None:
    """The scheme of a URL given as text, or a t-string starting with it; else ``None``."""
    if isinstance(url, str):
        return _scheme(url)
    if isinstance(url, Template) and url.strings and "://" in url.strings[0]:
        return _scheme(url.strings[0])
    return None


class SQL(TwoWayConnection[DbApiConnection, AsyncSqlConnection]):
    """A Postgres or MySQL database, by URL: a DB-API connection (psycopg 3, PyMySQL).

    ``with warehouse() as db: cursor = db.cursor()``; the block's work is committed when it
    ends, rolled back on an error, as psycopg's own ``with`` does.

    TLS is libpq's ``sslmode`` in the URL, for both: ``verify-full`` (the default) needs TLS
    and checks the server's certificate and name, against ``ca_cert`` or the system's CAs;
    ``verify-ca`` the certificate only; ``require`` TLS, unchecked; ``prefer`` and ``allow``
    TLS when offered; ``disable`` none (``?sslmode=disable``, for a server without TLS).
    """

    def __init__(
        self,
        name: str,
        /,
        *,
        url: str,
        user: str | None = None,
        password: str | None = None,
        ca_cert: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        """An SQL connection named ``name``.

        Args:
            name: Its name, unique in the DAG folder.
            url: ``postgresql://host:5432/db``, ``mysql://...`` or ``mariadb://...``, with
                ``?sslmode=...`` for another TLS mode than ``verify-full``; a value a t-string
                fills in is percent-encoded (decision 188), so a secret can't change the URL.
            user: The user, if not in the URL.
            password: Its password, if not in the URL: given to the driver as it is, never
                put in the URL.
            ca_cert: The CA certificates (PEM text) to check the server's against, instead of
                the system's.
            timeout: Seconds to connect.

        Raises:
            ValueError: For another scheme, another ``sslmode``, a query key a mysql://
                URL doesn't take, or a user or a password both in the URL and given.
        """
        scheme = _scheme_written(url)
        if scheme is not None and scheme not in _POSTGRES + _MYSQL:
            raise ValueError(
                f"connection `{name}`: the URL is postgresql://, mysql:// or mariadb://, not "
                f"{scheme}://"
            )
        try:
            # A t-string's text, its values left out: they are encoded, so its structure
            # (the keys, the user, the password) is known now.
            written = _template_text(url)
            if written is not None:
                _credentials(written, user, password)
            if written is not None and scheme is not None:
                _sslmode(written, scheme, mode_written=not isinstance(url, Template))
        except ValueError as e:
            raise ValueError(f"connection `{name}`: {e}") from None
        given = {
            "url": url,
            "user": user,
            "password": password,
            "ca_cert": ca_cert,
            "timeout": timeout,
        }
        super().__init__(
            _sql,
            _asql,
            defined_values(_sql, given, name, "sql", _build.caller_file(), urls=("url",)),
            name=name,
            kind="sql",
            checker=_check_sql,
        )


# --- Generic --------------------------------------------------------------------------------


# `Any`: schedr's own "no type check" for a value (a key of one's own has no declared type);
# the class takes `object` for type checkers.
def _generic(**values: Any) -> Iterator[dict[str, object]]:
    yield dict(values)


def _check_generic(_values_given: dict[str, object], _values: Mapping[str, object]) -> None:
    """Nothing to reach: it holds values only."""


async def _ageneric(**values: Any) -> AsyncIterator[dict[str, object]]:
    yield dict(values)


class Generic(TwoWayConnection[dict[str, object], dict[str, object]]):
    """Keys of one's own: the task gets them as a dict.

    ``with feature_flags() as flags: flags["region"]``, or ``async with``.
    """

    def __init__(self, name: str, /, **values: object) -> None:
        """A connection named ``name``, holding ``values``.

        Args:
            name: Its name, unique in the DAG folder.
            **values: Its keys' values: plain values, secrets and t-strings of them.
        """
        super().__init__(
            _generic,
            _ageneric,
            defined_values(_generic, values, name, "generic", _build.caller_file()),
            name=name,
            kind="generic",
            checker=_check_generic,
        )
