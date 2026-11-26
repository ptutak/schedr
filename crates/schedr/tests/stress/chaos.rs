//! What goes wrong on purpose: a pod dying with everything in it, a process frozen, the network
//! to the database stalled, a Service that sends new connections to whichever master is ready,
//! and each worker's own link to the master cut, one way or both, or slowed (M4.8z); with a
//! timeline of what was done, and the tasks' processes watched to the end.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::check::now_ns;
use crate::support::Cluster;

/// Kills a role and everything it started (`kill -9` on the whole process tree), as a pod
/// dying with its node would: tasks don't outlive it. The killed processes and when.
pub fn pod_death(cluster: &mut Cluster, name: &str) -> HashMap<u32, u128> {
    let root = cluster
        .roles
        .iter()
        .find(|r| r.name == name)
        .unwrap()
        .child
        .id();
    let tree = process_tree(root);
    // The role first, so it starts nothing more; then what it started.
    for pid in &tree {
        signal(*pid, "KILL");
    }
    let at = now_ns();
    cluster.kill(name);
    tree.into_iter().map(|pid| (pid, at)).collect()
}

/// `pid` and its descendants, parents before children.
fn process_tree(pid: u32) -> Vec<u32> {
    let mut parents: HashMap<u32, Vec<u32>> = HashMap::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Some(child) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        // `pid (comm) state ppid ...`: the command may hold spaces and parentheses.
        let Some(parent) = stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.split_whitespace().nth(1))
            .and_then(|p| p.parse::<u32>().ok())
        else {
            continue;
        };
        parents.entry(parent).or_default().push(child);
    }
    let mut tree = vec![pid];
    let mut next = 0;
    while next < tree.len() {
        if let Some(children) = parents.get(&tree[next]) {
            tree.extend(children);
        }
        next += 1;
    }
    tree
}

/// Sends `SIG<name>` to `pid` (it may be gone already).
pub fn signal(pid: u32, name: &str) {
    let _ = Command::new("kill")
        .args([&format!("-{name}"), &pid.to_string()])
        .status();
}

/// The process ID of the role started as `name`.
pub fn pid_of(cluster: &Cluster, name: &str) -> u32 {
    cluster
        .roles
        .iter()
        .find(|r| r.name == name)
        .unwrap()
        .child
        .id()
}

/// Which way bytes go through a [`Proxy`]: to its upstream (a worker's messages, on a worker's
/// link) or back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Way {
    Up,
    Down,
}

/// What a [`Proxy`]'s pumps read on each pass: held ways, and a delay per chunk.
#[derive(Default)]
struct Link {
    /// Up, then down: nothing goes that way while set (a silent cut: no reset, no error, the
    /// sender's buffers fill up as on a lost network).
    held: [AtomicBool; 2],
    /// Milliseconds each chunk waits before it is passed on (a slow link).
    delay_ms: AtomicU64,
    /// The address answers nothing: no connection is accepted, and those open are closed.
    silent: AtomicBool,
    /// The workers' ends of the open connections, closed when the address goes silent.
    open: Mutex<Vec<TcpStream>>,
    /// The connections accepted so far; those numbered below `stranded` pass nothing either way
    /// and are never closed (an old connection dead without a word), the later ones pass.
    accepted: AtomicU64,
    stranded: AtomicU64,
}

/// A TCP proxy on 127.0.0.1: forwards each new connection to the current upstream, and can be
/// frozen (nothing goes through, either way or one, until thawed), like a stalled network, or
/// slowed.
pub struct Proxy {
    pub port: u16,
    listener: Arc<TcpListener>,
    /// What fills the listener's queue while it is silent.
    filling: Mutex<Vec<TcpStream>>,
    upstream: Arc<Mutex<Option<SocketAddr>>>,
    link: Arc<Link>,
    stop: Arc<AtomicBool>,
    accepting: Option<JoinHandle<()>>,
}

impl Proxy {
    pub fn new(upstream: Option<SocketAddr>) -> Self {
        let listener = Arc::new(TcpListener::bind("127.0.0.1:0").unwrap());
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let upstream = Arc::new(Mutex::new(upstream));
        let link = Arc::new(Link::default());
        let stop = Arc::new(AtomicBool::new(false));
        let accepting = std::thread::spawn({
            let (upstream, link, stop) = (upstream.clone(), link.clone(), stop.clone());
            let listener = listener.clone();
            move || accept(&listener, &upstream, &link, &stop)
        });
        Self {
            port,
            listener,
            filling: Mutex::default(),
            upstream,
            link,
            stop,
            accepting: Some(accepting),
        }
    }

    /// Holds both ways, or lets both go.
    pub fn freeze(&self, frozen: bool) {
        self.hold(Way::Up, frozen);
        self.hold(Way::Down, frozen);
    }

    /// The address answers nothing, or does again: the open connections are closed, and a new
    /// one's SYN is dropped (the queue full, nothing accepted), as a killed pod's address drops
    /// it (B296). Unlike a cut, whose connections are accepted and then held.
    pub fn silence(&self, silent: bool) {
        self.link.silent.store(silent, Ordering::SeqCst);
        let mut filling = self.filling.lock().unwrap();
        filling.clear();
        if !silent {
            rustix::net::listen(&*self.listener, 128).unwrap();
            return;
        }
        // The accepting thread is past its last accept.
        std::thread::sleep(Duration::from_millis(100));
        for stream in self.link.open.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
        rustix::net::listen(&*self.listener, 1).unwrap();
        let address = self.listener.local_addr().unwrap();
        while let Ok(stream) = TcpStream::connect_timeout(&address, Duration::from_millis(300)) {
            filling.push(stream);
            assert!(filling.len() < 64, "the listener's queue doesn't fill");
        }
    }

    /// The connections open now pass nothing more, either way, and are never closed; new ones
    /// pass (B301: a connection that died without a word, its worker connecting again).
    pub fn strand(&self, stranded: bool) {
        let below = if stranded {
            self.link.accepted.load(Ordering::SeqCst) + 1
        } else {
            0
        };
        self.link.stranded.store(below, Ordering::SeqCst);
    }

    /// Holds one way, or lets it go.
    pub fn hold(&self, way: Way, held: bool) {
        self.link.held[way as usize].store(held, Ordering::SeqCst);
    }

    /// Delays each chunk by `delay` (zero: none).
    pub fn slow(&self, delay: Duration) {
        self.link.delay_ms.store(
            u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.freeze(false);
        self.slow(Duration::ZERO);
        if let Some(accepting) = self.accepting.take() {
            let _ = accepting.join();
        }
    }
}

fn accept(
    listener: &TcpListener,
    upstream: &Mutex<Option<SocketAddr>>,
    link: &Arc<Link>,
    stop: &Arc<AtomicBool>,
) {
    while !stop.load(Ordering::SeqCst) {
        if link.silent.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        let Ok((client, _)) = listener.accept() else {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        };
        let to = *upstream.lock().unwrap();
        let (link, stop) = (link.clone(), stop.clone());
        let number = link.accepted.fetch_add(1, Ordering::SeqCst) + 1;
        // Each connection dialled in its own thread, within a limit: an upstream that stopped
        // accepting (a master exiting, its port still bound) held this loop for the kernel's
        // SYN retries (over 2 minutes), every other worker's connection waiting behind it,
        // which a Service never does (stress 05, 2026-11-05).
        std::thread::spawn(move || {
            let Some(server) =
                to.and_then(|to| TcpStream::connect_timeout(&to, UPSTREAM_CONNECT).ok())
            else {
                // Nowhere to go: refused, as a Service with no ready endpoint.
                drop(client);
                return;
            };
            client.set_nonblocking(false).unwrap();
            let mut open = link.open.lock().unwrap();
            open.retain(|stream| stream.peer_addr().is_ok());
            open.push(client.try_clone().unwrap());
            drop(open);
            for (way, from, to) in [
                (
                    Way::Up,
                    client.try_clone().unwrap(),
                    server.try_clone().unwrap(),
                ),
                (Way::Down, server, client),
            ] {
                let (link, stop) = (link.clone(), stop.clone());
                std::thread::spawn(move || pump((way, number), from, to, &link, &stop));
            }
        });
    }
}

/// How long a [`Proxy`] tries to reach its upstream for a new connection.
const UPSTREAM_CONNECT: Duration = Duration::from_secs(2);

/// Copies one direction of a connection, holding what it read while that way is held, each
/// chunk after the link's delay.
fn pump(
    (way, number): (Way, u64),
    mut from: TcpStream,
    mut to: TcpStream,
    link: &Link,
    stop: &AtomicBool,
) {
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        let delay = link.delay_ms.load(Ordering::SeqCst);
        if delay > 0 && !stop.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(delay));
        }
        while (link.held[way as usize].load(Ordering::SeqCst)
            || number < link.stranded.load(Ordering::SeqCst))
            && !stop.load(Ordering::SeqCst)
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        if to.write_all(&buffer[..read]).is_err() {
            break;
        }
    }
    // A stranded connection's end is held too: it never closes (agy's review of M5.3g).
    while number < link.stranded.load(Ordering::SeqCst) && !stop.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = to.shutdown(Shutdown::Write);
}

/// Points `proxy` at whichever of `masters` (name, gRPC port) answers `/readyz` with 200 on its
/// ops port, every 200 ms, as a Kubernetes Service follows its ready endpoints. Stops when
/// `stop` is set.
pub fn follow_ready_master(
    proxy: &Proxy,
    masters: Arc<Mutex<Vec<(SocketAddr, SocketAddr)>>>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let upstream = proxy.upstream.clone();
    std::thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            let candidates = masters.lock().unwrap().clone();
            let ready = candidates
                .into_iter()
                .find(|(_, ops)| ready(*ops))
                .map(|(grpc, _)| grpc);
            *upstream.lock().unwrap() = ready;
            std::thread::sleep(Duration::from_millis(200));
        }
    })
}

/// Whether `GET /readyz` on `ops` answers 200.
fn ready(ops: SocketAddr) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&ops, Duration::from_millis(300)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    if stream
        .write_all(b"GET /readyz HTTP/1.0\r\nHost: localhost\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut answer = String::new();
    let _ = stream.read_to_string(&mut answer);
    answer.starts_with("HTTP/1.1 200") || answer.starts_with("HTTP/1.0 200")
}

/// What the harness did to the cluster, and when: kept in the scenario's bundle (always), so a
/// failure can be read against the roles' logs and the tasks' marks. Times are both since the
/// scenario's start and since the epoch in nanoseconds (as the marks' `date +%s%N`).
pub struct Timeline {
    started: Instant,
    lines: Mutex<Vec<String>>,
}

impl Timeline {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Instant::now(),
            lines: Mutex::new(Vec::new()),
        })
    }

    /// Notes `what`, now (also on stderr, with the test's output).
    pub fn note(&self, what: &str) {
        let line = format!(
            "{:>9.3}s {} {what}",
            self.started.elapsed().as_secs_f64(),
            now_ns()
        );
        eprintln!("chaos: {line}");
        self.lines.lock().unwrap().push(line);
    }

    pub fn text(&self) -> String {
        let mut text = String::new();
        for line in self.lines.lock().unwrap().iter() {
            let _ = writeln!(text, "{line}");
        }
        text
    }
}

/// A time a worker couldn't hear the master or be heard (a cut, one way or both, or the worker
/// frozen), in epoch nanoseconds; `to` is `None` while it lasts.
#[derive(Debug, Clone)]
pub struct Trouble {
    pub worker: String,
    pub from: u128,
    pub to: Option<u128>,
    /// The worker itself was frozen: it could stop none of its tasks.
    pub frozen: bool,
}

/// Each worker's own link to the master, through a proxy of its own, to cut or slow one worker
/// at a time; what was done goes to the timeline.
pub struct Links {
    master: SocketAddr,
    proxies: BTreeMap<String, Proxy>,
    pub timeline: Arc<Timeline>,
    troubles: Vec<Trouble>,
}

impl Links {
    pub fn new(master: SocketAddr, timeline: Arc<Timeline>) -> Self {
        Self {
            master,
            proxies: BTreeMap::new(),
            timeline,
            troubles: Vec::new(),
        }
    }

    /// The master URL for `worker`: its own proxy (made once, kept across restarts).
    pub fn url(&mut self, worker: &str) -> String {
        let master = self.master;
        let proxy = self
            .proxies
            .entry(worker.to_owned())
            .or_insert_with(|| Proxy::new(Some(master)));
        format!("http://127.0.0.1:{}", proxy.port)
    }

    fn proxy(&self, worker: &str) -> &Proxy {
        &self.proxies[worker]
    }

    /// Holds `ways` of `worker`'s link (a silent cut: nothing closes, nothing arrives).
    pub fn cut(&mut self, worker: &str, ways: &[Way]) {
        for way in ways {
            self.proxy(worker).hold(*way, true);
        }
        self.timeline.note(&format!("cut {worker} {ways:?}"));
        self.troubles.push(Trouble {
            worker: worker.to_owned(),
            from: now_ns(),
            to: None,
            frozen: false,
        });
    }

    /// Lets `worker`'s link go both ways again, at full speed.
    pub fn heal(&mut self, worker: &str) {
        let proxy = self.proxy(worker);
        proxy.freeze(false);
        proxy.slow(Duration::ZERO);
        self.timeline.note(&format!("heal {worker}"));
        self.end_trouble(worker);
    }

    /// `worker`'s open connection strands ([`Proxy::strand`]), or its stranded ones pass again.
    pub fn strand(&mut self, worker: &str, stranded: bool) {
        self.proxy(worker).strand(stranded);
        let what = if stranded { "strand" } else { "unstrand" };
        self.timeline.note(&format!("{what} {worker}"));
    }

    /// How many connections `worker`'s proxy has accepted.
    pub fn accepted(&self, worker: &str) -> u64 {
        self.proxy(worker).link.accepted.load(Ordering::SeqCst)
    }

    /// `worker`'s way to the master answers nothing ([`Proxy::silence`]), or does again.
    pub fn silence(&mut self, worker: &str, silent: bool) {
        self.proxy(worker).silence(silent);
        let what = if silent { "silence" } else { "answer" };
        self.timeline.note(&format!("{what} {worker}"));
        if silent {
            self.troubles.push(Trouble {
                worker: worker.to_owned(),
                from: now_ns(),
                to: None,
                frozen: false,
            });
        } else {
            self.end_trouble(worker);
        }
    }

    /// Delays each chunk on `worker`'s link by `delay`, both ways (not a trouble: nothing is
    /// lost, only late).
    pub fn slow(&self, worker: &str, delay: Duration) {
        self.proxy(worker).slow(delay);
        self.timeline
            .note(&format!("slow {worker} by {}ms a chunk", delay.as_millis()));
    }

    /// Freezes the worker's own process (SIGSTOP; its tasks run on), as a node that stops
    /// scheduling it.
    pub fn freeze_worker(&mut self, cluster: &crate::support::Cluster, worker: &str) {
        signal(pid_of(cluster, worker), "STOP");
        self.timeline.note(&format!("freeze {worker}"));
        self.troubles.push(Trouble {
            worker: worker.to_owned(),
            from: now_ns(),
            to: None,
            frozen: true,
        });
    }

    pub fn thaw_worker(&mut self, cluster: &crate::support::Cluster, worker: &str) {
        signal(pid_of(cluster, worker), "CONT");
        self.timeline.note(&format!("thaw {worker}"));
        self.end_trouble(worker);
    }

    fn end_trouble(&mut self, worker: &str) {
        let now = now_ns();
        for trouble in &mut self.troubles {
            if trouble.worker == worker && trouble.to.is_none() {
                trouble.to = Some(now);
            }
        }
    }

    /// Every trouble so far.
    pub fn troubles(&self) -> Vec<Trouble> {
        self.troubles.clone()
    }
}

/// Watches the processes the tasks' marks name (each `start` and `cstart` line's PID) until
/// they are gone: when each was last seen alive, the end of a try that left no `end` mark
/// (stopped by its worker, say), so a cleanup starting before it is caught.
pub struct ProcessWatch {
    stop: Arc<AtomicBool>,
    seen: Arc<Mutex<HashMap<u32, u128>>>,
    thread: Option<JoinHandle<()>>,
}

impl ProcessWatch {
    pub fn new(marks: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(Mutex::new(HashMap::new()));
        let thread = std::thread::spawn({
            let (stop, seen) = (stop.clone(), seen.clone());
            move || watch(&marks, &stop, &seen)
        });
        Self {
            stop,
            seen,
            thread: Some(thread),
        }
    }

    /// Stops watching: each PID's last moment seen alive.
    pub fn finish(mut self) -> HashMap<u32, u128> {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        self.seen.lock().unwrap().clone()
    }
}

fn watch(marks: &std::path::Path, stop: &AtomicBool, seen: &Mutex<HashMap<u32, u128>>) {
    let mut read = 0;
    let mut alive: Vec<u32> = Vec::new();
    while !stop.load(Ordering::SeqCst) {
        let text = std::fs::read(marks).unwrap_or_default();
        // Only whole lines: a line being appended is read next time.
        let end = text
            .iter()
            .rposition(|b| *b == b'\n')
            .map_or(read, |i| i + 1);
        for line in String::from_utf8_lossy(&text[read.min(end)..end]).lines() {
            let fields: Vec<&str> = line.split_whitespace().collect();
            if matches!(fields.first(), Some(&("start" | "cstart")))
                && let Some(pid) = fields.get(4).and_then(|p| p.parse().ok())
            {
                alive.push(pid);
            }
        }
        read = end.max(read);
        let now = now_ns();
        let mut seen = seen.lock().unwrap();
        alive.retain(|pid| {
            let running = std::path::Path::new(&format!("/proc/{pid}")).exists();
            if running {
                seen.insert(*pid, now);
            }
            running
        });
        drop(seen);
        std::thread::sleep(Duration::from_millis(100));
    }
}
