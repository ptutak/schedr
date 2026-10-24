//! What goes wrong on purpose: a pod dying with everything in it, a process frozen, the network
//! to the database stalled, and a Service that sends new connections to whichever master is
//! ready.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

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

/// A TCP proxy on 127.0.0.1: forwards each new connection to the current upstream, and can be
/// frozen (nothing goes through, either way, until thawed), like a stalled network.
pub struct Proxy {
    pub port: u16,
    upstream: Arc<Mutex<Option<SocketAddr>>>,
    frozen: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    accepting: Option<JoinHandle<()>>,
}

impl Proxy {
    pub fn new(upstream: Option<SocketAddr>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let upstream = Arc::new(Mutex::new(upstream));
        let frozen = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let accepting = std::thread::spawn({
            let (upstream, frozen, stop) = (upstream.clone(), frozen.clone(), stop.clone());
            move || accept(&listener, &upstream, &frozen, &stop)
        });
        Self {
            port,
            upstream,
            frozen,
            stop,
            accepting: Some(accepting),
        }
    }

    pub fn freeze(&self, frozen: bool) {
        self.frozen.store(frozen, Ordering::SeqCst);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.frozen.store(false, Ordering::SeqCst);
        if let Some(accepting) = self.accepting.take() {
            let _ = accepting.join();
        }
    }
}

fn accept(
    listener: &TcpListener,
    upstream: &Mutex<Option<SocketAddr>>,
    frozen: &Arc<AtomicBool>,
    stop: &Arc<AtomicBool>,
) {
    while !stop.load(Ordering::SeqCst) {
        let Ok((client, _)) = listener.accept() else {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        };
        let to = *upstream.lock().unwrap();
        let Some(server) = to.and_then(|to| TcpStream::connect(to).ok()) else {
            // Nowhere to go: refused, as a Service with no ready endpoint.
            drop(client);
            continue;
        };
        client.set_nonblocking(false).unwrap();
        for (from, to) in [
            (client.try_clone().unwrap(), server.try_clone().unwrap()),
            (server, client),
        ] {
            let (frozen, stop) = (frozen.clone(), stop.clone());
            std::thread::spawn(move || pump(from, to, &frozen, &stop));
        }
    }
}

/// Copies one direction of a connection, holding what it read while frozen.
fn pump(mut from: TcpStream, mut to: TcpStream, frozen: &AtomicBool, stop: &AtomicBool) {
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = match from.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        while frozen.load(Ordering::SeqCst) && !stop.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(10));
        }
        if to.write_all(&buffer[..read]).is_err() {
            break;
        }
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
