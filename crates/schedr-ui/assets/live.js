// schedr's live pages, one stream per browser (PROJECT.md, decision 175): a SharedWorker,
// started by a browser's first live page of this UI and ended with its last, holds the UI's
// `/events` stream and hands every change to each page, which decides which concern it.
//
// A page connects, then says when it was made (`{since}`, the UI's clock). The worker answers
// what changed since: the changes it kept (a minute's), or `refresh` when it doesn't know
// (its stream opened after the page was made, but for the first page, whose `since` the
// stream itself was opened with). When the stream opens again after it dropped, every page
// gets `refresh`. A page going away says `{bye: true}`; one that can't (it crashed, or was
// discarded) is let go once it hasn't said `{alive: true}` (every 30 s) for 3 minutes: a
// hidden page's timers may run only once a minute. One let go that speaks again (it was
// frozen) is taken back, with `refresh`.
"use strict";
var base = self.location.pathname.replace(/\/assets\/live\.js$/, "");
var pages = [];
var kept = [];
var opened = null;
var source = null;
var SILENT = 180000;

function send(page, message) {
  try { page.postMessage(message); } catch (e) {}
}
function all(message) {
  pages.forEach(function (page) { send(page.port, message); });
}
// What changed since `since`, for a page made then.
function answer(page) {
  if (page.covered) return;
  page.covered = true;
  if (opened === null || page.since < opened) {
    send(page.port, { type: "change", change: { kind: "refresh" } });
    return;
  }
  kept.forEach(function (change) {
    if (change.at > page.since) send(page.port, { type: "change", change: change });
  });
}
function close() {
  if (source === null) return;
  // The last page went: closed, opened again by the next one.
  source.close();
  source = null;
  opened = null;
  kept = [];
}
function leave(page) {
  pages = pages.filter(function (p) { return p !== page; });
  if (pages.length === 0) close();
}
function join(page) {
  pages.push(page);
  if (source === null) {
    // The stream starts from this page's moment: what it missed comes as `refresh`.
    page.covered = true;
    open(page.since);
  } else if (opened !== null) {
    answer(page);
  }
}
setInterval(function () {
  var now = Date.now();
  pages.filter(function (p) { return now - p.heard >= SILENT; }).forEach(leave);
}, 30000);
function open(since) {
  source = new EventSource(base + "/events?since=" + since, { withCredentials: true });
  source.addEventListener("hello", function (event) {
    var now = Number(event.data);
    if (opened !== null) {
      // Opened again: what changed while it was down isn't known.
      kept = [];
      all({ type: "change", change: { kind: "refresh" } });
    }
    opened = now;
    pages.forEach(answer);
  });
  source.addEventListener("change", function (event) {
    var change = JSON.parse(event.data);
    kept.push(change);
    var old = opened !== null ? change.at - 60000 : 0;
    while (kept.length > 1000 || kept[0].at < old) kept.shift();
    all({ type: "change", change: change });
  });
  source.addEventListener("ping", function () { all({ type: "ping" }); });
}

self.onconnect = function (event) {
  var port = event.ports[0];
  var page = { port: port, since: 0, covered: false, heard: Date.now(), joined: false };
  port.onmessage = function (message) {
    var data = message.data || {};
    page.heard = Date.now();
    if (data.bye) {
      page.joined = false;
      leave(page);
      return;
    }
    if (data.alive) {
      if (page.joined && pages.indexOf(page) < 0) {
        // Let go while it was silent: what changed meanwhile isn't known to it.
        page.since = Date.now();
        page.covered = true;
        join(page);
        send(port, { type: "change", change: { kind: "refresh" } });
      }
      return;
    }
    page.since = Number(data.since) || 0;
    if (typeof EventSource === "undefined") {
      // No streams in workers here: the page holds its own.
      send(port, { type: "alone" });
      return;
    }
    page.joined = true;
    join(page);
  };
  port.start();
};
