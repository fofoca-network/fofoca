//! The C ABI, driven from Rust through the crate's `rlib`.
//!
//! Every assertion here is about the *boundary* rather than the mesh: NULL
//! handling, the error slot, the length-then-fill buffer convention. It needs no
//! C compiler, so the contract stays covered on any host.
#![expect(
    unsafe_code,
    reason = "the surface under test is `unsafe extern \"C\"`; calling it is the test"
)]

use std::ffi::{CStr, CString, c_char, c_long};
use std::thread;
use std::time::{Duration, Instant};

use fofoca_ffi::ffi::{
    FofocaFrame, FofocaOpts, FofocaPipe, fofoca_close, fofoca_id, fofoca_last_error,
    fofoca_max_chunk, fofoca_name, fofoca_nickname, fofoca_open, fofoca_peer_count,
    fofoca_peers_json, fofoca_recv, fofoca_send, fofoca_state_json, fofoca_state_merge,
    fofoca_version,
};

/// Zeroed selectors: no id and no topic, so [`fofoca_open`] mints a private
/// loopback mesh — no discovery, no network, nothing to clean up but the handle.
fn create_opts(nick: &CStr) -> FofocaOpts {
    FofocaOpts {
        mesh: std::ptr::null(),
        topic: std::ptr::null(),
        nick: nick.as_ptr(),
        name: std::ptr::null(),
        is_public: 0,
        mdns: 0,
        dht: 0,
        relay: 0,
        max_peers: 0,
    }
}

/// The error slot's current contents, as an owned string.
fn last_error() -> Option<String> {
    let ptr = fofoca_last_error();
    if ptr.is_null() {
        return None;
    }
    // SAFETY: non-NULL means the slot holds a live `CString` owned by this
    // thread; it stays valid until this thread's next `mesh_*` call, and the
    // copy below happens before any.
    Some(
        unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned(),
    )
}

fn open(opts: &FofocaOpts) -> *mut FofocaPipe {
    // SAFETY: `opts` is a live, fully-initialized `FofocaOpts` whose string fields
    // are NULL or point at `CStr`s that outlive the call.
    unsafe { fofoca_open(opts) }
}

/// Read a JSON document out through the two-call convention: ask for the length,
/// then fill a buffer of that size.
fn read_json(
    handle: *mut FofocaPipe,
    reader: unsafe extern "C" fn(*mut FofocaPipe, *mut c_char, usize) -> c_long,
) -> String {
    // SAFETY: a NULL buffer with a zero capacity is the documented "how long is
    // it?" query — the callee writes nothing.
    let needed = unsafe { reader(handle, std::ptr::null_mut(), 0) };
    assert!(needed >= 0, "length query failed: {:?}", last_error());
    let mut buf = vec![0_u8; usize::try_from(needed).expect("a sane length") + 1];
    // SAFETY: the buffer is writable for exactly the capacity being passed.
    let written = unsafe { reader(handle, buf.as_mut_ptr().cast::<c_char>(), buf.len()) };
    assert_eq!(written, needed, "the second call must agree on the length");
    buf.truncate(usize::try_from(written).expect("a sane length"));
    String::from_utf8(buf).expect("the document is UTF-8")
}

#[test]
fn version_and_chunk_are_reported() {
    let version = fofoca_version();
    assert!(!version.is_null(), "fofoca_version returned NULL");
    // SAFETY: non-NULL, and the pointer is valid for the process's lifetime.
    let version = unsafe { CStr::from_ptr(version) };
    assert!(
        !version.to_bytes().is_empty(),
        "the version stamp is empty: {version:?}"
    );
    assert!(
        fofoca_max_chunk() > 0,
        "a frame must carry at least one byte of payload"
    );
}

#[test]
fn a_bad_mesh_id_fails_with_a_reason() {
    let nick = CString::new("solo").expect("no interior NUL");
    let bogus = CString::new("not-a-mesh-id").expect("no interior NUL");
    let mut opts = create_opts(&nick);
    opts.mesh = bogus.as_ptr();

    let handle = open(&opts);
    assert!(handle.is_null(), "an unparseable id must not open a handle");
    let error = last_error().expect("a failure must leave a reason in the error slot");
    assert!(
        !error.is_empty(),
        "the error slot holds an empty string rather than a reason"
    );
}

#[test]
fn null_arguments_are_errors_not_crashes() {
    // Each rejected call is checked on the spot: a call clears the error slot on
    // entry, so only the most recent outcome is ever in there.
    // SAFETY: passing NULL is explicitly part of each function's contract — the
    // point of this test is that it is rejected rather than dereferenced.
    unsafe {
        assert!(fofoca_open(std::ptr::null()).is_null());
        assert!(last_error().is_some(), "fofoca_open(NULL) left no reason");

        assert_eq!(
            fofoca_send(std::ptr::null_mut(), std::ptr::null(), [].as_ptr(), 0),
            -1
        );
        assert!(last_error().is_some(), "fofoca_send(NULL) left no reason");

        assert_eq!(
            fofoca_state_merge(std::ptr::null_mut(), std::ptr::null()),
            -1
        );
        assert!(
            last_error().is_some(),
            "fofoca_state_merge(NULL) left no reason"
        );

        assert_eq!(
            fofoca_state_json(std::ptr::null_mut(), std::ptr::null_mut(), 0),
            -1
        );
        assert!(
            last_error().is_some(),
            "fofoca_state_json(NULL) left no reason"
        );

        assert_eq!(fofoca_peer_count(std::ptr::null_mut()), -1);
        assert!(
            last_error().is_some(),
            "fofoca_peer_count(NULL) left no reason"
        );

        assert!(fofoca_id(std::ptr::null()).is_null());
        assert!(fofoca_name(std::ptr::null()).is_null());
        assert!(fofoca_nickname(std::ptr::null()).is_null());

        // Closing NULL is a no-op success, so a caller's cleanup path needs no
        // guard of its own — and succeeding clears the slot.
        assert_eq!(fofoca_close(std::ptr::null_mut()), 0);
        assert!(
            last_error().is_none(),
            "a successful call must clear the error slot"
        );
    }
}

#[test]
fn a_handle_serves_identity_state_and_roster() {
    let nick = CString::new("solo").expect("no interior NUL");
    let opts = create_opts(&nick);
    let handle = open(&opts);
    assert!(!handle.is_null(), "fofoca_open failed: {:?}", last_error());

    // SAFETY: `handle` is live for the rest of this test; the returned pointers
    // are borrowed from it and read before it is closed.
    let (id, mine) = unsafe { (fofoca_id(handle), fofoca_nickname(handle)) };
    assert!(!id.is_null() && !mine.is_null());
    // SAFETY: both are non-NULL NUL-terminated strings owned by the handle.
    let (id, mine) = unsafe { (CStr::from_ptr(id), CStr::from_ptr(mine)) };
    let id_str = id.to_str().expect("the id is UTF-8");
    assert!(
        !id_str.contains("://") && id_str.is_ascii(),
        "expected a bare base58 id, got {id_str:?}"
    );
    assert_eq!(
        mine.to_bytes(),
        b"solo",
        "the requested nickname is in force"
    );

    // A merge is readable back out of the local replica immediately.
    let merge = CString::new(r#"{"probe":"ffi-smoke"}"#).expect("no interior NUL");
    // SAFETY: live handle, NUL-terminated JSON.
    let merged = unsafe { fofoca_state_merge(handle, merge.as_ptr()) };
    assert_eq!(merged, 0, "state_merge failed: {:?}", last_error());
    let state = read_json(handle, fofoca_state_json);
    assert!(
        state.contains("ffi-smoke"),
        "the merged key is missing from the state document: {state}"
    );

    // The roster is the `agent-gossip peers` shape: this node alone, counting itself.
    let peers = read_json(handle, fofoca_peers_json);
    let parsed: serde_json::Value = serde_json::from_str(&peers).expect("the roster is valid JSON");
    assert_eq!(parsed["count"], 1, "a lone member counts itself: {peers}");
    assert_eq!(
        parsed["peers"].as_array().map(Vec::len),
        Some(0),
        "a lone member has no peers: {peers}"
    );

    // `fofoca_peer_count` deliberately answers the other question — how many
    // *others* — so a lone member reads 0 where the JSON's `count` reads 1. A
    // caller waiting for company loops on this one.
    // SAFETY: live handle for the rest of this test.
    let others = unsafe { fofoca_peer_count(handle) };
    assert_eq!(
        others,
        0,
        "a lone member has no peers but itself: {:?}",
        last_error()
    );

    // An undersized buffer must report the length it needs and write nothing.
    let mut too_small = [0_u8; 4];
    // SAFETY: live handle, and the buffer really is 4 bytes.
    let needed = unsafe { fofoca_state_json(handle, too_small.as_mut_ptr().cast::<c_char>(), 4) };
    assert!(
        needed > 4,
        "the state document should not fit in 4 bytes (got {needed})"
    );
    assert_eq!(
        too_small, [0; 4],
        "nothing may be written when the buffer is too small"
    );

    // Nobody else is here, so a receive can only time out — and a timeout is 0,
    // never confused with an end-of-stream frame.
    let mut frame = FofocaFrame {
        nick: [0; 64],
        directed: 0,
        eof: 0,
        len: 0,
    };
    let mut buf = vec![0_u8; fofoca_max_chunk()];
    // SAFETY: live handle, buffer writable for the capacity passed, `out` writable.
    let got = unsafe { fofoca_recv(handle, buf.as_mut_ptr(), buf.len(), 200, &raw mut frame) };
    assert_eq!(got, 0, "expected a timeout, got {got}: {:?}", last_error());

    // SAFETY: the handle came from `fofoca_open` and is not used after this.
    let closed = unsafe { fofoca_close(handle) };
    assert_eq!(closed, 0, "fofoca_close failed: {:?}", last_error());
}

/// The mesh name is decoded from the mesh id itself (not gossiped), so a
/// creator's chosen name is already visible through `fofoca_name()` on a joiner
/// the instant `fofoca_open` returns — no roster wait needed.
#[test]
fn a_joiner_reads_back_the_creators_mesh_name() {
    let creator_nick = CString::new("alice").expect("no interior NUL");
    let creator_name = CString::new("jam-room").expect("no interior NUL");
    let mut create_opts = create_opts(&creator_nick);
    create_opts.name = creator_name.as_ptr();

    let creator = open(&create_opts);
    assert!(
        !creator.is_null(),
        "fofoca_open (create) failed: {:?}",
        last_error()
    );

    // SAFETY: `creator` is live for the rest of this test; the returned
    // pointers are borrowed from it and read before it is closed.
    let (id, name) = unsafe { (fofoca_id(creator), fofoca_name(creator)) };
    assert!(!id.is_null(), "fofoca_id returned NULL");
    assert!(!name.is_null(), "fofoca_name returned NULL");
    // SAFETY: both are non-NULL NUL-terminated strings owned by the handle.
    let (id, name) = unsafe { (CStr::from_ptr(id), CStr::from_ptr(name)) };
    assert_eq!(
        name.to_bytes(),
        b"jam-room",
        "the creator's own fofoca_name must echo what it asked for"
    );
    let id = id.to_owned();

    let joiner_nick = CString::new("bob").expect("no interior NUL");
    let join_opts = create_opts_for_join(&id, &joiner_nick);
    let joiner = open(&join_opts);
    assert!(
        !joiner.is_null(),
        "fofoca_open (join) failed: {:?}",
        last_error()
    );

    // SAFETY: `joiner` is live for the rest of this test.
    let joined_name = unsafe { fofoca_name(joiner) };
    assert!(!joined_name.is_null(), "fofoca_name (joiner) returned NULL");
    // SAFETY: non-NULL NUL-terminated string owned by the handle.
    let joined_name = unsafe { CStr::from_ptr(joined_name) };
    assert_eq!(
        joined_name.to_bytes(),
        b"jam-room",
        "a joiner must read back the same mesh name the creator minted"
    );

    // SAFETY: each handle came from `fofoca_open` and is not used after this.
    unsafe {
        assert_eq!(fofoca_close(joiner), 0, "fofoca_close (joiner) failed");
        assert_eq!(fofoca_close(creator), 0, "fofoca_close (creator) failed");
    }
}

/// Zeroed selectors except `mesh`, so [`fofoca_open`] joins the given id rather
/// than creating a fresh mesh.
fn create_opts_for_join(id: &CStr, nick: &CStr) -> FofocaOpts {
    FofocaOpts {
        mesh: id.as_ptr(),
        topic: std::ptr::null(),
        nick: nick.as_ptr(),
        name: std::ptr::null(),
        is_public: 0,
        mdns: 0,
        dht: 0,
        relay: 0,
        max_peers: 0,
    }
}

/// Four peers (creator + 3 joiners) must each see the other three.
/// Regression for rooms that stalled at 3 total participants.
fn four_peers_converge(public: bool) {
    let creator_nick = CString::new("p0").expect("no interior NUL");
    let mut opts = create_opts(&creator_nick);
    if public {
        // Match mallorca's default create-room discovery flags.
        opts.is_public = 1;
        opts.mdns = 1;
        opts.dht = 1;
        opts.relay = 1;
    }
    let creator = open(&opts);
    assert!(
        !creator.is_null(),
        "fofoca_open (create) failed: {:?}",
        last_error()
    );

    // SAFETY: creator is live until the end of the test.
    let id_ptr = unsafe { fofoca_id(creator) };
    assert!(!id_ptr.is_null(), "fofoca_id returned NULL");
    // SAFETY: non-NULL NUL-terminated string owned by the handle.
    let id = unsafe { CStr::from_ptr(id_ptr) }.to_owned();

    let joiner_nicks = [
        CString::new("p1").expect("no interior NUL"),
        CString::new("p2").expect("no interior NUL"),
        CString::new("p3").expect("no interior NUL"),
    ];
    let mut handles = vec![creator];
    for nick in &joiner_nicks {
        let handle = open(&create_opts_for_join(&id, nick));
        assert!(
            !handle.is_null(),
            "fofoca_open (join {}) failed: {:?}",
            nick.to_string_lossy(),
            last_error()
        );
        handles.push(handle);
    }

    let deadline = Instant::now() + Duration::from_secs(45);
    let mut counts = vec![0_i64; handles.len()];
    while Instant::now() < deadline {
        for (idx, &handle) in handles.iter().enumerate() {
            // SAFETY: each handle came from fofoca_open and is still open.
            counts[idx] = unsafe { fofoca_peer_count(handle) };
        }
        if counts.iter().all(|&count| count >= 3) {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }

    assert!(
        counts.iter().all(|&count| count >= 3),
        "expected every peer to see ≥3 others within 45s (public={public}); peer_counts={counts:?}"
    );

    // SAFETY: each handle came from fofoca_open and is not used after this.
    for handle in handles {
        unsafe {
            assert_eq!(
                fofoca_close(handle),
                0,
                "fofoca_close failed: {:?}",
                last_error()
            );
        }
    }
}

#[test]
fn four_peers_converge_on_loopback() {
    four_peers_converge(false);
}

#[test]
fn four_peers_converge_on_public() {
    four_peers_converge(true);
}
