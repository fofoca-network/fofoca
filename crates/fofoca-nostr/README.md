# fofoca-nostr

A small Nostr relay client. fofoca uses it to find peers and to carry the WebRTC
offer and answer when the iroh relay is not available.

- **Events.** Events are NIP-01 events with ephemeral kinds (20000–29999). Relays
  forward these kinds and store nothing. Each event carries one `x` tag. A
  random BIP-340 key for each pool signs the events. This key satisfies the
  relay and proves nothing more. The engine seals and signs its own payload
  inside `content`.
- **Sockets.** A process opens one socket for each relay URL, and all pools
  share it.
- **Relay choice.** A `Pool` holds the healthiest `width` relays of a ranked
  list. A pool at width 1 and a pool at width 3 on the same ranking still
  overlap.
- **Relay health.** The rules are the same as Trystero's:
  - `rate-limited:` backs off from 1 min, doubling up to 15 min.
  - `blocked:`, `restricted:`, `auth-required:` and `pow:` retire the relay for
    the life of the process.
  - A dropped socket reconnects, starting at 1 s and doubling up to 1 min. The
    delay resets only after a socket stays up for 30 s.
  - A relay that owes us the echo of our own event for 10 s stops counting
    toward the width, because it accepts events and forwards none.
  - A relay that is dialing counts as usable for up to 10 s. Until then, a
    pool on dead relays opens no spares.

## Probe

`cargo run -p fofoca-nostr --example probe` tests which public relays forward
an ephemeral event between two subscribers.

On 2026-09-24:
- 20 of 32 candidates forwarded the event, with round trips from 57 ms to 1.1 s.
- With `probe cadence`, 10 nodes published 90 events in 30 s on one tag:
  - `nos.lol` and `relay.primal.net` delivered 81 of 81 events to every node.
  - `relay.damus.io` delivered 4–5 of 81.

## Tests

`cargo test -p fofoca-nostr` runs against `test_relay::TestRelay`, an
in-process relay (feature `test-relay`). It checks ids and signatures, filters
events like a real relay, and can refuse, swallow events, or drop connections.

A few tests wait on real timers: backoff, the echo deadline, and a relay that
hangs up. So the suite takes about a minute. The sleeps are what the tests
measure, so do not shorten them.
