# fofoca-pipe

The portable byte-pipe embedding: the `pipe_data` / `pipe_eof` wire
contract, the engine driver that implements it, and the join ritual every
pipe peer shares.

Every peer on a pipe mesh must agree on the frame taxonomy bit for bit.
Treat a change here as a wire break.

It is a crate rather than a module so a browser tab and a terminal are
two builds of one copy and cannot drift. `join` also owns mesh-id
resolution: two peers land on the same mesh only if they derive the same
id from the same selectors.

The crate has no features. See `src/lib.rs` for the API: `PipeApp`,
`Session`, `PipeEvent`, and the frame helpers.
