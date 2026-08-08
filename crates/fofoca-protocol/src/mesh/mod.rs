//! The mesh identifier, at two levels:
//!
//! - [`MeshId`] — the validated bare `Base58Check` string. Possession proves
//!   the checksum, version, and full payload structure. Code: [`id`].
//! - [`Mesh`] — the *decoded* structure (32-byte seed + name +
//!   [`MeshConfig`]), with the `Base58Check` codec (this file). `MeshId`
//!   is what flows through the wire/CLI; `Mesh` is what `setup_mesh`
//!   derives identity and behavior from.
//!
//! See `docs/mesh-hash.md` for the full byte layout and rationale.
//!
//! Also home to [`MeshName`] ([`name`]), the lookup allowlist ([`lookup`])
//! carried in the id, and the relay-ladder parsing.

use std::fmt;
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use iroh_base::{EndpointId, SecretKey};
use rand::RngCore;

use super::crypto;
use crate::base58check;

mod id;
mod lookup;
mod name;

pub use id::{MeshId, MeshIdError};
pub use lookup::{
    AdvertiseRequiresReachable, DEFAULT_DIRECTORY, DirectorySelection, LookupOpts, MeshConfig,
    RelayChoice, resolve_lookups, validate_advertise,
};
pub use lookup::{LookupSet, RelayLadder, RelayLadderError, RelaySelection};
pub use name::{MeshName, NameError};

/// Id format version. A single byte reserved so the encoding can evolve;
/// an unknown version is rejected.
const VERSION: u8 = 1;
const SEED_LEN: usize = 32;
/// Wire bound for the encoded name in bytes. `MeshName::new` caps the
/// name at `ident::MAX_CHARS` scalar values; each is at most 4 UTF-8
/// bytes, so the encoded form fits this many bytes (and inside the
/// 1-byte length field).
const NAME_MAX_BYTES: usize = super::ident::MAX_CHARS * 4;

/// A mesh identifier — bare `Base58Check` payload.
///
/// The token carries the random `seed` plus the mesh's [`MeshConfig`]
/// (lookups); **no peer address is ever stored**. The
/// gossip topic, the well-known rendezvous identity (every joiner's
/// bootstrap target), and the loopback port ladder are all derived from
/// `seed` in memory, so the mesh is creator-independent and survives
/// the creator's death. The config is mixed into the topic derivation,
/// so every member of a mesh shares the same config.
///
/// Wire format (little-endian):
///
/// ```text
/// [1 byte version]
/// [32 bytes seed]
/// [1 byte name length in bytes, 1..=128]
/// [N bytes name (UTF-8, <=32 scalars, charset enforced by MeshName)]
/// [2 bytes config length] [config bytes (see MeshConfig::to_bytes)]
/// ```
#[derive(Clone)]
pub struct Mesh {
    pub name: MeshName,
    seed: [u8; SEED_LEN],
    /// The Argon2id-stretched password key, once a password is applied.
    /// Every derivation (topic, rendezvous, port ladder) switches onto it,
    /// so holding the id without the password computes nothing reachable.
    /// Never serialized — `encode_bytes` writes `seed`.
    stretched_key: Option<[u8; SEED_LEN]>,
    /// The invite-only derivation secret (the "root"), held only in memory:
    /// baked into no hash, carried only by creator-minted invites. When set,
    /// every derivation switches onto it (see [`Self::effective_seed`]), so the
    /// bare invite-only hash reaches nothing. Set on the creator by
    /// [`Self::set_invite`] and on a redeemer by [`Self::apply_invite`].
    invite_key: Option<[u8; SEED_LEN]>,
    /// The Ed25519 issuer **private** key of an invite-only mesh — the mint
    /// authority, held only by the creator's live session (in-memory only, so a
    /// restart ends minting). `None` on every non-creator. The matching public
    /// key rides the hash (`config.issuer_pubkey`).
    issuer_secret: Option<[u8; 32]>,
    pub config: MeshConfig,
}

impl fmt::Debug for Mesh {
    // Redact all secret material — the seed *is* the join capability, and the
    // invite root / issuer secret gate an invite-only mesh — so a stray `{:?}`
    // in a log can never leak them (mirrors `crypto::Password`).
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Mesh")
            .field("name", &self.name)
            .field("seed", &"***")
            .field("stretched_key", &self.stretched_key.map(|_| "***"))
            .field("invite_key", &self.invite_key.map(|_| "***"))
            .field("issuer_secret", &self.issuer_secret.map(|_| "***"))
            .field("config", &self.config)
            .finish()
    }
}

impl Mesh {
    #[must_use]
    pub fn new(seed: [u8; SEED_LEN], name: MeshName, config: MeshConfig) -> Self {
        Mesh {
            name,
            seed,
            stretched_key: None,
            invite_key: None,
            issuer_secret: None,
            config,
        }
    }

    /// Build a mesh deterministically from an arbitrary string (the `topic`
    /// command). The seed is [`crypto::topic_seed`] of the string; the name is
    /// the string itself sanitized into a [`MeshName`]
    /// ([`MeshName::from_topic_string`]). Both are deterministic functions of
    /// the string, so callers passing the same string + config converge on the
    /// same id with zero coordination.
    #[must_use]
    pub fn from_topic(topic: &str, config: MeshConfig) -> Self {
        let seed = crypto::topic_seed(topic);
        let name = MeshName::from_topic_string(topic);
        Mesh {
            name,
            seed,
            stretched_key: None,
            invite_key: None,
            issuer_secret: None,
            config,
        }
    }

    /// The wire seed (what the encoded id carries). Test-only: production
    /// derivations go through [`Mesh::effective_seed`] so the password mix
    /// can never be bypassed.
    #[cfg(test)]
    pub(crate) fn seed(&self) -> &[u8; SEED_LEN] {
        &self.seed
    }

    /// The seed every derivation uses: the invite root for an invite-only
    /// mesh, the stretched password key when a password is applied, else the
    /// wire seed. Asserts the secret is present before deriving — deriving from
    /// the raw seed would silently land in an unreachable topic, strictly worse
    /// than failing loudly. (Invite-only and password are mutually exclusive in
    /// the id: an invite-only mesh's password, if any, only wraps its invite
    /// tickets — it never folds into the topic here.)
    fn effective_seed(&self) -> &[u8; SEED_LEN] {
        if self.requires_invite() {
            return self
                .invite_key
                .as_ref()
                .expect("invite-only mesh derived before an invite was applied");
        }
        if let Some(key) = &self.stretched_key {
            return key;
        }
        assert!(
            self.config.password.is_none(),
            "passworded mesh derived before the password was applied"
        );
        &self.seed
    }

    /// Whether the id carries a password verifier (joiners must present
    /// the password).
    #[must_use]
    pub fn requires_password(&self) -> bool {
        self.config.password.is_some()
    }

    /// The Argon2id-stretched password key, once a password has been applied
    /// (`set_password`/`apply_password`) — `None` for a passwordless mesh. The
    /// daemon retains this to key the state/meta channel encryption; it is the
    /// same value every derivation switches onto ([`Self::effective_seed`]).
    #[must_use]
    pub fn stretched_key(&self) -> Option<[u8; SEED_LEN]> {
        self.stretched_key
    }

    /// Join-side: stretch `password` (salt = the wire seed), check it
    /// against the verifier the id carries, and switch every derivation
    /// onto the stretched key. ~100ms of Argon2id by design.
    ///
    /// # Errors
    /// The id carries no verifier (passwordless mesh), or the password is
    /// wrong.
    pub fn apply_password(&mut self, password: &crypto::Password) -> Result<()> {
        let Some(expected) = &self.config.password else {
            bail!("this mesh has no password — drop --password");
        };
        let key = crypto::stretch_mesh_password(password, &self.seed);
        if crypto::password_verifier(&key) != *expected {
            bail!("wrong password");
        }
        self.stretched_key = Some(key);
        Ok(())
    }

    /// Create-side: stretch `password`, bake its verifier into the config
    /// (the encoded id must carry it), and switch every derivation onto
    /// the stretched key. ~100ms of Argon2id by design.
    pub fn set_password(&mut self, password: &crypto::Password) {
        let key = crypto::stretch_mesh_password(password, &self.seed);
        self.config.password = Some(crypto::password_verifier(&key));
        self.stretched_key = Some(key);
    }

    /// Whether this mesh is invite-only (the id carries an issuer pubkey, so
    /// the bare hash cannot join — a creator-minted invite is required).
    #[must_use]
    pub fn requires_invite(&self) -> bool {
        self.config.issuer_pubkey.is_some()
    }

    /// Create-side: turn this into an invite-only mesh. Mints a random invite
    /// **root** (the derivation secret, held only in memory and in the invites
    /// this creator issues) and an Ed25519 **issuer** keypair (the mint
    /// authority), baking the issuer public key into the id. Both secrets stay
    /// in-memory only, so a creator restart ends minting.
    pub fn set_invite(&mut self) {
        let mut root = [0u8; SEED_LEN];
        rand::rng().fill_bytes(&mut root);
        let mut issuer = [0u8; 32];
        rand::rng().fill_bytes(&mut issuer);
        let issuer_pubkey = SecretKey::from_bytes(&issuer).public();
        self.config.issuer_pubkey = Some(*issuer_pubkey.as_bytes());
        self.invite_key = Some(root);
        self.issuer_secret = Some(issuer);
    }

    /// Redeem-side: switch every derivation onto the invite `root` carried by a
    /// verified invite ticket. The redeemer holds no issuer secret, so it can
    /// join but never mint.
    pub(crate) fn apply_invite(&mut self, root: [u8; SEED_LEN]) {
        self.invite_key = Some(root);
    }

    /// The issuer private key, for signing a minted invite (creator only).
    pub(crate) fn issuer_secret(&self) -> Option<&[u8; 32]> {
        self.issuer_secret.as_ref()
    }

    /// The issuer public key the id carries — a redeemer verifies each invite's
    /// signature against it.
    pub(crate) fn issuer_pubkey(&self) -> Option<&[u8; 32]> {
        self.config.issuer_pubkey.as_ref()
    }

    /// The invite root secret to embed in a minted invite (creator only).
    pub(crate) fn invite_key(&self) -> Option<&[u8; SEED_LEN]> {
        self.invite_key.as_ref()
    }

    /// The public identity salt (the wire seed) an invite ticket wraps its root
    /// under when password-protected — public and in the hash, so both minter
    /// and redeemer derive the same wrap key from the password alone.
    pub(crate) fn wrap_salt(&self) -> &[u8; SEED_LEN] {
        &self.seed
    }

    /// The gossip `TopicId` — the one derivation entry point, so no caller
    /// can forget the password mix.
    #[must_use]
    pub fn topic_id(&self) -> crate::topic::TopicId {
        crypto::derive_topic_id(self.effective_seed(), &self.name, &self.config_bytes())
    }

    /// The shared rendezvous endpoint secret every co-host binds.
    #[must_use]
    pub fn rendezvous_secret(&self) -> SecretKey {
        crypto::rendezvous_secret(self.effective_seed())
    }

    #[must_use]
    pub fn lookups(&self) -> &LookupOpts {
        &self.config.lookups
    }

    /// True when the mesh is loopback-only (no off-machine lookups).
    #[must_use]
    pub fn is_loopback(&self) -> bool {
        self.config.lookups.is_loopback()
    }

    /// `"private"`/`"public"` label for output, derived from the lookups.
    #[must_use]
    pub fn network_label(&self) -> &'static str {
        self.config.lookups.network_label()
    }

    /// The canonical config bytes mixed into the topic derivation (so a
    /// mesh's config is part of its identity).
    pub(crate) fn config_bytes(&self) -> Vec<u8> {
        self.config.to_bytes()
    }

    /// Well-known rendezvous `EndpointId`, derived from the effective seed.
    /// Every joiner computes this locally and bootstraps gossip from it; it
    /// is co-hosted by members rather than pinned to the creator.
    #[must_use]
    pub fn rendezvous_id(&self) -> EndpointId {
        crypto::rendezvous_id(self.effective_seed())
    }

    /// Deterministic loopback port *ladder* for loopback-only meshes (no
    /// pkarr/DNS to resolve `rendezvous_id`). Preference order; a beacon
    /// binds the first free rung, joiners try all rungs.
    #[must_use]
    pub fn rendezvous_ports(&self) -> [u16; crypto::RENDEZVOUS_LADDER] {
        crypto::rendezvous_ports(self.effective_seed())
    }

    fn encode_bytes(&self) -> Vec<u8> {
        let config = self.config.to_bytes();
        let mut buf =
            Vec::with_capacity(1 + SEED_LEN + 1 + self.name.as_bytes().len() + 2 + config.len());
        buf.push(VERSION);
        buf.extend_from_slice(&self.seed);
        // MeshName guarantees 1..=128 UTF-8 bytes, so a 1-byte length is safe.
        buf.push(self.name.len_u8());
        buf.extend_from_slice(self.name.as_bytes());
        let config_len =
            u16::try_from(config.len()).expect("MeshConfig encodes well within a u16 length");
        buf.extend_from_slice(&config_len.to_le_bytes());
        buf.extend_from_slice(&config);
        buf
    }

    fn decode_bytes(bytes: &[u8]) -> Result<Self> {
        let mut pos = 0usize;
        let version = *bytes.get(pos).context("Mesh identifier too short")?;
        pos += 1;
        if version != VERSION {
            bail!("Unsupported mesh id version: {version}");
        }

        let seed_slice = bytes
            .get(pos..pos + SEED_LEN)
            .context("Mesh identifier too short")?;
        let mut seed = [0u8; SEED_LEN];
        seed.copy_from_slice(seed_slice);
        pos += SEED_LEN;

        let name_len = *bytes.get(pos).context("Truncated mesh name length")? as usize;
        pos += 1;
        if name_len == 0 || name_len > NAME_MAX_BYTES {
            bail!("Invalid mesh name length: {name_len}");
        }
        let name_raw = bytes
            .get(pos..pos + name_len)
            .context("Truncated mesh name")?;
        pos += name_len;
        let name_str = std::str::from_utf8(name_raw).context("Invalid mesh name UTF-8")?;
        let name = MeshName::new(name_str).context("Invalid mesh name")?;

        let config_len =
            lookup::read_u16(bytes, &mut pos).context("Truncated config length")? as usize;
        let config_raw = bytes
            .get(pos..pos + config_len)
            .context("Truncated mesh config")?;
        pos += config_len;
        if pos != bytes.len() {
            bail!("Trailing bytes in mesh identifier");
        }
        let config = MeshConfig::from_bytes(config_raw)?;

        Ok(Mesh {
            name,
            seed,
            stretched_key: None,
            invite_key: None,
            issuer_secret: None,
            config,
        })
    }
}

impl fmt::Display for Mesh {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.encode_bytes();
        write!(f, "{}", base58check::encode(&bytes))
    }
}

impl FromStr for Mesh {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let bytes = base58check::decode(s, "mesh identifier")?;
        Self::decode_bytes(&bytes)
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod mesh_tests;
