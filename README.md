# tide

A Redis-backed background job queue for MoonBit, inspired by Oban (Elixir).

## Prerequisites

- [MoonBit toolchain](https://www.moonbitlang.com/download/) (`moon`, `moonc`)
- Redis 5+ (for integration tests)
- opam 2.x (for the proof toolchain)

## Setup

### 1. MoonBit toolchain

```bash
# Install or update moon
curl -fsSL https://cli.moonbitlang.com/install/unix.sh | bash
moon version   # moonc v0.9+
```

Install module dependencies:

```bash
moon install
```

### 2. Proof toolchain (why3 + Z3)

`moon prove` requires **why3 1.7.2** and **Z3 4.12.6**. These are separate from the MoonBit toolchain.

#### why3 1.7.2

why3 1.7.2 requires OCaml < 5.4.0. Create a dedicated opam switch so it does not conflict with other OCaml projects:

```bash
# One-time: create a switch with OCaml 5.3.0
opam switch create why3 5.3.0 --yes

# Install why3 1.7.2 into that switch
opam install why3=1.7.2 --switch=why3 --yes
```

Add to `~/.zshrc` (must come after any existing `opam env` line):

```bash
eval $(opam env --switch=why3)
```

#### Z3 4.12.6

why3 1.7.2 and `moonc prove` recognise Z3 up to **4.12.x**. The Homebrew formula tracks 4.15+ which is not recognised, so install the prebuilt binary directly from the Z3 GitHub releases:

```bash
curl -L "https://github.com/Z3Prover/z3/releases/download/z3-4.12.6/z3-4.12.6-arm64-osx-11.0.zip" \
  -o /tmp/z3-4.12.6.zip
unzip /tmp/z3-4.12.6.zip -d /tmp/z3-4.12.6
mkdir -p ~/bin
cp /tmp/z3-4.12.6/z3-4.12.6-arm64-osx-11.0/bin/z3 ~/bin/z3
chmod +x ~/bin/z3
```

Add to `~/.zshrc` so `~/bin` takes priority on PATH:

```bash
export PATH="$HOME/bin:$PATH"
```

#### Configure why3

After both binaries are on PATH, register Z3 with why3:

```bash
why3 config detect   # should print: Found prover Z3 version 4.12.6, OK.
```

#### Verify the full proof toolchain

```bash
why3 --version   # Why3 platform, version 1.7.2
z3 --version     # Z3 version 4.12.6 - 64 bit
moon prove       # all proof goals pass
```

## Development commands

```bash
moon build          # compile all packages
moon test           # run all unit tests
moon prove          # verify proof_assert invariants (requires proof toolchain above)
moon fmt            # format code
moon info           # regenerate .mbti interface files (run after public API changes)
```

`moon test` and `moon prove` must both pass before a change is considered complete.

## Project structure

```
src/
  redis/          # Pure-MoonBit RESP2 client and connection pool
  job/            # Job data model, state machine, hash serialisation
  engine/         # Engine trait + RedisEngine implementation
  tide/           # Public API (Tide.Instance, Config)
```

Each package directory contains:

| File pattern | Purpose |
|---|---|
| `*.mbt` | Source files |
| `*_test.mbt` | Unit tests (same package scope; can access private functions) |
| `*.mbtp` | Proof predicate definitions consumed by `moon prove` |
| `moon.pkg.json` | Package metadata and imports |

## Integration tests

Integration tests require a running Redis instance:

```bash
docker compose up -d redis   # or: redis-server --daemonize yes
moon test --filter integration
```

See `TODO.md` for the full feature roadmap.
