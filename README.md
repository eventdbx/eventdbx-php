# EventDBX PHP SDK (native)

This SDK talks to the EventDBX control socket directly over Cap'n Proto + Noise
via a small Rust native library exposed through PHP FFI.

## Build the native library

```bash
cd native
cargo build --release
```

The compiled library will land in `native/target/release`:

- macOS: `libeventdbx_php_native.dylib`
- Linux: `libeventdbx_php_native.so`
- Windows: `eventdbx_php_native.dll`

If you prefer a debug build, use `cargo build` and point the PHP loader at
`native/target/debug`.

## PHP usage

```php
use EventDbx\Client;

$client = new Client([
    'host' => '127.0.0.1',
    'port' => 6363,
    'token' => getenv('EVENTDBX_TOKEN'),
    // 'tenantId' => 'default',
    // 'noNoise' => true, // only when the server allows plaintext
]);

$page = $client->list('person', ['take' => 10]);
$created = $client->create('person', 'p-1', 'person_registered', [
    'payload' => ['name' => 'Ada'],
    'metadata' => ['@source' => 'sdk-demo'],
]);
$client->apply('person', 'p-1', 'person_updated', [
    'payload' => ['status' => 'active'],
]);
$events = $client->events('person', 'p-1');
$verify = $client->verify('person', 'p-1');
```

All client methods return associative arrays decoded from the JSON responses:

- `list`: `{ items: [...], nextCursor: string|null }`
- `events`: `{ items: [...], nextCursor: string|null }`
- `get`: `{ found: bool, aggregate: mixed }`
- `select`: `{ found: bool, selection: mixed }`
- `create`: `{ aggregate: mixed }`
- `apply` / `patch`: `{ event: mixed }`
- `archive` / `restore`: `{ aggregate: mixed }`
- `verify`: `{ merkleRoot: string }`
- `createSnapshot`: `{ snapshot: mixed }`
- `listSnapshots`: `{ items: [...snapshot rows...] }`
- `getSnapshot`: `{ found: bool, snapshot: mixed }`

### Requirements

- PHP 8.1+ with the `ffi` extension enabled.
- Rust toolchain to build the native library.
- Access to an EventDBX control endpoint and token.
