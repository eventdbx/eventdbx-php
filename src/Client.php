<?php

declare(strict_types=1);

namespace EventDbx;

use EventDbx\Exception\EventDbxException;
use FFI;
use FFI\CData;

final class Client
{
    private const CDEF = <<<CDEF
    typedef struct DbxHandle DbxHandle;

    void dbx_string_free(char* ptr);

    DbxHandle* dbx_client_new(const char* config_json, char** error_out);
    void dbx_client_free(DbxHandle* handle);

    char* dbx_list_aggregates(DbxHandle* handle, const char* aggregate_type, const char* options_json, char** error_out);
    char* dbx_get_aggregate(DbxHandle* handle, const char* aggregate_type, const char* aggregate_id, char** error_out);
    char* dbx_select_aggregate(DbxHandle* handle, const char* aggregate_type, const char* aggregate_id, const char* fields_json, char** error_out);
    char* dbx_list_events(DbxHandle* handle, const char* aggregate_type, const char* aggregate_id, const char* options_json, char** error_out);
    char* dbx_append_event(DbxHandle* handle, const char* aggregate_type, const char* aggregate_id, const char* event_type, const char* options_json, char** error_out);
    char* dbx_create_aggregate(DbxHandle* handle, const char* aggregate_type, const char* aggregate_id, const char* event_type, const char* options_json, char** error_out);
    char* dbx_patch_event(DbxHandle* handle, const char* aggregate_type, const char* aggregate_id, const char* event_type, const char* patch_json, const char* options_json, char** error_out);
    char* dbx_set_archive(DbxHandle* handle, const char* aggregate_type, const char* aggregate_id, bool archived, const char* options_json, char** error_out);
    char* dbx_verify_aggregate(DbxHandle* handle, const char* aggregate_type, const char* aggregate_id, char** error_out);
    CDEF;

    private FFI $ffi;
    private CData $handle;

    /**
     * @param array<string,mixed> $config
     */
    public function __construct(array $config, ?string $libraryPath = null)
    {
        $lib = $libraryPath ?? self::defaultLibraryPath();
        if (!is_file($lib)) {
            throw new EventDbxException("Native library not found at {$lib}. Build it with `cargo build --release` inside native/.");
        }

        $this->ffi = FFI::cdef(self::CDEF, $lib);
        $configJson = $this->encode($config);

        $error = $this->ffi->new('char*');
        $handle = $this->ffi->dbx_client_new($configJson, FFI::addr($error));
        $this->throwIfError($error);
        if (FFI::isNull($handle)) {
            throw new EventDbxException('Failed to create EventDBX client (no handle returned)');
        }
        $this->handle = $handle;
    }

    public function __destruct()
    {
        if (isset($this->handle) && !FFI::isNull($this->handle)) {
            $this->ffi->dbx_client_free($this->handle);
        }
    }

    /**
     * @param array<string,mixed> $options
     */
    public function list(string $aggregateType = '', array $options = []): array
    {
        return $this->callJson(
            'dbx_list_aggregates',
            $aggregateType,
            $this->encode($options),
        );
    }

    public function get(string $aggregateType, string $aggregateId): array
    {
        return $this->callJson(
            'dbx_get_aggregate',
            $aggregateType,
            $aggregateId,
        );
    }

    /**
     * @param list<string> $fields
     */
    public function select(string $aggregateType, string $aggregateId, array $fields): array
    {
        return $this->callJson(
            'dbx_select_aggregate',
            $aggregateType,
            $aggregateId,
            $this->encode($fields),
        );
    }

    /**
     * @param array<string,mixed> $options
     */
    public function events(string $aggregateType, string $aggregateId, array $options = []): array
    {
        return $this->callJson(
            'dbx_list_events',
            $aggregateType,
            $aggregateId,
            $this->encode($options),
        );
    }

    /**
     * @param array<string,mixed> $options
     */
    public function apply(string $aggregateType, string $aggregateId, string $eventType, array $options = []): array
    {
        return $this->callJson(
            'dbx_append_event',
            $aggregateType,
            $aggregateId,
            $eventType,
            $this->encode($options),
        );
    }

    /**
     * @param array<string,mixed> $options
     */
    public function create(string $aggregateType, string $aggregateId, string $eventType, array $options = []): array
    {
        return $this->callJson(
            'dbx_create_aggregate',
            $aggregateType,
            $aggregateId,
            $eventType,
            $this->encode($options),
        );
    }

    /**
     * @param array<int,array<string,mixed>> $patch
     * @param array<string,mixed> $options
     */
    public function patch(string $aggregateType, string $aggregateId, string $eventType, array $patch, array $options = []): array
    {
        return $this->callJson(
            'dbx_patch_event',
            $aggregateType,
            $aggregateId,
            $eventType,
            $this->encode($patch),
            $this->encode($options),
        );
    }

    /**
     * @param array<string,mixed> $options
     */
    public function archive(string $aggregateType, string $aggregateId, array $options = []): array
    {
        return $this->callJson(
            'dbx_set_archive',
            $aggregateType,
            $aggregateId,
            true,
            $this->encode($options),
        );
    }

    /**
     * @param array<string,mixed> $options
     */
    public function restore(string $aggregateType, string $aggregateId, array $options = []): array
    {
        return $this->callJson(
            'dbx_set_archive',
            $aggregateType,
            $aggregateId,
            false,
            $this->encode($options),
        );
    }

    public function verify(string $aggregateType, string $aggregateId): array
    {
        return $this->callJson(
            'dbx_verify_aggregate',
            $aggregateType,
            $aggregateId,
        );
    }

    private function encode(mixed $value): string
    {
        $json = json_encode($value);
        if ($json === false) {
            throw new EventDbxException('Failed to encode request payload to JSON');
        }
        return $json;
    }

    /**
     * @param string $function
     * @param mixed ...$args
     */
    private function callJson(string $function, ...$args): array
    {
        $error = $this->ffi->new('char*');
        $callArgs = array_merge([$this->handle], $args, [FFI::addr($error)]);
        $jsonPtr = $this->ffi->{$function}(...$callArgs);
        $this->throwIfError($error);

        if ($jsonPtr === null || FFI::isNull($jsonPtr)) {
            throw new EventDbxException("{$function} returned no data");
        }

        $json = FFI::string($jsonPtr);
        $this->ffi->dbx_string_free($jsonPtr);

        $decoded = json_decode($json, true);
        if ($decoded === null && json_last_error() !== JSON_ERROR_NONE) {
            throw new EventDbxException("Failed to decode response JSON: " . json_last_error_msg());
        }

        return $decoded ?? [];
    }

    private function throwIfError(CData $errorPtr): void
    {
        if (FFI::isNull($errorPtr)) {
            return;
        }
        $message = FFI::string($errorPtr);
        $this->ffi->dbx_string_free($errorPtr);
        if ($message !== '') {
            throw new EventDbxException($message);
        }
    }

    private static function defaultLibraryPath(): string
    {
        $root = dirname(__DIR__) . '/native/target';
        $names = [];
        if (stripos(PHP_OS_FAMILY, 'Windows') === 0) {
            $names[] = 'release/eventdbx_php_native.dll';
            $names[] = 'debug/eventdbx_php_native.dll';
        } elseif (stripos(PHP_OS_FAMILY, 'Darwin') === 0) {
            $names[] = 'release/libeventdbx_php_native.dylib';
            $names[] = 'debug/libeventdbx_php_native.dylib';
        } else {
            $names[] = 'release/libeventdbx_php_native.so';
            $names[] = 'debug/libeventdbx_php_native.so';
        }

        foreach ($names as $name) {
            $candidate = $root . '/' . $name;
            if (is_file($candidate)) {
                return $candidate;
            }
        }

        // fallback to the first candidate for clearer error messages
        return $root . '/' . $names[0];
    }
}
