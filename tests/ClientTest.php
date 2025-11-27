<?php

declare(strict_types=1);

namespace EventDbx\Tests;

use EventDbx\Client;
use EventDbx\Exception\EventDbxException;
use PHPUnit\Framework\TestCase;

final class ClientTest extends TestCase
{
    private static string $libraryPath;

    public static function setUpBeforeClass(): void
    {
        self::$libraryPath = self::compileNativeStub();
    }

    private static function compileNativeStub(): string
    {
        if (PHP_OS_FAMILY === 'Windows') {
            self::markTestSkipped('Native stub compilation is not supported on Windows for this test suite.');
        }

        $compiler = trim((string) shell_exec('command -v cc'));
        if ($compiler === '') {
            self::markTestSkipped('No C compiler found to build the native stub.');
        }

        $source = __DIR__ . '/Fixtures/eventdbx_native_stub.c';
        $buildDir = sys_get_temp_dir() . '/eventdbx-php-tests';
        if (!is_dir($buildDir) && !mkdir($buildDir, 0777, true) && !is_dir($buildDir)) {
            self::fail('Unable to create build directory for native stub.');
        }

        $extension = PHP_OS_FAMILY === 'Darwin' ? 'dylib' : 'so';
        $libraryPath = $buildDir . '/libeventdbx_php_native_stub.' . $extension;

        if (!is_file($libraryPath) || filemtime($libraryPath) < filemtime($source)) {
            $format = PHP_OS_FAMILY === 'Darwin'
                ? '%s -dynamiclib -o %s %s'
                : '%s -shared -fPIC -o %s %s';

            $command = sprintf(
                $format,
                escapeshellcmd($compiler),
                escapeshellarg($libraryPath),
                escapeshellarg($source),
            );

            $output = [];
            exec($command, $output, $exitCode);
            if ($exitCode !== 0) {
                self::fail("Failed to compile stub library: " . implode("\n", $output));
            }
        }

        return $libraryPath;
    }

    private function createClient(array $config = ['dsn' => 'memory']): Client
    {
        return new Client($config, self::$libraryPath);
    }

    public function testThrowsWhenLibraryIsMissing(): void
    {
        $this->expectException(EventDbxException::class);
        $this->expectExceptionMessage('Native library not found');

        new Client(['dsn' => 'anything'], '/path/to/nowhere/libeventdbx_php_native.so');
    }

    public function testConstructorPropagatesNativeError(): void
    {
        $this->expectException(EventDbxException::class);
        $this->expectExceptionMessage('config failure from stub library');

        new Client(['mode' => 'config-error'], self::$libraryPath);
    }

    public function testGetReturnsDecodedResponse(): void
    {
        $client = $this->createClient();

        $result = $client->get('order', '123');

        $this->assertSame('dbx_get_aggregate', $result['function']);
        $this->assertSame('order', $result['aggregate_type']);
        $this->assertSame('123', $result['aggregate_id']);
    }

    public function testArchiveAndRestoreToggleArchiveFlag(): void
    {
        $client = $this->createClient();

        $archived = $client->archive('order', '42');
        $restored = $client->restore('order', '42');

        $this->assertTrue($archived['archived']);
        $this->assertFalse($restored['archived']);
    }

    public function testNativeErrorsBubbleUp(): void
    {
        $client = $this->createClient();

        $this->expectException(EventDbxException::class);
        $this->expectExceptionMessage('native error from stub library');

        $client->apply('order', 'native-error', 'created', ['foo' => 'bar']);
    }

    public function testNullResponseThrows(): void
    {
        $client = $this->createClient();

        $this->expectException(EventDbxException::class);
        $this->expectExceptionMessage('dbx_list_events returned no data');

        $client->events('order', 'no-data');
    }

    public function testInvalidJsonResponseThrows(): void
    {
        $client = $this->createClient();

        $this->expectException(EventDbxException::class);
        $this->expectExceptionMessage('Failed to decode response JSON');

        $client->get('order', 'bad-json');
    }

    public function testApplyThrowsWhenOptionsCannotBeEncoded(): void
    {
        $client = $this->createClient();

        $this->expectException(EventDbxException::class);
        $this->expectExceptionMessage('Failed to encode request payload to JSON');

        $client->apply('order', '123', 'created', ['infinite' => INF]);
    }

    public function testCreateSnapshotReturnsDecodedResponse(): void
    {
        $client = $this->createClient();

        $result = $client->createSnapshot('order', '123', ['comment' => 'checkpoint']);

        $this->assertSame('dbx_create_snapshot', $result['function']);
        $this->assertSame('order', $result['aggregate_type']);
        $this->assertSame('123', $result['aggregate_id']);
        $this->assertSame(['comment' => 'checkpoint'], $result['options']);
    }

    public function testListSnapshotsReturnsDecodedResponse(): void
    {
        $client = $this->createClient();

        $result = $client->listSnapshots(['aggregateType' => 'order', 'version' => 5]);

        $this->assertSame('dbx_list_snapshots', $result['function']);
        $this->assertSame(['aggregateType' => 'order', 'version' => 5], $result['options']);
    }

    public function testGetSnapshotReturnsDecodedResponse(): void
    {
        $client = $this->createClient();

        $result = $client->getSnapshot(42, ['token' => 'demo']);

        $this->assertSame('dbx_get_snapshot', $result['function']);
        $this->assertSame(42, $result['snapshot_id']);
        $this->assertSame(['token' => 'demo'], $result['options']);
    }
}
