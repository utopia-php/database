<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;

class SwooleAbsentTest extends TestCase
{
    /**
     * ext-swoole is optional: it is absent from composer.json's require block, so a
     * consumer may run the library on a PHP that does not have it. The lifecycle-hook
     * context lookup and the lost-connection check both reach Swoole classes, and an
     * unguarded reference there is a fatal Error rather than a caught exception.
     *
     * An extension cannot be unloaded from a running interpreter, so this drives a
     * subprocess started with -n, which skips php.ini and every conf.d file and
     * therefore loads no shared extension. Swoole ships as a shared extension both in
     * the test image and in every environment that installs it through pecl.
     */
    public function testDatabaseOperatesWithoutSwoole(): void
    {
        $fixture = __DIR__ . '/Support/swoole-absent.php';

        $command = \escapeshellarg(PHP_BINARY) . ' -n ' . \escapeshellarg($fixture) . ' 2>&1';

        \exec($command, $lines, $status);

        $output = \implode(PHP_EOL, $lines);

        if (\str_contains($output, 'swoole=1')) {
            $this->markTestSkipped('swoole is statically compiled into ' . PHP_BINARY . ', so its absence cannot be exercised');
        }

        $this->assertSame(0, $status, "Fixture exited {$status} without swoole:" . PHP_EOL . $output);

        $this->assertStringContainsString('swoole=0', $output, $output);
        $this->assertStringContainsString('create=ok', $output, $output);
        $this->assertStringContainsString('silent=ok', $output, $output);
        $this->assertStringContainsString('deleted=3', $output, $output);
        $this->assertStringContainsString('remaining=0', $output, $output);
        $this->assertStringContainsString('hasError=0', $output, $output);
    }
}
