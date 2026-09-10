<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;

class SwooleAbsentTest extends TestCase
{
    /**
     * @param array<string> $flags
     * @return array{status: int, output: string}
     */
    private function runFixture(array $flags): array
    {
        $command = \escapeshellarg(PHP_BINARY);

        foreach ($flags as $flag) {
            $command .= ' ' . $flag;
        }

        $command .= ' ' . \escapeshellarg(__DIR__ . '/Support/swoole-absent.php') . ' 2>&1';

        \exec($command, $lines, $status);

        return ['status' => $status, 'output' => \implode(PHP_EOL, $lines)];
    }

    /**
     * ext-swoole is optional: it is absent from composer.json's require block, so a
     * consumer may run the library on a PHP that does not have it. An extension cannot
     * be unloaded from a running interpreter, so this drives a subprocess started with
     * -n, which skips php.ini and every conf.d file and therefore loads no shared
     * extension. Swoole ships as a shared extension in the test image and in every
     * environment that installs it through pecl.
     */
    public function testDatabaseOperatesWithoutSwoole(): void
    {
        ['status' => $status, 'output' => $output] = $this->runFixture(['-n']);

        if (\str_contains($output, 'swoole=1')) {
            $this->markTestSkipped('swoole is statically compiled into ' . PHP_BINARY . ', so its absence cannot be exercised');
        }

        $this->assertSame(0, $status, "Fixture exited {$status} without swoole:" . PHP_EOL . $output);

        $this->assertStringContainsString('create=ok', $output, $output);
        $this->assertStringContainsString('silent=ok', $output, $output);
        $this->assertStringContainsString('deleted=3', $output, $output);
        $this->assertStringContainsString('remaining=0', $output, $output);
        $this->assertStringContainsString('lostDetected=5', $output, $output);
        $this->assertStringContainsString('unrelatedDetected=0', $output, $output);
    }

    /**
     * Swoole\Database\DetectsLostConnections comes from Swoole's PHP-land library, which
     * swoole.enable_library=Off switches off while leaving the extension loaded. Lost
     * connections must still be recognised, so detection cannot rest on that class.
     */
    public function testLostConnectionsAreDetectedWithoutSwooleLibrary(): void
    {
        if (! \extension_loaded('swoole')) {
            $this->markTestSkipped('swoole is not loaded, so its library cannot be switched off');
        }

        ['status' => $status, 'output' => $output] = $this->runFixture(['-d swoole.enable_library=Off']);

        $this->assertSame(0, $status, "Fixture exited {$status} with the swoole library disabled:" . PHP_EOL . $output);

        $this->assertStringContainsString('lostDetected=5', $output, $output);
        $this->assertStringContainsString('unrelatedDetected=0', $output, $output);
    }
}
