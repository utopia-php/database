<?php

namespace Tests\Unit\Migration;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Migration\Strategy\OnlineSchemaChange;

class OnlineSchemaChangeTest extends TestCase
{
    public function testAlterRestoresPriorLockState(): void
    {
        $locks = [true];
        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getAlterLocks')->willReturnCallback(static fn (): bool => $locks[0]);
        $adapter->method('enableAlterLocks')->willReturnCallback(function (bool $enable) use (&$locks, $adapter): Adapter {
            $locks[] = $enable;
            $locks[0] = $enable;

            return $adapter;
        });

        $db = $this->createMock(Database::class);
        $db->method('getAdapter')->willReturn($adapter);

        $strategy = new OnlineSchemaChange();
        $strategy->alter($db, 'users', function () use (&$locks): void {
            $this->assertFalse($locks[0]);
        });

        $this->assertSame([true, false, true], $locks);
    }

    public function testAlterLeavesLocksDisabledWhenTheyStartedDisabled(): void
    {
        $locks = [false];
        $adapter = $this->createMock(Adapter::class);
        $adapter->method('getAlterLocks')->willReturnCallback(static fn (): bool => $locks[0]);
        $adapter->method('enableAlterLocks')->willReturnCallback(function (bool $enable) use (&$locks, $adapter): Adapter {
            $locks[0] = $enable;

            return $adapter;
        });

        $db = $this->createMock(Database::class);
        $db->method('getAdapter')->willReturn($adapter);

        $strategy = new OnlineSchemaChange();
        $strategy->alter($db, 'users', function (): void {
        });

        $this->assertFalse($locks[0]);
    }
}
