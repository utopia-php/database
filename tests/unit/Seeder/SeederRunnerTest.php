<?php

namespace Tests\Unit\Seeder;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Database;
use Utopia\Database\Seeder\Seeder;
use Utopia\Database\Seeder\SeederRunner;

class SeederRunnerTest extends TestCase
{
    public function testRunsInDependencyOrder(): void
    {
        $order = [];

        $seederA = new class ($order) extends Seeder {
            private array $order;

            public function __construct(array &$order)
            {
                $this->order = &$order;
            }

            public function run(Database $db): void
            {
                $this->order[] = 'A';
            }
        };

        $seederB = new class ($order, $seederA::class) extends Seeder {
            private array $order;

            private string $depClass;

            public function __construct(array &$order, string $depClass)
            {
                $this->order = &$order;
                $this->depClass = $depClass;
            }

            public function dependencies(): array
            {
                return [$this->depClass];
            }

            public function run(Database $db): void
            {
                $this->order[] = 'B';
            }
        };

        $runner = new SeederRunner();
        $runner->register($seederA);
        $runner->register($seederB);

        $db = $this->createMock(Database::class);
        $runner->run($db);

        $this->assertEquals(['A', 'B'], $order);
    }

    public function testDoesNotRunSameSeederTwice(): void
    {
        $count = 0;

        $seeder = new class ($count) extends Seeder {
            private int $count;

            public function __construct(int &$count)
            {
                $this->count = &$count;
            }

            public function run(Database $db): void
            {
                $this->count++;
            }
        };

        $runner = new SeederRunner();
        $runner->register($seeder);

        $db = $this->createMock(Database::class);
        $runner->run($db);

        $this->assertEquals(1, $count);
        $this->assertArrayHasKey($seeder::class, $runner->getExecuted());
    }

    public function testResetAllowsRerun(): void
    {
        $count = 0;

        $seeder = new class ($count) extends Seeder {
            private int $count;

            public function __construct(int &$count)
            {
                $this->count = &$count;
            }

            public function run(Database $db): void
            {
                $this->count++;
            }
        };

        $runner = new SeederRunner();
        $runner->register($seeder);

        $db = $this->createMock(Database::class);
        $runner->run($db);
        $runner->reset();
        $runner->run($db);

        $this->assertEquals(2, $count);
    }
}
