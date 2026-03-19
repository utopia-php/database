<?php

namespace Tests\Unit\Migration;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Attribute;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Index;
use Utopia\Database\Migration\Migration;
use Utopia\Database\Migration\MigrationGenerator;
use Utopia\Database\Migration\MigrationRunner;
use Utopia\Database\Migration\MigrationTracker;
use Utopia\Database\Schema\DiffResult;
use Utopia\Database\Schema\SchemaChange;
use Utopia\Database\Schema\SchemaChangeType;
use Utopia\Query\Schema\ColumnType;
use Utopia\Query\Schema\IndexType;

class MigrationRunnerTest extends TestCase
{
    private Database $db;

    protected function setUp(): void
    {
        $this->db = $this->createMock(Database::class);
    }

    private function createMigration(string $version, ?callable $up = null, ?callable $down = null): Migration
    {
        return new class ($version, $up, $down) extends Migration {
            private string $ver;

            /** @var callable|null */
            private $upFn;

            /** @var callable|null */
            private $downFn;

            public function __construct(string $ver, ?callable $upFn = null, ?callable $downFn = null)
            {
                $this->ver = $ver;
                $this->upFn = $upFn;
                $this->downFn = $downFn;
            }

            public function version(): string
            {
                return $this->ver;
            }

            public function up(Database $db): void
            {
                if ($this->upFn) {
                    ($this->upFn)($db);
                }
            }

            public function down(Database $db): void
            {
                if ($this->downFn) {
                    ($this->downFn)($db);
                }
            }
        };
    }

    private function createTrackerMock(array $appliedVersions = [], int $lastBatch = 0, array $batchDocs = []): MigrationTracker
    {
        $tracker = $this->createMock(MigrationTracker::class);
        $tracker->method('setup');
        $tracker->method('getAppliedVersions')->willReturn($appliedVersions);
        $tracker->method('getLastBatch')->willReturn($lastBatch);
        $tracker->method('getByBatch')->willReturnCallback(function (int $batch) use ($batchDocs) {
            return $batchDocs[$batch] ?? [];
        });
        $tracker->method('markApplied');
        $tracker->method('markRolledBack');

        return $tracker;
    }

    public function testMigrateRunsPendingMigrationsInVersionOrder(): void
    {
        $order = [];

        $m1 = $this->createMigration('002', function () use (&$order) {
            $order[] = '002';
        });
        $m2 = $this->createMigration('001', function () use (&$order) {
            $order[] = '001';
        });

        $tracker = $this->createTrackerMock();
        $this->db->method('withTransaction')->willReturnCallback(fn (callable $cb) => $cb());

        $runner = new MigrationRunner($this->db, $tracker);
        $runner->migrate([$m1, $m2]);

        $this->assertEquals(['001', '002'], $order);
    }

    public function testMigrateSkipsAlreadyAppliedMigrations(): void
    {
        $executed = [];

        $m1 = $this->createMigration('001', function () use (&$executed) {
            $executed[] = '001';
        });
        $m2 = $this->createMigration('002', function () use (&$executed) {
            $executed[] = '002';
        });

        $tracker = $this->createTrackerMock(appliedVersions: ['001']);
        $this->db->method('withTransaction')->willReturnCallback(fn (callable $cb) => $cb());

        $runner = new MigrationRunner($this->db, $tracker);
        $runner->migrate([$m1, $m2]);

        $this->assertEquals(['002'], $executed);
    }

    public function testMigrateReturnsCountOfExecutedMigrations(): void
    {
        $m1 = $this->createMigration('001');
        $m2 = $this->createMigration('002');

        $tracker = $this->createTrackerMock();
        $this->db->method('withTransaction')->willReturnCallback(fn (callable $cb) => $cb());

        $runner = new MigrationRunner($this->db, $tracker);
        $count = $runner->migrate([$m1, $m2]);

        $this->assertEquals(2, $count);
    }

    public function testMigrateWithNoPendingReturnsZero(): void
    {
        $m1 = $this->createMigration('001');

        $tracker = $this->createTrackerMock(appliedVersions: ['001']);

        $runner = new MigrationRunner($this->db, $tracker);
        $count = $runner->migrate([$m1]);

        $this->assertEquals(0, $count);
    }

    public function testRollbackCallsDownInReverseOrder(): void
    {
        $order = [];

        $m1 = $this->createMigration('001', null, function () use (&$order) {
            $order[] = '001';
        });
        $m2 = $this->createMigration('002', null, function () use (&$order) {
            $order[] = '002';
        });

        $batchDocs = [
            1 => [
                new Document(['version' => '002']),
                new Document(['version' => '001']),
            ],
        ];

        $tracker = $this->createTrackerMock(lastBatch: 1, batchDocs: $batchDocs);
        $this->db->method('withTransaction')->willReturnCallback(fn (callable $cb) => $cb());

        $runner = new MigrationRunner($this->db, $tracker);
        $runner->rollback([$m1, $m2], 1);

        $this->assertEquals(['002', '001'], $order);
    }

    public function testRollbackBySteps(): void
    {
        $order = [];

        $m1 = $this->createMigration('001', null, function () use (&$order) {
            $order[] = '001';
        });
        $m2 = $this->createMigration('002', null, function () use (&$order) {
            $order[] = '002';
        });
        $m3 = $this->createMigration('003', null, function () use (&$order) {
            $order[] = '003';
        });

        $batchDocs = [
            1 => [new Document(['version' => '001'])],
            2 => [new Document(['version' => '002']), new Document(['version' => '003'])],
        ];

        $tracker = $this->createTrackerMock(lastBatch: 2, batchDocs: $batchDocs);
        $this->db->method('withTransaction')->willReturnCallback(fn (callable $cb) => $cb());

        $runner = new MigrationRunner($this->db, $tracker);
        $count = $runner->rollback([$m1, $m2, $m3], 1);

        $this->assertEquals(2, $count);
        $this->assertEquals(['002', '003'], $order);
    }

    public function testRollbackReturnsCount(): void
    {
        $m1 = $this->createMigration('001', null, function () {
        });

        $batchDocs = [
            1 => [new Document(['version' => '001'])],
        ];

        $tracker = $this->createTrackerMock(lastBatch: 1, batchDocs: $batchDocs);
        $this->db->method('withTransaction')->willReturnCallback(fn (callable $cb) => $cb());

        $runner = new MigrationRunner($this->db, $tracker);
        $count = $runner->rollback([$m1], 1);

        $this->assertEquals(1, $count);
    }

    public function testStatusReturnsAllMigrationsWithAppliedFlag(): void
    {
        $m1 = $this->createMigration('001');
        $m2 = $this->createMigration('002');

        $tracker = $this->createTrackerMock(appliedVersions: ['001']);

        $runner = new MigrationRunner($this->db, $tracker);
        $status = $runner->status([$m1, $m2]);

        $this->assertCount(2, $status);
        $this->assertTrue($status[0]['applied']);
        $this->assertFalse($status[1]['applied']);
    }

    public function testStatusReturnsSortedByVersion(): void
    {
        $m1 = $this->createMigration('003');
        $m2 = $this->createMigration('001');

        $tracker = $this->createTrackerMock();

        $runner = new MigrationRunner($this->db, $tracker);
        $status = $runner->status([$m1, $m2]);

        $this->assertEquals('001', $status[0]['version']);
        $this->assertEquals('003', $status[1]['version']);
    }

    public function testGetTrackerReturnsMigrationTracker(): void
    {
        $tracker = $this->createTrackerMock();
        $runner = new MigrationRunner($this->db, $tracker);
        $this->assertSame($tracker, $runner->getTracker());
    }

    public function testMigrationGeneratorGenerateEmptyProducesValidPHP(): void
    {
        $generator = new MigrationGenerator();
        $output = $generator->generateEmpty('V001_CreateUsers');

        $this->assertStringContainsString('class V001_CreateUsers extends Migration', $output);
        $this->assertStringContainsString("return '001'", $output);
        $this->assertStringContainsString('public function up(Database $db): void', $output);
        $this->assertStringContainsString('public function down(Database $db): void', $output);
    }

    public function testMigrationGeneratorGenerateWithDiffResultIncludesUpDownMethods(): void
    {
        $diff = new DiffResult([
            new SchemaChange(
                type: SchemaChangeType::AddAttribute,
                attribute: new Attribute(key: 'email', type: ColumnType::String, size: 255),
            ),
        ]);

        $generator = new MigrationGenerator();
        $output = $generator->generate($diff, 'V002_AddEmail');

        $this->assertStringContainsString('class V002_AddEmail extends Migration', $output);
        $this->assertStringContainsString("return '002'", $output);
        $this->assertStringContainsString('email', $output);
    }

    public function testMigrationGeneratorExtractVersionFromV001Prefix(): void
    {
        $generator = new MigrationGenerator();
        $output = $generator->generateEmpty('V042_SomeChange');
        $this->assertStringContainsString("return '042'", $output);
    }

    public function testMigrationGeneratorFallsBackToClassName(): void
    {
        $generator = new MigrationGenerator();
        $output = $generator->generateEmpty('CreateUsersTable');
        $this->assertStringContainsString("return 'CreateUsersTable'", $output);
    }

    public function testMigrationAbstractClassNameReturnsClassName(): void
    {
        $migration = $this->createMigration('001');
        $this->assertIsString($migration->name());
        $this->assertNotEmpty($migration->name());
    }

    public function testMigrateWithEmptyArrayReturnsZero(): void
    {
        $tracker = $this->createTrackerMock();
        $runner = new MigrationRunner($this->db, $tracker);
        $count = $runner->migrate([]);
        $this->assertEquals(0, $count);
    }

    public function testRollbackWithNoMigrationsInBatch(): void
    {
        $tracker = $this->createTrackerMock(lastBatch: 1, batchDocs: [1 => []]);
        $this->db->method('withTransaction')->willReturnCallback(fn (callable $cb) => $cb());

        $runner = new MigrationRunner($this->db, $tracker);
        $count = $runner->rollback([], 1);
        $this->assertEquals(0, $count);
    }

    public function testMigrationGeneratorGenerateWithDropAttribute(): void
    {
        $diff = new DiffResult([
            new SchemaChange(
                type: SchemaChangeType::DropAttribute,
                attribute: new Attribute(key: 'legacy', type: ColumnType::String, size: 100),
            ),
        ]);

        $generator = new MigrationGenerator();
        $output = $generator->generate($diff, 'V003_DropLegacy');

        $this->assertStringContainsString('legacy', $output);
        $this->assertStringContainsString('deleteAttribute', $output);
    }

    public function testMigrationGeneratorGenerateWithAddIndex(): void
    {
        $diff = new DiffResult([
            new SchemaChange(
                type: SchemaChangeType::AddIndex,
                index: new Index(key: 'idx_email', type: IndexType::Index, attributes: ['email']),
            ),
        ]);

        $generator = new MigrationGenerator();
        $output = $generator->generate($diff, 'V004_AddIndex');

        $this->assertStringContainsString('idx_email', $output);
        $this->assertStringContainsString('createIndex', $output);
    }
}
