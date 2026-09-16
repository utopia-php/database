<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\PDO;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * Column-level permissions against real SQL.
 *
 * SQLite uses the shared SQL adapter implementation -- the same _perms DDL, the same
 * writes, and the same EXISTS conditions that MariaDB and Postgres emit -- and it runs
 * from a temp file with no services, so it covers the generated SQL here rather than
 * only under docker. The rest of the column-permission behaviour is adapter
 * independent and is covered against the in-memory adapter.
 */
class ColumnPermissionSqlTest extends TestCase
{
    protected Authorization $authorization;

    protected Database $database;

    protected string $file;

    protected function setUp(): void
    {
        $this->file = \sys_get_temp_dir() . '/utopia_colperm_' . \uniqid() . '.sql';

        $pdo = new PDO('sqlite:' . $this->file, null, null, SQLite::getPDOAttributes());
        $adapter = new SQLite($pdo);
        $adapter->setEmulateMySQL(true);

        $this->authorization = new Authorization();

        $this->database = new Database($adapter, new Cache(new NoCache()));
        $this->database
            ->setAuthorization($this->authorization)
            ->setDatabase('utopiaTests')
            ->setNamespace('cp_' . \uniqid());

        $this->database->create();

        $this->authorization->skip(function () {
            $this->database->createCollection('employees', documentSecurity: true, columnSecurity: true, permissions: [
                Permission::read(Role::any(), 'name'),
            ]);
            $this->database->createAttribute('employees', 'name', Database::VAR_STRING, 128, false);
            $this->database->createAttribute('employees', 'salary', Database::VAR_INTEGER, 8, false);

            // hr may read salary on e1 only
            $this->database->createDocument('employees', new Document([
                '$id' => 'e1',
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'name' => 'Bob',
                'salary' => 100000,
            ]));
            // nobody may read salary on e2, and its salary is higher
            $this->database->createDocument('employees', new Document([
                '$id' => 'e2',
                '$permissions' => [],
                'name' => 'Ann',
                'salary' => 200000,
            ]));
        });
    }

    protected function tearDown(): void
    {
        if (isset($this->file) && \file_exists($this->file)) {
            @\unlink($this->file);
        }
    }

    /**
     * @param array<string> $roles
     */
    private function as(array $roles): void
    {
        $this->authorization->cleanRoles();

        foreach ($roles as $role) {
            $this->authorization->addRole($role);
        }
    }

    /**
     * @param array<Document> $rows
     * @return array<string, array<string>>
     */
    private function shape(array $rows): array
    {
        $shape = [];

        foreach ($rows as $row) {
            $shape[$row->getId()] = \array_values(\array_filter(
                \array_keys($row->getArrayCopy()),
                fn (string $key) => !\str_starts_with($key, '$')
            ));
        }

        return $shape;
    }

    public function testColumnIsPersistedOnThePermissionsTable(): void
    {
        $rows = $this->authorization->skip(
            fn () => $this->database->find('employees', [Query::equal('$id', ['e1'])])
        );

        $this->assertSame(['read("user:hr", "salary")'], $rows[0]->getPermissions());
    }

    /**
     * The row gate matches on the role with _column left out of the predicate, so a
     * grant scoped to one column still makes the row visible. This is the case that
     * an assembled-string match (Mongo, and Postgres' jsonb path) gets wrong.
     */
    public function testColumnScopedGrantAloneMakesTheRowVisible(): void
    {
        $this->authorization->skip(function () {
            $this->database->createCollection('scoped', documentSecurity: true, columnSecurity: true, permissions: []);
            $this->database->createAttribute('scoped', 'name', Database::VAR_STRING, 128, false);
            $this->database->createAttribute('scoped', 'salary', Database::VAR_INTEGER, 8, false);

            $this->database->createDocument('scoped', new Document([
                '$id' => 'only',
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'name' => 'Bob',
                'salary' => 5,
            ]));
        });

        $this->as(['any', 'user:hr']);

        $this->assertSame(['only' => ['salary']], $this->shape($this->database->find('scoped')));
        $this->assertSame(1, $this->database->count('scoped'));
    }

    public function testUnfilteredFindMasksPerRow(): void
    {
        $this->as(['any', 'user:hr']);

        $this->assertSame(
            ['e1' => ['name', 'salary'], 'e2' => ['name']],
            $this->shape($this->database->find('employees'))
        );
    }

    /**
     * Case 4: salary is granted per document, which the collection-level floor cannot
     * see. The EXISTS settles it per row.
     */
    public function testFilterOnAPerDocumentGrantedColumnReturnsThatRow(): void
    {
        $this->as(['any', 'user:hr']);

        $this->assertSame(
            ['e1' => ['name', 'salary']],
            $this->shape($this->database->find('employees', [Query::greaterThan('salary', 95000)]))
        );
    }

    public function testOrderingOnAPerDocumentGrantedColumnKeepsOnlyGrantingRows(): void
    {
        $this->as(['any', 'user:hr']);

        $this->assertSame(
            ['e1' => ['name', 'salary']],
            $this->shape($this->database->find('employees', [Query::orderDesc('salary')]))
        );
    }

    /**
     * e2 satisfies every one of these predicates. If it ever appears, the filter has
     * become an oracle for a value the caller may not read.
     */
    public function testPredicateCannotBoundAValueTheCallerCannotRead(): void
    {
        $this->as(['any', 'user:hr']);

        foreach ([150000, 190000, 199999] as $threshold) {
            $this->assertSame(
                [],
                $this->database->find('employees', [Query::greaterThan('salary', $threshold)]),
                "threshold {$threshold} leaked e2"
            );
        }
    }

    public function testSumCountsOnlyRowsThatGrantTheColumn(): void
    {
        $this->as(['any', 'user:hr']);

        // e1 only. Not 300000, and not e2's 200000.
        $this->assertSame(100000, $this->database->sum('employees', 'salary'));
        $this->assertSame(1, $this->database->count('employees', [Query::greaterThan('salary', 95000)]));
    }

    public function testCallerWithNoGrantOnTheColumnMatchesNothing(): void
    {
        $this->as(['any']);

        $this->assertSame([], $this->database->find('employees', [Query::greaterThan('salary', 1)]));
        $this->assertSame(0, $this->database->sum('employees', 'salary'));

        // ...while the rows themselves stay visible through the collection's name grant
        $this->assertSame(
            ['e1' => ['name'], 'e2' => ['name']],
            $this->shape($this->database->find('employees'))
        );
    }

    /**
     * A permission can name a column that no longer exists -- written before the
     * column was dropped, or restored from a backup. The caller can then read nothing,
     * while the row filter still matches on the role. Returning the document would
     * disclose its id and timestamps to someone entitled to none of its data.
     */
    public function testDocumentIsInvisibleWhenNoGrantedColumnExists(): void
    {
        $this->authorization->skip(function () {
            $this->database->createDocument('employees', new Document([
                '$id' => 'ghost',
                '$permissions' => [Permission::read(Role::user('nobody'), 'salary')],
                'name' => 'Cid',
                'salary' => 1,
            ]));

            // the granted column disappears from under the permission
            $this->database->deleteAttribute('employees', 'salary');
            $this->database->createAttribute('employees', 'salary', Database::VAR_INTEGER, 8, false);
        });

        $this->as(['user:nobody']);

        $document = $this->database->getDocument('employees', 'ghost');

        $this->assertTrue($document->isEmpty(), 'document leaked its metadata');
        $this->assertSame([], $this->database->find('employees'));
    }

    public function testSelectOfAnUnreadableColumnIsMaskedNotDropped(): void
    {
        $this->as(['any']);

        $rows = $this->database->find('employees', [Query::select(['salary'])]);

        $this->assertCount(2, $rows);
        $this->assertNull($rows[0]->getAttribute('salary'));
    }
}
