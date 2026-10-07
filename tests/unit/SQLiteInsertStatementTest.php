<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\Memory;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Profiler\QueryLog;
use Utopia\Database\Validator\Authorization;

final class SQLiteInsertStatementTest extends TestCase
{
    private const string COLLECTION = 'notes';

    /**
     * @return iterable<string, array{bool, bool}>
     */
    public static function tables(): iterable
    {
        yield 'plain tables' => [false, false];
        yield 'plain tables, document permissions' => [false, true];
        yield 'shared tables' => [true, false];
        yield 'shared tables, document permissions' => [true, true];
    }

    #[DataProvider('tables')]
    public function testCreateDocumentRunsOneInsertPerTableAndNoSelect(bool $shared, bool $documentPermissions): void
    {
        $database = $this->database($shared);
        $database->createDocument(self::COLLECTION, $this->document('warmup', $documentPermissions));

        $profiler = $database->enableProfiling()->getProfiler();
        $this->assertNotNull($profiler);
        $profiler->reset();

        $created = $database->createDocument(self::COLLECTION, $this->document('measured', $documentPermissions));

        $statements = \array_map(
            static fn (QueryLog $log): string => \strtoupper(\ltrim((string) \preg_replace('#/\*.*?\*/#s', '', $log->query))),
            $profiler->getLogs(),
        );
        $inserts = \array_values(\array_filter($statements, static fn (string $statement): bool => \str_starts_with($statement, 'INSERT')));
        $selects = \array_values(\array_filter($statements, static fn (string $statement): bool => \str_starts_with($statement, 'SELECT')));

        $this->assertSame([], $selects);
        $this->assertCount($documentPermissions ? 2 : 1, $inserts);
        $this->assertSame(\count($inserts), \count(\array_unique(\array_map(
            static fn (string $statement): string => (string) \preg_replace('/^INSERT\s+INTO\s+(\S+).*$/s', '$1', $statement),
            $inserts,
        ))));

        $this->assertSame('2', $created->getSequence());
        $this->assertSame('measured', $database->getDocument(self::COLLECTION, 'measured')->getId());
    }

    private function document(string $id, bool $documentPermissions): Document
    {
        return new Document([
            Document::ID => $id,
            Document::PERMISSIONS => $documentPermissions ? [Permission::read(Role::any())] : [],
            'body' => $id,
        ]);
    }

    private function database(bool $shared): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new Memory()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('insert_statement')
            ->setNamespace('insert_statement');

        if ($shared) {
            $database->setSharedTables(true)->setTenant(1);
        }

        $database->create();
        $database->addHook(new Permissions());
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'body', size: 64)],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
            ],
            documentSecurity: true,
        ));

        return $database;
    }
}
