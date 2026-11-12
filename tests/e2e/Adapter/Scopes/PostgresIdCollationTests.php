<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Collection;
use Utopia\Database\Document;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;

/**
 * The `_uid` unique index, and the `_perms` one outside shared tables, are built COLLATE utf8_ci_ai, so ids are
 * unique case-insensitively and every id comparison has to be made in that collation to use the index.
 */
trait PostgresIdCollationTests
{
    public function testCaseVariantIdFindsTheDocument(): void
    {
        $database = $this->getDatabase();
        $database->createCollection(Collection::create(id: 'caseVariantRead', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));

        $database->createDocument('caseVariantRead', new Document(['$id' => 'abc']));

        $this->assertSame('abc', $database->getDocument('caseVariantRead', 'ABC')->getId());
        $this->assertSame(['abc'], \array_map(
            static fn (Document $document): string => $document->getId(),
            $database->find('caseVariantRead', [Query::equal('$id', ['ABC'])]),
        ));

        $database->deleteCollection('caseVariantRead');
    }

    public function testIdLookupUsesTheIdIndex(): void
    {
        $database = $this->getDatabase();
        $pdo = self::$pdo;
        $this->assertNotNull($pdo);

        $database->createCollection(Collection::create(id: 'idIndexPlan', permissions: [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
        ]));
        $database->createDocuments('idIndexPlan', \array_map(
            static fn (int $i): Document => new Document(['$id' => 'doc'.$i]),
            \range(1, 1000),
        ));
        $pdo->exec('ANALYZE "'.$database->getDatabase().'"."'.$database->getNamespace().'_idIndexPlan"');

        $table = $database->getNamespace().'_idIndexPlan';
        $scans = function () use ($pdo, $table): int {
            $pdo->query('SELECT pg_stat_force_next_flush()');
            $pdo->query('SELECT pg_stat_clear_snapshot()');

            $statement = $pdo->prepare("SELECT COALESCE(SUM(idx_scan), 0) FROM pg_stat_user_indexes WHERE relname = :table AND RIGHT(indexrelname, 4) = '_uid'");
            $statement->execute([':table' => $table]);

            return (int) $statement->fetchColumn();
        };

        $before = $scans();

        $pdo->exec('SET enable_seqscan = off');

        try {
            $results = $database->find('idIndexPlan', [Query::equal('$id', ['doc500'])]);
        } finally {
            $pdo->exec('RESET enable_seqscan');
        }

        $this->assertCount(1, $results);
        $this->assertGreaterThan($before, $scans(), 'A lookup by id must be answered from the _uid index');

        $database->deleteCollection('idIndexPlan');
    }

    public function testCaseVariantWritesKeepPermissionsConsistent(): void
    {
        $database = $this->getDatabase();
        $pdo = self::$pdo;
        $this->assertNotNull($pdo);

        $database->createCollection(Collection::create(id: 'caseVariantWrite', permissions: [
            Permission::create(Role::any()),
        ], documentSecurity: true));

        $database->createDocument('caseVariantWrite', new Document([
            '$id' => 'abc',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $permissions = function () use ($pdo, $database): array {
            $statement = $pdo->prepare('SELECT "_document", "_type" FROM "'.$database->getDatabase().'"."'.$database->getNamespace().'_caseVariantWrite_perms" ORDER BY "_type"');
            $statement->execute();

            return $statement->fetchAll(\PDO::FETCH_NUM);
        };

        $database->updateDocument('caseVariantWrite', 'ABC', new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::delete(Role::any()),
            ],
        ]));

        $this->assertSame([['abc', 'delete'], ['abc', 'read']], $permissions());

        $this->assertFalse($database->getDocument('caseVariantWrite', 'abc')->isEmpty());
        $this->assertTrue($database->deleteDocument('caseVariantWrite', 'ABC'));

        $this->assertSame([], $permissions(), 'Deleting through a case variant must remove the stored id\'s permission rows');
        $this->assertTrue($database->getDocument('caseVariantWrite', 'abc')->isEmpty(), 'The stored id must not be served from the cache');

        $database->deleteCollection('caseVariantWrite');
    }
}
