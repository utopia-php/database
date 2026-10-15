<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;

/**
 * aggregate() reads a joined collection as a direct list of it would be.
 */
trait JoinReadRuleTests
{
    public function testJoinReadRuleAggregateRefusesAJoinedCollectionWithoutGrantOrDocumentSecurity(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->seedJoinReadRule($database, 'jrule_deny', joinedPermissions: [Permission::create(Role::any())], joinedDocumentSecurity: false);

        try {
            $queries = [Query::join($joined, 'entry', [Query::on('$id', 'mainId')]), Query::count('*', 'rows')];

            $this->withJoinReadRuleRoles($database, [Role::any()->toString()], function () use ($database, $main, $queries): void {
                try {
                    $database->aggregate($main, $queries);
                    $this->fail('aggregate() must refuse a joined collection readable neither at collection nor at document level');
                } catch (AuthorizationException $exception) {
                    $this->assertStringContainsString('jrule_deny_j', $exception->getMessage());
                }
            });

            $rows = $database->getAuthorization()->skip(fn (): array => $database->aggregate($main, $queries));
            $this->assertSame(2, self::joinReadRuleInteger($rows[0]['rows']), 'with authorization disabled every joined row is read');
        } finally {
            $this->deleteJoinReadRuleCollections($database, $main, $joined);
        }
    }

    public function testJoinReadRuleAggregateFiltersAJoinedCollectionPerDocument(): void
    {
        $database = $this->getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->seedJoinReadRule($database, 'jrule_owned', joinedPermissions: [Permission::create(Role::any())], joinedDocumentSecurity: true);

        try {
            $this->withJoinReadRuleRoles($database, [Role::any()->toString(), Role::user('alice')->toString()], function () use ($database, $main, $joined): void {
                $joinedRows = $database->aggregate($joined, [Query::count('*', 'rows'), Query::sum('amount', 'total')]);
                $rows = $database->aggregate($main, [
                    Query::join($joined, 'entry', [Query::on('$id', 'mainId')]),
                    Query::count('*', 'rows'),
                    Query::sum('entry.amount', 'total'),
                ]);

                $this->assertSame([1, 5], [self::joinReadRuleInteger($joinedRows[0]['rows']), self::joinReadRuleInteger($joinedRows[0]['total'])], 'a direct aggregate reads only the rows the caller holds read on');
                $this->assertSame([1, 5], [self::joinReadRuleInteger($rows[0]['rows']), self::joinReadRuleInteger($rows[0]['total'])], 'an aggregate over the join reads the same rows');
            });
        } finally {
            $this->deleteJoinReadRuleCollections($database, $main, $joined);
        }
    }

    /**
     * A main collection every caller reads, with one document, and a joined collection holding two entries for it:
     * one readable by user alice and one by user bob.
     *
     * @param  list<string>  $joinedPermissions
     * @return array{string, string}
     */
    private function seedJoinReadRule(Database $database, string $prefix, array $joinedPermissions, bool $joinedDocumentSecurity): array
    {
        $main = $prefix.'_m';
        $joined = $prefix.'_j';
        $this->deleteJoinReadRuleCollections($database, $main, $joined);

        $database->createCollection(Collection::create(
            id: $main,
            attributes: [Attribute::string(key: 'name', size: 16)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        $database->createCollection(Collection::create(
            id: $joined,
            attributes: [Attribute::string(key: 'mainId', size: 36), Attribute::integer(key: 'amount')],
            permissions: $joinedPermissions,
            documentSecurity: $joinedDocumentSecurity,
        ));

        $database->createDocument($main, new Document(['$id' => 'customer', 'name' => 'Customer', '$permissions' => [Permission::read(Role::any())]]));
        $database->getAuthorization()->skip(function () use ($database, $joined): void {
            $database->createDocument($joined, new Document(['$id' => 'alice-entry', 'mainId' => 'customer', 'amount' => 5, '$permissions' => [Permission::read(Role::user('alice'))]]));
            $database->createDocument($joined, new Document(['$id' => 'bob-entry', 'mainId' => 'customer', 'amount' => 7, '$permissions' => [Permission::read(Role::user('bob'))]]));
        });

        return [$main, $joined];
    }

    private function deleteJoinReadRuleCollections(Database $database, string ...$collections): void
    {
        foreach ($collections as $collection) {
            if ($database->collectionExists($collection)) {
                $database->deleteCollection($collection);
            }
        }
    }

    /**
     * @param  list<string>  $roles
     * @param  callable(): void  $callback
     */
    private function withJoinReadRuleRoles(Database $database, array $roles, callable $callback): void
    {
        $authorization = $database->getAuthorization();
        $previous = $authorization->getRoles();
        $authorization->cleanRoles();
        foreach ($roles as $role) {
            $authorization->addRole($role);
        }

        try {
            $callback();
        } finally {
            $authorization->cleanRoles();
            foreach ($previous as $role) {
                $authorization->addRole($role);
            }
        }
    }

    private static function joinReadRuleInteger(mixed $value): ?int
    {
        return \is_numeric($value) ? (int) $value : null;
    }
}
