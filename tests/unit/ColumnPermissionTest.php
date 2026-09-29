<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Database\Validator\Permissions;

class ColumnPermissionTest extends TestCase
{
    public function testParseWithoutColumnGrantsEveryColumn(): void
    {
        foreach (['read("any")', 'read("users")', 'read("user:123")', 'read("team:123/admin")'] as $string) {
            $permission = Permission::parse($string);

            $this->assertSame(Permission::COLUMN_ALL, $permission->getColumn());
            $this->assertTrue($permission->isForAllColumns());
            $this->assertSame($string, $permission->toString());
        }
    }

    public function testParseWithColumn(): void
    {
        $permission = Permission::parse('read("user:123", "salary")');

        $this->assertSame('read', $permission->getPermission());
        $this->assertSame('user', $permission->getRole());
        $this->assertSame('123', $permission->getIdentifier());
        $this->assertSame('', $permission->getDimension());
        $this->assertSame('salary', $permission->getColumn());
        $this->assertFalse($permission->isForAllColumns());
    }

    public function testParseWithColumnAndDimension(): void
    {
        $permission = Permission::parse('update("team:abc/owner", "salary")');

        $this->assertSame('update', $permission->getPermission());
        $this->assertSame('team', $permission->getRole());
        $this->assertSame('abc', $permission->getIdentifier());
        $this->assertSame('owner', $permission->getDimension());
        $this->assertSame('salary', $permission->getColumn());
    }

    /**
     * @return array<string, array{string}>
     */
    public static function roundTripProvider(): array
    {
        return [
            'no column' => ['read("any")'],
            'column' => ['read("user:123", "salary")'],
            'dimension and column' => ['read("team:abc/owner", "salary")'],
            'update column' => ['update("user:123", "name")'],
            'create column' => ['create("users", "name")'],
        ];
    }

    /**
     * @dataProvider roundTripProvider
     */
    public function testRoundTrip(string $string): void
    {
        $this->assertSame($string, Permission::parse($string)->toString());
    }

    public function testFactories(): void
    {
        $this->assertSame('read("user:123")', Permission::read(Role::user('123')));
        $this->assertSame('read("user:123", "salary")', Permission::read(Role::user('123'), 'salary'));
        $this->assertSame('update("team:abc/owner", "name")', Permission::update(Role::team('abc', 'owner'), 'name'));
        $this->assertSame('create("users", "name")', Permission::create(Role::users(), 'name'));
    }

    public function testAggregatePreservesColumn(): void
    {
        $aggregated = Permission::aggregate(['read("user:1", "name")']);

        $this->assertSame(['read("user:1", "name")'], $aggregated);
    }

    public function testEmptyColumnIsRejected(): void
    {
        $this->expectException(DatabaseException::class);
        Permission::parse('read("user:1", "")');
    }

    public function testWildcardColumnIsRejected(): void
    {
        $this->expectException(DatabaseException::class);
        Permission::parse('read("user:1", "*")');
    }

    /**
     * A column-scoped permission still grants its role ordinary row-level access --
     * the column narrows what is returned, it does not withhold the row. Asserted
     * through a read rather than through the shape of the extracted permission list.
     */
    public function testColumnScopedGrantStillGrantsTheRowToThatRole(): void
    {
        $authorization = new Authorization();

        $database = new Database(new Memory(), new Cache(new NoCache()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('columnPermissions')
            ->setNamespace('cpt_' . \uniqid());

        $database->create();

        $authorization->skip(function () use ($database) {
            $database->createCollection('employees', documentSecurity: true, columnSecurity: true, permissions: []);
            $database->createAttribute('employees', 'name', Database::VAR_STRING, 128, false);
            $database->createAttribute('employees', 'salary', Database::VAR_INTEGER, 8, false);

            $database->createDocument('employees', new Document([
                '$id' => 'e1',
                '$permissions' => [Permission::read(Role::user('hr'), 'salary')],
                'name' => 'Bob',
                'salary' => 100000,
            ]));
        });

        $authorization->cleanRoles();
        $authorization->addRole('user:hr');

        $document = $database->getDocument('employees', 'e1');

        $this->assertFalse($document->isEmpty(), 'a column-scoped grant must still make the row visible');
        $this->assertSame(100000, $document->getAttribute('salary'));
        $this->assertNull($document->getAttribute('name'));

        $authorization->cleanRoles();
        $authorization->addRole('user:other');

        $this->assertTrue($database->getDocument('employees', 'e1')->isEmpty());
    }

    public function testValidatorAcceptsColumnScopedReadCreateUpdate(): void
    {
        $validator = new Permissions(columns: ['salary', 'name']);

        $this->assertTrue($validator->isValid([
            'read("user:1", "salary")',
            'create("users", "name")',
            'update("team:abc/owner", "name")',
        ]), $validator->getDescription());
    }

    /**
     * The default is no columns, so a column-scoped permission is refused until the
     * caller names the columns that exist. There is no waiver: a caller who forgets is
     * told, rather than quietly having a grant on a column nobody checked accepted on
     * its behalf.
     */
    public function testValidatorRejectsColumnScopedGrantByDefault(): void
    {
        $validator = new Permissions();

        $this->assertFalse($validator->isValid(['read("user:1", "salary")']));
        $this->assertStringContainsString('does not exist', $validator->getDescription());

        $this->assertTrue($validator->isValid(['read("user:1")']), $validator->getDescription());
    }

    /**
     * An empty list is a collection with no columns, not a caller who did not say. Every
     * column-scoped grant names a column that does not exist.
     */
    public function testValidatorWithNoColumnsRejectsEveryColumnScopedGrant(): void
    {
        $validator = new Permissions(columns: []);

        $this->assertFalse($validator->isValid(['read("user:1", "salary")']));
        $this->assertStringContainsString('does not exist', $validator->getDescription());

        $this->assertTrue($validator->isValid(['read("user:1")']), $validator->getDescription());
    }

    public function testValidatorRejectsColumnScopedDelete(): void
    {
        $validator = new Permissions();

        $this->assertFalse($validator->isValid(['delete("user:1", "salary")']));
        $this->assertStringContainsString('cannot be scoped to a column', $validator->getDescription());
    }

    public function testValidatorRejectsColumnScopedWrite(): void
    {
        $validator = new Permissions();

        $this->assertFalse($validator->isValid(['write("user:1", "salary")']));
        $this->assertStringContainsString('cannot be scoped to a column', $validator->getDescription());
    }

    public function testValidatorRejectsUnknownColumnWhenColumnsGiven(): void
    {
        $validator = new Permissions(columns: ['name', 'email']);

        $this->assertTrue($validator->isValid(['read("user:1", "name")']), $validator->getDescription());
        $this->assertFalse($validator->isValid(['read("user:1", "salary")']));
        $this->assertStringContainsString('does not exist', $validator->getDescription());
    }

    public function testValidatorRejectsInvalidColumnKey(): void
    {
        $validator = new Permissions();

        $this->assertFalse($validator->isValid(['read("user:1", "_internal")']));
        $this->assertStringContainsString('not a valid column key', $validator->getDescription());
    }

    // ------------------------------------------------- grants on a missing column

    /**
     * @return array{Database, Authorization}
     */
    private function database(): array
    {
        $authorization = new Authorization();

        $database = new Database(new Memory(), new Cache(new NoCache()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('columnPermissions')
            ->setNamespace('cpm_' . \uniqid());

        $database->create();

        return [$database, $authorization];
    }

    /**
     * A collection created with no columns has nothing a permission could name, so a
     * grant on one is refused rather than held until that column appears and quietly
     * starts applying.
     */
    public function testCreateCollectionRejectsGrantOnMissingColumn(): void
    {
        [$database, $authorization] = $this->database();

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('does not exist');

        $authorization->skip(fn () => $database->createCollection(
            'employees',
            documentSecurity: true,
            columnSecurity: true,
            permissions: [Permission::read(Role::any(), 'salary')]
        ));
    }

    /**
     * The same grant is accepted when the column it names is created in the same call,
     * which is what makes the refusal above about the missing column rather than about
     * collection-level grants carrying columns at all.
     */
    public function testCreateCollectionAcceptsGrantOnColumnCreatedWithIt(): void
    {
        [$database, $authorization] = $this->database();

        $authorization->skip(fn () => $database->createCollection(
            'employees',
            attributes: [new Document([
                '$id' => 'salary',
                'key' => 'salary',
                'type' => Database::VAR_INTEGER,
                'size' => 8,
                'required' => false,
                'default' => null,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ])],
            documentSecurity: true,
            columnSecurity: true,
            permissions: [Permission::read(Role::any(), 'salary')]
        ));

        $collection = $authorization->skip(fn () => $database->getCollection('employees'));

        $this->assertSame([Permission::read(Role::any(), 'salary')], $collection->getPermissions());
    }

    /**
     * updateCollection judges the grant against the columns the collection has now, so
     * one naming a column that was never created is refused here too.
     */
    public function testUpdateCollectionRejectsGrantOnMissingColumn(): void
    {
        [$database, $authorization] = $this->collectionWithoutSalary();

        try {
            $authorization->skip(fn () => $database->updateCollection(
                'employees',
                [Permission::read(Role::any(), 'salary')],
                true,
                true
            ));
            $this->fail('updateCollection accepted a grant naming a column that does not exist');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('does not exist', $e->getMessage());
        }

        $collection = $authorization->skip(fn () => $database->getCollection('employees'));

        $this->assertSame([], $collection->getPermissions(), 'a refused update must change nothing');
    }

    /**
     * A collection with one column, 'name', and one document holding grants on it.
     * 'salary' is deliberately never created: it is the column these tests name.
     *
     * @return array{Database, Authorization}
     */
    private function collectionWithoutSalary(): array
    {
        [$database, $authorization] = $this->database();

        $authorization->skip(function () use ($database) {
            $database->createCollection('employees', documentSecurity: true, columnSecurity: true, permissions: []);
            $database->createAttribute('employees', 'name', Database::VAR_STRING, 128, false);

            $database->createDocument('employees', new Document([
                '$id' => 'e1',
                '$permissions' => [
                    Permission::read(Role::any(), 'name'),
                    Permission::update(Role::any(), 'name'),
                ],
                'name' => 'Bob',
            ]));
        });

        return [$database, $authorization];
    }

    /**
     * A grant naming a column that does not exist confers nothing today and binds
     * late if that column is ever created -- a restriction nobody reviewed at the
     * moment it became real. The write is refused instead.
     */
    public function testCreateDocumentRejectsGrantOnMissingColumn(): void
    {
        [$database, $authorization] = $this->collectionWithoutSalary();

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('does not exist');

        $authorization->skip(fn () => $database->createDocument('employees', new Document([
            '$id' => 'e2',
            '$permissions' => [Permission::read(Role::any(), 'salary')],
            'name' => 'Ann',
        ])));
    }

    /**
     * Same refusal with validation skipped. Storage is keyed by the column's identity,
     * so a grant naming no column has nothing to be stored against -- which makes this
     * a property of the write path rather than of the validator in front of it.
     */
    public function testCreateDocumentRejectsGrantOnMissingColumnWithoutValidation(): void
    {
        [$database, $authorization] = $this->collectionWithoutSalary();

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('does not exist');

        $authorization->skip(fn () => $database->skipValidation(
            fn () => $database->createDocument('employees', new Document([
                '$id' => 'e2',
                '$permissions' => [Permission::read(Role::any(), 'salary')],
                'name' => 'Ann',
            ]))
        ));
    }

    /**
     * The update path has to refuse what the create path refuses. Accepting here would
     * leave the same orphan grant in storage by a different door.
     */
    public function testUpdateDocumentRejectsGrantOnMissingColumn(): void
    {
        [$database, $authorization] = $this->collectionWithoutSalary();

        $update = fn () => $authorization->skip(fn () => $database->updateDocument('employees', 'e1', new Document([
            '$id' => 'e1',
            '$permissions' => [Permission::read(Role::any(), 'salary')],
            'name' => 'Bob',
        ])));

        try {
            $update();
            $this->fail('updateDocument accepted a grant naming a column that does not exist');
        } catch (DatabaseException $e) {
            $this->assertStringContainsString('does not exist', $e->getMessage());
        }

        $stored = $authorization->skip(fn () => $database->getDocument('employees', 'e1'));

        $this->assertSame(
            [Permission::read(Role::any(), 'name'), Permission::update(Role::any(), 'name')],
            $stored->getPermissions(),
            'a refused update must leave the stored permissions untouched'
        );
    }

    public function testUpdateDocumentRejectsGrantOnMissingColumnWithoutValidation(): void
    {
        [$database, $authorization] = $this->collectionWithoutSalary();

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('does not exist');

        $authorization->skip(fn () => $database->skipValidation(
            fn () => $database->updateDocument('employees', 'e1', new Document([
                '$id' => 'e1',
                '$permissions' => [Permission::read(Role::any(), 'salary')],
                'name' => 'Bob',
            ]))
        ));
    }

    /**
     * A rename changes an attribute's key and leaves its identity alone.
     *
     * This pins a design decision rather than a caller-visible behaviour, which is why
     * it sits here and not beside the e2e test that covers what callers need -- that
     * grants survive a rename. More than one design delivers that: storing the key in
     * _column and migrating every permission row on rename does too. This says which one
     * is in force, and it lives in the unit tier because no adapter has a say in it --
     * stamping and preserving the identity happen in Database, and the adapter only
     * renames a physical column.
     *
     * If the design is ever traded for another, this is the test to delete.
     */
    public function testRenamingAColumnDoesNotChangeItsIdentity(): void
    {
        [$database, $authorization] = $this->database();

        $identities = fn (): array => $authorization->skip(function () use ($database): array {
            $map = [];

            foreach ($database->getCollection('employees')->getAttribute('attributes', []) as $attribute) {
                $map[$attribute['key']] = $attribute[Database::ATTRIBUTE_INTERNAL_ID] ?? '';
            }

            return $map;
        });

        $authorization->skip(function () use ($database) {
            $database->createCollection('employees', documentSecurity: true, columnSecurity: true, permissions: []);
            $database->createAttribute('employees', 'salary', Database::VAR_INTEGER, 8, false);
        });

        $before = $identities();
        $this->assertArrayHasKey('salary', $before);
        $this->assertNotSame('', $before['salary']);

        $authorization->skip(fn () => $database->updateAttribute('employees', 'salary', newKey: 'pay'));

        $after = $identities();
        $this->assertArrayNotHasKey('salary', $after, 'the key moved');
        $this->assertSame($before['salary'], $after['pay'] ?? null, 'the identity did not');
    }
}
