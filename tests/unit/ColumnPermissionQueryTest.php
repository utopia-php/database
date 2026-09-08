<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

/**
 * Query, aggregate and write gates for column-level permissions.
 *
 * The collection grants `any` read and create on `name` only, so every caller is
 * demonstrably column-restricted and the gates are exercised without a stored
 * document having to be consulted.
 */
class ColumnPermissionQueryTest extends TestCase
{
    protected Authorization $authorization;

    protected Database $database;

    protected function setUp(): void
    {
        $this->authorization = new Authorization();

        $this->database = new Database(new Memory(), new Cache(new NoCache()));
        $this->database
            ->setAuthorization($this->authorization)
            ->setDatabase('columnPermissions')
            ->setNamespace('colq_' . \uniqid());

        if (!$this->database->exists()) {
            $this->database->create();
        }

        $this->authorization->skip(function () {
            $this->database->createCollection('employees', documentSecurity: true, permissions: [
                Permission::read(Role::any(), 'name'),
                Permission::create(Role::any(), 'name'),
            ]);

            $this->database->createAttribute('employees', 'name', Database::VAR_STRING, 128, false);
            $this->database->createAttribute('employees', 'salary', Database::VAR_INTEGER, 8, false);

            $this->database->createDocument('employees', new Document([
                '$id' => 'e1',
                '$permissions' => [
                    Permission::read(Role::user('hr'), 'salary'),
                    Permission::update(Role::any(), 'name'),
                ],
                'name' => 'Bob',
                'salary' => 100000,
            ]));
        });

        $this->authorization->cleanRoles();
        $this->authorization->addRole('any');
    }

    public function testCreateOfGrantedColumnIsAllowed(): void
    {
        $created = $this->database->createDocument('employees', new Document([
            '$id' => 'c1',
            'name' => 'Alice',
        ]));

        $this->assertSame('c1', $created->getId());
    }

    public function testCreateOfUngrantedColumnIsRejected(): void
    {
        $this->expectException(AuthorizationException::class);
        $this->expectExceptionMessage('Missing "create" permission for column "salary"');

        $this->database->createDocument('employees', new Document([
            '$id' => 'c2',
            'salary' => 9,
        ]));
    }

    public function testFilterOnReadableColumnIsAllowed(): void
    {
        $this->assertCount(1, $this->database->find('employees', [Query::equal('name', ['Bob'])]));
    }

    /**
     * Masking hides the value, but an unguarded filter turns the result set into an
     * oracle for it.
     */
    public function testFilterOnUnreadableColumnIsRejected(): void
    {
        $this->expectException(AuthorizationException::class);
        $this->expectExceptionMessage('Missing "read" permission for column "salary"');

        $this->database->find('employees', [Query::greaterThan('salary', 1)]);
    }

    public function testOrderByUnreadableColumnIsRejected(): void
    {
        $this->expectException(AuthorizationException::class);

        $this->database->find('employees', [Query::orderDesc('salary')]);
    }

    public function testSelectOfUnreadableColumnIsRejected(): void
    {
        $this->expectException(AuthorizationException::class);

        $this->database->find('employees', [Query::select(['salary'])]);
    }

    public function testCountFilteredByUnreadableColumnIsRejected(): void
    {
        $this->expectException(AuthorizationException::class);

        $this->database->count('employees', [Query::equal('salary', [100000])]);
    }

    /**
     * Without this guard sum() extracts a masked column in a single call.
     */
    public function testSumOfUnreadableColumnIsRejected(): void
    {
        $this->expectException(AuthorizationException::class);
        $this->expectExceptionMessage('Missing "read" permission for column "salary"');

        $this->database->sum('employees', 'salary');
    }

    public function testBulkUpdateOfGrantedColumnIsAllowed(): void
    {
        $this->assertSame(1, $this->database->updateDocuments('employees', new Document([
            'name' => 'Renamed',
        ])));
    }

    /**
     * The collection grants no update at all, so the restriction is only visible on
     * the document itself: the bulk path has to check each row, not just the schema.
     */
    public function testBulkUpdateOfUngrantedColumnIsRejected(): void
    {
        $this->expectException(AuthorizationException::class);
        $this->expectExceptionMessage('Missing "update" permission for column "salary"');

        $this->database->updateDocuments('employees', new Document(['salary' => 1]));
    }

    public function testIncreaseOfUngrantedColumnIsRejected(): void
    {
        $this->expectException(AuthorizationException::class);
        $this->expectExceptionMessage('Missing "update" permission for column "salary"');

        $this->database->increaseDocumentAttribute('employees', 'e1', 'salary', 1);
    }

    public function testPermissionsScopedToUnreadableColumnsAreMasked(): void
    {
        $document = $this->database->getDocument('employees', 'e1');

        $this->assertSame(['update("any", "name")'], $document->getPermissions());
    }

    /**
     * Because $permissions is masked, writing a document straight back would delete
     * the grants the caller never saw.
     */
    public function testMaskedPermissionsSurviveARoundTrip(): void
    {
        $document = $this->database->getDocument('employees', 'e1');

        $this->database->updateDocument('employees', 'e1', new Document([
            '$permissions' => $document->getPermissions(),
            'name' => 'Bob2',
        ]));

        $stored = $this->authorization->skip(
            fn () => $this->database->getDocument('employees', 'e1')
        );

        $this->assertContains('read("user:hr", "salary")', $stored->getPermissions());
        $this->assertContains('update("any", "name")', $stored->getPermissions());
        $this->assertSame(100000, $stored->getAttribute('salary'));
    }
}
