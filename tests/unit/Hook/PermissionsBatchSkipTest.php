<?php

namespace Tests\Unit\Hook;

use PDO;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Permission;
use Utopia\Database\PermissionType;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class PermissionsBatchSkipTest extends TestCase
{
    private const string COLLECTION = 'movies';

    private PDO $pdo;

    /**
     * @var list<string>
     */
    private array $statements = [];

    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $this->pdo = new class ('sqlite::memory:', $this->record(...)) extends PDO {
            public function __construct(string $dsn, private readonly \Closure $record)
            {
                parent::__construct($dsn);
            }

            /**
             * @param  array<mixed>  $options
             */
            #[\Override]
            public function prepare(string $query, array $options = []): \PDOStatement|false
            {
                ($this->record)($query);

                return parent::prepare($query, $options);
            }
        };

        $this->database = new Database(new SQLite($this->pdo), new Cache(new None()));
        $this->database
            ->setAuthorization(new Authorization())
            ->setDatabase('permissions')
            ->setNamespace('batch_skip_'.\uniqid());
        $this->database->addHook(new Permissions());
        $this->database->create();
        $this->database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'title', size: 64)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any())],
            documentSecurity: true,
        ));

        foreach (['first', 'second'] as $id) {
            $this->database->createDocument(self::COLLECTION, new Document([
                Document::ID => $id,
                Document::PERMISSIONS => self::stored(),
                'title' => $id,
            ]));
        }
    }

    public function testABulkUpdateKeepingEveryDocumentsPermissionsTouchesNoPermissionRows(): void
    {
        $statements = $this->statementsDuring(fn (): int => $this->database->updateDocuments(
            self::COLLECTION,
            new Document(['title' => 'renamed', Document::PERMISSIONS => \array_reverse(self::stored())]),
        ));

        $this->assertSame([], $this->permissionStatements($statements));
        foreach (['first', 'second'] as $id) {
            $document = $this->database->getDocument(self::COLLECTION, $id);
            $this->assertSame('renamed', $document->getAttribute('title'));
            $this->assertEqualsCanonicalizing(self::stored(), $document->getPermissions());
        }
    }

    public function testABulkUpdateChangingThePermissionsRewritesTheirRows(): void
    {
        $changed = [Permission::read(Role::user('reader')), Permission::update(Role::any())];

        $statements = $this->statementsDuring(fn (): int => $this->database->updateDocuments(
            self::COLLECTION,
            new Document([Document::PERMISSIONS => $changed]),
        ));

        $this->assertNotSame([], $this->permissionStatements($statements));
        $this->assertEqualsCanonicalizing($changed, $this->database->getAuthorization()->skip(
            fn (): array => $this->database->getDocument(self::COLLECTION, 'first')->getPermissions(),
        ));
    }

    public function testABulkUpdateRewritesThePermissionsOfOnlyTheDocumentsThatChangeThem(): void
    {
        $reader = [Permission::read(Role::user('reader')), Permission::update(Role::any())];
        $this->database->updateDocument(self::COLLECTION, 'second', new Document([Document::PERMISSIONS => $reader]));

        $statements = $this->statementsDuring(fn (): int => $this->database->updateDocuments(
            self::COLLECTION,
            new Document(['title' => 'renamed', Document::PERMISSIONS => self::stored()]),
        ));

        $this->assertNotSame([], $this->permissionStatements($statements));
        foreach (['first', 'second'] as $id) {
            $document = $this->database->getAuthorization()->skip(fn (): Document => $this->database->getDocument(self::COLLECTION, $id));
            $this->assertSame('renamed', $document->getAttribute('title'));
            $this->assertEqualsCanonicalizing(self::stored(), $document->getPermissions());
        }
    }

    public function testTheHookReadsNoPermissionsWhenEveryDocumentKeepsItsOwn(): void
    {
        $adapter = new SQLite($this->pdo);
        $adapter->setNamespace('batch_skip_adapter_'.\uniqid());
        $authorization = new Authorization();
        $authorization->disable();
        $adapter->setAuthorization($authorization);
        $adapter->addWriteHook(new Permissions());
        $this->assertTrue($adapter->createCollection(self::COLLECTION));
        $collection = new Document([Document::ID => self::COLLECTION]);
        $documents = $adapter->createDocuments($collection, [
            new Document([Document::ID => 'first', Document::PERMISSIONS => self::stored()]),
            new Document([Document::ID => 'second', Document::PERMISSIONS => self::stored()]),
        ]);
        $updates = new class ([Document::PERMISSIONS => self::stored()]) extends Document {
            public int $calls = 0;

            #[\Override]
            public function getPermissionsByType(PermissionType|string $type): array
            {
                $this->calls++;

                return parent::getPermissionsByType($type);
            }
        };

        $before = \count($this->statements);
        $adapter->updateDocuments($collection, $updates, $documents, ['first' => true, 'second' => true]);

        $this->assertSame(0, $updates->calls, 'no document is eligible, so no permission type is read from the update');
        $this->assertSame([], $this->permissionStatements(\array_slice($this->statements, $before)));
    }

    /**
     * @return list<string>
     */
    private static function stored(): array
    {
        return [Permission::read(Role::any()), Permission::update(Role::any())];
    }

    /**
     * @param  callable(): int  $write
     * @return list<string>
     */
    private function statementsDuring(callable $write): array
    {
        $before = \count($this->statements);
        $this->assertSame(2, $write());

        return \array_slice($this->statements, $before);
    }

    private function record(string $statement): void
    {
        $this->statements[] = $statement;
    }

    /**
     * @param  list<string>  $statements
     * @return list<string>
     */
    private function permissionStatements(array $statements): array
    {
        return \array_values(\array_filter($statements, static fn (string $statement): bool => \str_contains($statement, '_perms')));
    }
}
