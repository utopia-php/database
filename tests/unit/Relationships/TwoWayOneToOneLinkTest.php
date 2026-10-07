<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\Validator\Authorization;

final class TwoWayOneToOneLinkTest extends TestCase
{
    private const string MESSAGE = 'Document already has a related document';

    /**
     * @return array<string, array{Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @return iterable<string, array{Closure(): Adapter, Closure(string): (string|Document)}>
     */
    public static function links(): iterable
    {
        $values = [
            'id' => static fn (string $id): string => $id,
            'document' => static fn (string $id): Document => new Document(['$id' => $id]),
        ];

        foreach (self::adapters() as $adapterName => [$adapter]) {
            foreach ($values as $valueName => $value) {
                yield $adapterName.', '.$valueName => [$adapter, $value];
            }
        }
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  Closure(string): (string|Document)  $value
     */
    #[DataProvider('links')]
    public function testLinkingAFreeDocumentWhoseIdMatchesALinkedDocumentOfThisCollectionSucceeds(Closure $adapter, Closure $value): void
    {
        $database = $this->database($adapter);

        $database->updateDocument('parent', 'c', new Document(['partner' => $value('a')]));

        $this->assertSame('a', $this->link($database, 'parent', 'c', 'partner'));
        $this->assertSame('b', $this->link($database, 'parent', 'a', 'partner'));
        $this->assertSame('a', $this->link($database, 'child', 'b', 'parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  Closure(string): (string|Document)  $value
     */
    #[DataProvider('links')]
    public function testLinkingADocumentLinkedElsewhereThrowsTheRelationshipDuplicate(Closure $adapter, Closure $value): void
    {
        $database = $this->database($adapter);

        $this->assertRelationshipDuplicate(fn () => $database->updateDocument('parent', 'c', new Document(['partner' => $value('L')])));

        $this->assertNull($this->link($database, 'parent', 'c', 'partner'));
        $this->assertSame('x', $this->link($database, 'child', 'L', 'parent'));
        $this->assertSame('L', $this->link($database, 'parent', 'x', 'partner'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     * @param  Closure(string): (string|Document)  $value
     */
    #[DataProvider('links')]
    public function testLinkingFromTheChildSideToADocumentLinkedElsewhereThrowsTheRelationshipDuplicate(Closure $adapter, Closure $value): void
    {
        $database = $this->database($adapter);

        $this->assertRelationshipDuplicate(fn () => $database->updateDocument('child', 'free', new Document(['parent' => $value('x')])));

        $this->assertNull($this->link($database, 'child', 'free', 'parent'));
        $this->assertSame('L', $this->link($database, 'parent', 'x', 'partner'));
        $this->assertSame('x', $this->link($database, 'child', 'L', 'parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingAFreeDocumentByIdWhoseIdMatchesALinkedDocumentOfThisCollectionLinksBothSides(Closure $adapter): void
    {
        $database = $this->database($adapter);

        $database->updateDocument('parent', 'c', new Document(['partner' => 'a']));

        $this->assertSame('a', $this->link($database, 'parent', 'c', 'partner'));
        $this->assertSame('c', $this->link($database, 'child', 'a', 'parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testUnlinkingThenRelinkingToAnotherDocumentSucceeds(Closure $adapter): void
    {
        $database = $this->database($adapter);

        $database->updateDocument('parent', 'x', new Document(['partner' => null]));
        $database->updateDocument('parent', 'c', new Document(['partner' => 'L']));

        $this->assertNull($this->link($database, 'parent', 'x', 'partner'));
        $this->assertSame('L', $this->link($database, 'parent', 'c', 'partner'));
        $this->assertSame('c', $this->link($database, 'child', 'L', 'parent'));
    }

    /**
     * @param  callable(): mixed  $write
     */
    private function assertRelationshipDuplicate(callable $write): void
    {
        try {
            $write();
            $this->fail('Linking a document that is already linked elsewhere was accepted');
        } catch (Throwable $exception) {
            $this->assertSame(DuplicateException::class, $exception::class, $exception::class.': '.$exception->getMessage());
            $this->assertSame(self::MESSAGE, $exception->getMessage());
        }
    }

    private function link(Database $database, string $collection, string $id, string $key): ?string
    {
        $document = $database->getAuthorization()->skip(fn () => $database->skipRelationships(fn () => $database->getDocument($collection, $id)));
        $this->assertFalse($document->isEmpty(), $collection.' '.$id.' is missing');
        $value = $document->getAttribute($key);
        if ($value instanceof Document) {
            return $value->getId();
        }
        $this->assertTrue($value === null || \is_string($value));

        return $value;
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    private function database(Closure $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($adapter(), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('two_way_one_to_one')
            ->setNamespace('two_way_one_to_one_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships());
        $database->addHook(new Permissions());

        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
        $database->createCollection(Collection::create(id: 'parent', permissions: $permissions, documentSecurity: false));
        $database->createCollection(Collection::create(id: 'child', permissions: $permissions, documentSecurity: false));
        $database->createRelationship('parent', Relationship::oneToOne(relatedCollection: 'child', twoWay: true, key: 'partner', twoWayKey: 'parent'));

        foreach (['a', 'b', 'L', 'free'] as $id) {
            $database->createDocument('child', new Document(['$id' => $id]));
        }
        $database->createDocument('parent', new Document(['$id' => 'x', 'partner' => 'L']));
        $database->createDocument('parent', new Document(['$id' => 'a', 'partner' => 'b']));
        $database->createDocument('parent', new Document(['$id' => 'c']));

        return $database;
    }
}
