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
use Utopia\Database\Attribute;
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

final class TwoWayOneToOneDocumentLinkTest extends TestCase
{
    /**
     * @return iterable<string, array{Closure(): Adapter}>
     */
    public static function adapters(): iterable
    {
        yield 'memory' => [static fn (): Adapter => new Memory()];
        yield 'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))];
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingAnExistingDocumentByDocumentStoresTheBackReference(Closure $adapter): void
    {
        $database = $this->database($adapter);

        $updated = $database->updateDocument('parent', 'p2', new Document(['partner' => new Document(['$id' => 'c1'])]));

        $this->assertSame('c1', $this->idOf($updated->getAttribute('partner')));
        $this->assertSame('c1', $this->link($database, 'parent', 'p2', 'partner'));
        $this->assertSame('p2', $this->link($database, 'child', 'c1', 'parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingAnExistingDocumentByIdStoresTheBackReference(Closure $adapter): void
    {
        $database = $this->database($adapter);

        $database->updateDocument('parent', 'p2', new Document(['partner' => 'c1']));

        $this->assertSame('c1', $this->link($database, 'parent', 'p2', 'partner'));
        $this->assertSame('p2', $this->link($database, 'child', 'c1', 'parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingFromTheChildSideByDocumentStoresTheBackReference(Closure $adapter): void
    {
        $database = $this->database($adapter);

        $database->updateDocument('child', 'c1', new Document(['parent' => new Document(['$id' => 'p2'])]));

        $this->assertSame('p2', $this->link($database, 'child', 'c1', 'parent'));
        $this->assertSame('c1', $this->link($database, 'parent', 'p2', 'partner'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingByDocumentWritesTheDocumentsOwnAttributesWithTheBackReference(Closure $adapter): void
    {
        $database = $this->database($adapter);

        $database->updateDocument('parent', 'p2', new Document(['partner' => new Document(['$id' => 'c1', 'name' => 'renamed'])]));

        $child = $this->stored($database, 'child', 'c1');
        $this->assertSame('renamed', $child->getAttribute('name'));
        $this->assertSame('p2', $child->getAttribute('parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingADocumentLinkedElsewhereByDocumentIsRefused(Closure $adapter): void
    {
        $database = $this->database($adapter);

        try {
            $database->updateDocument('parent', 'p2', new Document(['partner' => new Document(['$id' => 'c3'])]));
            $this->fail('Linking a document that is already linked elsewhere was accepted');
        } catch (Throwable $exception) {
            $this->assertInstanceOf(DuplicateException::class, $exception, $exception::class.': '.$exception->getMessage());
        }

        $this->assertNull($this->link($database, 'parent', 'p2', 'partner'));
        $this->assertSame('p1', $this->link($database, 'child', 'c3', 'parent'));
        $this->assertSame('c3', $this->link($database, 'parent', 'p1', 'partner'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testUnlinkingThenRelinkingByDocumentStoresTheBackReference(Closure $adapter): void
    {
        $database = $this->database($adapter);

        $database->updateDocument('parent', 'p1', new Document(['partner' => null]));
        $database->updateDocument('parent', 'p2', new Document(['partner' => new Document(['$id' => 'c3'])]));

        $this->assertNull($this->link($database, 'parent', 'p1', 'partner'));
        $this->assertSame('c3', $this->link($database, 'parent', 'p2', 'partner'));
        $this->assertSame('p2', $this->link($database, 'child', 'c3', 'parent'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingANewDocumentByDocumentStoresTheBackReference(Closure $adapter): void
    {
        $database = $this->database($adapter);

        $database->updateDocument('parent', 'p2', new Document(['partner' => new Document(['$id' => 'c9', 'name' => 'new'])]));

        $this->assertSame('c9', $this->link($database, 'parent', 'p2', 'partner'));
        $this->assertSame('p2', $this->link($database, 'child', 'c9', 'parent'));
        $this->assertSame('new', $this->stored($database, 'child', 'c9')->getAttribute('name'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testLinkingByDocumentWritesNestedDocumentsToTheSameDepthAsCreating(Closure $adapter): void
    {
        $database = $this->database($adapter);

        $database->createDocument('parent', new Document([
            '$id' => 'p8',
            'partner' => ['$id' => 'c8', 'toy' => ['$id' => 't8', 'part' => ['$id' => 'x8']]],
        ]));
        $database->updateDocument('parent', 'p2', new Document([
            'partner' => new Document(['$id' => 'c1', 'toy' => ['$id' => 't1', 'part' => ['$id' => 'x1']]]),
        ]));
        $database->updateDocument('parent', 'p3', new Document([
            'partner' => new Document(['$id' => 'c7', 'toy' => ['$id' => 't7', 'part' => ['$id' => 'x7']]]),
        ]));

        foreach (['created' => ['p8', 'c8', 't8', 'x8'], 'linked' => ['p2', 'c1', 't1', 'x1'], 'new' => ['p3', 'c7', 't7', 'x7']] as $case => [$parent, $child, $toy, $part]) {
            $this->assertSame($child, $this->link($database, 'parent', $parent, 'partner'), $case);
            $this->assertSame($parent, $this->link($database, 'child', $child, 'parent'), $case);
            $this->assertSame($toy, $this->link($database, 'child', $child, 'toy'), $case);
            $this->assertSame($child, $this->link($database, 'toy', $toy, 'owner'), $case);
            $this->assertNull($this->link($database, 'toy', $toy, 'part'), $case);
            $this->assertTrue($this->stored($database, 'part', $part, false)->isEmpty(), $case.': the level past the relation depth limit was written');
        }
    }

    private function idOf(mixed $value): ?string
    {
        if ($value instanceof Document) {
            return $value->getId();
        }

        return \is_string($value) ? $value : null;
    }

    private function link(Database $database, string $collection, string $id, string $key): ?string
    {
        return $this->idOf($this->stored($database, $collection, $id)->getAttribute($key));
    }

    private function stored(Database $database, string $collection, string $id, bool $required = true): Document
    {
        $document = $database->getAuthorization()->skip(fn () => $database->skipRelationships(fn () => $database->getDocument($collection, $id)));

        if ($required) {
            $this->assertFalse($document->isEmpty(), $collection.' '.$id.' is missing');
        }

        return $document;
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
            ->setDatabase('two_way_one_to_one_document')
            ->setNamespace('two_way_one_to_one_document_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships($database));
        $database->addHook(new Permissions());

        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
        foreach (['parent', 'child', 'toy', 'part'] as $collection) {
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 64, required: false)],
                permissions: $permissions,
                documentSecurity: false,
            ));
        }
        $database->createRelationship(Relationship::oneToOne(collection: 'parent', relatedCollection: 'child', twoWay: true, key: 'partner', twoWayKey: 'parent'));
        $database->createRelationship(Relationship::oneToOne(collection: 'child', relatedCollection: 'toy', twoWay: true, key: 'toy', twoWayKey: 'owner'));
        $database->createRelationship(Relationship::oneToOne(collection: 'toy', relatedCollection: 'part', twoWay: true, key: 'part', twoWayKey: 'toy'));

        foreach (['c1', 'c2', 'c3'] as $id) {
            $database->createDocument('child', new Document(['$id' => $id]));
        }
        $database->createDocument('parent', new Document(['$id' => 'p1', 'partner' => 'c3']));
        $database->createDocument('parent', new Document(['$id' => 'p2']));
        $database->createDocument('parent', new Document(['$id' => 'p3']));

        return $database;
    }
}
