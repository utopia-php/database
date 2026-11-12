<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class TypedReadersTest extends TestCase
{
    private const string COLLECTION = 'books';

    public function testAReadAppliesTheDeclaredDefaults(): void
    {
        $database = $this->database();
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune', 'pages' => 412]));

        $document = $database->getDocument(self::COLLECTION, 'dune');

        $this->assertSame('Dune', $document->getAttribute('title'));
        $this->assertSame('paperback', $document->getAttribute('format'));
        $this->assertSame(412, $document->getAttribute('pages'));
        $this->assertFalse($document->getAttribute('signed'));
    }

    public function testCastingFollowsTheDeclaredTypes(): void
    {
        $database = $this->database();
        $collection = $database->getCollection(self::COLLECTION);

        $document = $database->casting($collection, new Document(['pages' => '12', 'signed' => 1, 'tags' => '["a","b"]', 'title' => 7]));

        $this->assertSame(12, $document->getAttribute('pages'));
        $this->assertTrue($document->getAttribute('signed'));
        $this->assertSame(['a', 'b'], $document->getAttribute('tags'));
        $this->assertSame(7, $document->getAttribute('title'));
    }

    public function testAPlainCollectionDocumentDecodesLikeTheCollection(): void
    {
        $database = $this->database();
        $collection = $database->getCollection(self::COLLECTION);
        $stored = new Document(['title' => 'Dune', 'tags' => ['a'], 'unknown' => 'kept']);

        $fromCollection = $database->decode($collection, clone $stored);
        $fromDocument = $database->decode($collection->toDocument(), clone $stored);

        $this->assertSame($fromCollection->getArrayCopy(), $fromDocument->getArrayCopy());
    }

    public function testFindFiltersAndOrdersByDeclaredAttributes(): void
    {
        $database = $this->database();
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'a', 'title' => 'A', 'pages' => 30]));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'b', 'title' => 'B', 'pages' => 10]));
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'c', 'title' => 'C', 'pages' => 20]));

        $found = $database->find(self::COLLECTION, [Query::greaterThan('pages', 15), Query::orderAsc('pages')]);

        $this->assertSame(['c', 'a'], \array_map(static fn (Document $document): string => $document->getId(), $found));
        $this->assertSame(2, $database->count(self::COLLECTION, [Query::greaterThan('pages', 15)]));
        $this->assertSame(60, $database->sum(self::COLLECTION, 'pages'));
    }

    public function testAFilterOnAnUndeclaredAttributeIsRefused(): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Attribute not found in schema: missing');

        $database->find(self::COLLECTION, [Query::equal('missing', ['x'])]);
    }

    public function testAFilterValueOfTheWrongTypeIsRefused(): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Query value is invalid for attribute "pages"');

        $database->find(self::COLLECTION, [Query::equal('pages', ['many'])]);
    }

    public function testSelectingAnUndeclaredAttributeIsRefused(): void
    {
        $database = $this->database();
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune']));

        $this->expectException(QueryException::class);

        $database->getDocument(self::COLLECTION, 'dune', [Query::select(['title', 'missing'])]);
    }

    public function testSelectingDeclaredAttributesReturnsOnlyThem(): void
    {
        $database = $this->database();
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune', 'pages' => 412]));

        $document = $database->getDocument(self::COLLECTION, 'dune', [Query::select(['title'])]);

        $this->assertSame('Dune', $document->getAttribute('title'));
        $this->assertFalse($document->offsetExists('pages'));
    }

    public function testIncreaseChangesADeclaredInteger(): void
    {
        $database = $this->database();
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune', 'pages' => 412]));

        $document = $database->increaseDocumentAttribute(self::COLLECTION, 'dune', 'pages', 8);

        $this->assertSame(420, $document->getAttribute('pages'));
    }

    public function testIncreaseOfAStringAttributeIsRefused(): void
    {
        $database = $this->database();
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune']));

        $this->expectException(TypeException::class);

        $database->increaseDocumentAttribute(self::COLLECTION, 'dune', 'title');
    }

    public function testDecreaseOfAnUndeclaredAttributeIsRefused(): void
    {
        $database = $this->database();
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune']));

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Attribute not found');

        $database->decreaseDocumentAttribute(self::COLLECTION, 'dune', 'missing');
    }

    public function testAJoinOfAMissingCollectionIsAQueryError(): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage("Joined collection 'missing' not found");

        $database->count(self::COLLECTION, [Query::join('missing', 'm', [Query::equal('m.title', ['x'])])]);
    }

    public function testUpdatingDocumentsEncodesTheDeclaredFilters(): void
    {
        $database = $this->database();
        $database->createDocument(self::COLLECTION, new Document([Document::ID => 'dune', 'title' => 'Dune', 'tags' => ['a']]));

        $updated = $database->updateDocuments(self::COLLECTION, new Document(['tags' => ['b', 'c']]));

        $this->assertSame(1, $updated);
        $this->assertSame(['b', 'c'], $database->getDocument(self::COLLECTION, 'dune')->getAttribute('tags'));
    }

    private function database(): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('typed_readers')
            ->setNamespace('typed_readers_'.\uniqid());
        $database->create();
        $database->addHook(new Permissions());

        $database->createCollection(Collection::create(
            self::COLLECTION,
            attributes: [
                Attribute::string('title', 64),
                Attribute::string('format', 32, default: 'paperback'),
                Attribute::integer('pages'),
                Attribute::boolean('signed', default: false),
                Attribute::string('tags', 32, array: true),
            ],
            indexes: [Index::key('by_pages', ['pages'])],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            documentSecurity: false,
        ));

        return $database;
    }
}
