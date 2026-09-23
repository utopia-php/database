<?php

namespace Tests\Unit\Documents;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Feature\Upserts;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Permissions;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationType;

final class LiteralUniqueIdTest extends TestCase
{
    private const string LITERAL_ID = 'unique()';

    private const string WIDGETS = 'widgets';

    private const string ALBUMS = 'albums';

    private const string ARTISTS = 'artists';

    private const string ARTIST = 'artist';

    /**
     * @return iterable<string, array{Adapter}>
     */
    public static function adapters(): iterable
    {
        yield 'SQLite' => [new SQLite(new PDO('sqlite::memory:'))];
        yield 'Memory' => [new Memory()];
    }

    /**
     * @return iterable<string, array{Adapter}>
     */
    public static function upsertAdapters(): iterable
    {
        foreach (self::adapters() as $name => [$adapter]) {
            if ($adapter->hasFeature(Upserts::class)) {
                yield $name => [$adapter];
            }
        }
    }

    #[DataProvider('adapters')]
    public function testCreateDocumentStoresTheLiteralId(Adapter $adapter): void
    {
        $database = $this->database($adapter, self::WIDGETS);

        $created = $database->createDocument(self::WIDGETS, new Document([
            Document::ID => self::LITERAL_ID,
            'name' => 'created',
        ]));

        $this->assertSame(self::LITERAL_ID, $created->getId());
        $this->assertStoredUnderLiteralId($database, self::WIDGETS, 'created');
    }

    #[DataProvider('adapters')]
    public function testCreateDocumentsStoresTheLiteralId(Adapter $adapter): void
    {
        $database = $this->database($adapter, self::WIDGETS);

        $created = [];
        $count = $database->createDocuments(
            self::WIDGETS,
            [new Document([Document::ID => self::LITERAL_ID, 'name' => 'batched'])],
            onNext: function (Document $document) use (&$created): void {
                $created[] = $document->getId();
            },
        );

        $this->assertSame(1, $count);
        $this->assertSame([self::LITERAL_ID], $created);
        $this->assertStoredUnderLiteralId($database, self::WIDGETS, 'batched');
    }

    #[DataProvider('upsertAdapters')]
    public function testUpsertDocumentsStoresAndUpdatesTheLiteralId(Adapter $adapter): void
    {
        $database = $this->database($adapter, self::WIDGETS);

        foreach (['inserted', 'updated'] as $name) {
            $upserted = [];
            $database->upsertDocuments(
                self::WIDGETS,
                [new Document([Document::ID => self::LITERAL_ID, 'name' => $name])],
                onNext: function (Document $document) use (&$upserted): void {
                    $upserted[] = $document->getId();
                },
            );

            $this->assertSame([self::LITERAL_ID], $upserted);
            $this->assertStoredUnderLiteralId($database, self::WIDGETS, $name);
        }
    }

    #[DataProvider('adapters')]
    public function testANestedRelatedDocumentStoresTheLiteralId(Adapter $adapter): void
    {
        $database = $this->database($adapter, self::ALBUMS, self::ARTISTS);
        $database->createRelationship(new Relationship(
            collection: self::ALBUMS,
            relatedCollection: self::ARTISTS,
            type: RelationType::ManyToOne,
            key: self::ARTIST,
        ));

        $database->createDocument(self::ALBUMS, new Document([
            Document::ID => 'album',
            'name' => 'album',
            self::ARTIST => [Document::ID => self::LITERAL_ID, 'name' => 'nested'],
        ]));

        $artist = $database->getDocument(self::ALBUMS, 'album')->getAttribute(self::ARTIST);
        $this->assertInstanceOf(Document::class, $artist);
        $this->assertSame(self::LITERAL_ID, $artist->getId(), 'the parent must link to the literal id');
        $this->assertStoredUnderLiteralId($database, self::ARTISTS, 'nested');
    }

    #[DataProvider('adapters')]
    public function testCreateDocumentStillGeneratesAnIdForAnEmptyId(Adapter $adapter): void
    {
        $database = $this->database($adapter, self::WIDGETS);

        $created = $database->createDocument(self::WIDGETS, new Document(['name' => 'generated']));

        $this->assertGeneratedIds($database, [$created->getId()], 1);
    }

    #[DataProvider('adapters')]
    public function testCreateDocumentsStillGeneratesAnIdForAnEmptyId(Adapter $adapter): void
    {
        $database = $this->database($adapter, self::WIDGETS);

        $created = [];
        $database->createDocuments(
            self::WIDGETS,
            [new Document(['name' => 'first']), new Document(['name' => 'second'])],
            onNext: function (Document $document) use (&$created): void {
                $created[] = $document->getId();
            },
        );

        $this->assertGeneratedIds($database, $created, 2);
    }

    #[DataProvider('upsertAdapters')]
    public function testUpsertDocumentsStillGeneratesAnIdForAnEmptyId(Adapter $adapter): void
    {
        $database = $this->database($adapter, self::WIDGETS);

        $upserted = [];
        $database->upsertDocuments(
            self::WIDGETS,
            [new Document(['name' => 'first']), new Document(['name' => 'second'])],
            onNext: function (Document $document) use (&$upserted): void {
                $upserted[] = $document->getId();
            },
        );

        $this->assertGeneratedIds($database, $upserted, 2);
    }

    private function database(Adapter $adapter, string ...$collections): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setDatabase('literal_unique_id')
            ->setNamespace('literal_unique_id_'.\uniqid())
            ->addHook(new Relationships($database))
            ->addHook(new Permissions());
        $database->create();

        foreach ($collections as $collection) {
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [Attribute::string('name', size: 64)],
                permissions: [
                    Permission::create(Role::any()),
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                ],
            ));
        }

        return $database;
    }

    private function assertStoredUnderLiteralId(Database $database, string $collection, string $name): void
    {
        $this->assertSame($name, $database->getDocument($collection, self::LITERAL_ID)->getAttribute('name'));
        $this->assertSame(
            [self::LITERAL_ID],
            \array_map(static fn (Document $document): string => $document->getId(), $database->find($collection)),
            'nothing may be stored under a generated id',
        );
    }

    /**
     * @param  list<string>  $ids
     */
    private function assertGeneratedIds(Database $database, array $ids, int $count): void
    {
        $this->assertCount($count, $ids);
        $this->assertCount($count, \array_unique($ids), 'each document without an id gets its own id');

        foreach ($ids as $id) {
            $this->assertNotSame('', $id);
            $this->assertSame($id, $database->getDocument(self::WIDGETS, $id)->getId());
        }
    }
}
