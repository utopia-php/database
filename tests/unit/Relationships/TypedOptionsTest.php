<?php

namespace Tests\Unit\Relationships;

use Closure;
use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Restricted as RestrictedException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Validator\Authorization;

final class TypedOptionsTest extends TestCase
{
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
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testBothSidesStoreTheRelationshipOptionsShape(Closure $adapter): void
    {
        $database = $this->database($adapter());

        $created = $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'albums', twoWay: true, twoWayKey: 'artist'));

        $this->assertSame($this->sorted($created->toOptions(RelationshipSide::Parent)), $this->storedOptions($database, 'artists', 'albums'));
        $this->assertSame($this->sorted($created->inverse('artists')->toOptions(RelationshipSide::Child)), $this->storedOptions($database, 'albums', 'artist'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAnUpdateFromTheChildSideStoresTheOptionsOnBothSides(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'albums', twoWay: true, twoWayKey: 'artist'));

        $updated = $database->updateRelationship('albums', 'artist', new RelationshipUpdate(onDelete: RelationshipDeleteAction::SetNull));

        $this->assertSame('artists', $updated->relatedCollection);
        $this->assertSame('artist', $updated->key);
        $this->assertSame('albums', $updated->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::SetNull, $updated->onDelete);
        $this->assertSame($this->sorted($updated->toOptions(RelationshipSide::Child)), $this->storedOptions($database, 'albums', 'artist'));
        $this->assertSame($this->sorted($updated->inverse('albums')->toOptions(RelationshipSide::Parent)), $this->storedOptions($database, 'artists', 'albums'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testDeletingFollowsTheStoredDeleteAction(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'albums', twoWay: true, twoWayKey: 'artist'));
        $database->createDocument('albums', new Document(['$id' => 'b1', 'title' => 'First']));
        $database->createDocument('artists', new Document(['$id' => 'a1', 'name' => 'Ada', 'albums' => ['b1']]));

        try {
            $database->deleteDocument('artists', 'a1');
            $this->fail('A restricted relationship must keep a parent that has children');
        } catch (RestrictedException) {
            $this->assertFalse($database->getDocument('artists', 'a1')->isEmpty());
        }

        $database->updateRelationship('albums', 'artist', new RelationshipUpdate(onDelete: RelationshipDeleteAction::SetNull));
        $database->deleteDocument('artists', 'a1');

        $album = $database->getDocument('albums', 'b1');
        $this->assertFalse($album->isEmpty());
        $this->assertNull($album->getAttribute('artist'));
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testPopulationReadsEachSideFromItsStoredSide(Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'albums', twoWay: true, twoWayKey: 'artist'));
        $database->createDocument('albums', new Document(['$id' => 'b1', 'title' => 'First']));
        $database->createDocument('albums', new Document(['$id' => 'b2', 'title' => 'Second']));
        $database->createDocument('artists', new Document(['$id' => 'a1', 'name' => 'Ada', 'albums' => ['b1', 'b2']]));

        $albums = $database->getDocument('artists', 'a1')->getAttribute('albums');
        $this->assertIsArray($albums);
        $ids = [];
        foreach ($albums as $album) {
            $this->assertInstanceOf(Document::class, $album);
            $ids[] = $album->getId();
        }
        \sort($ids);
        $this->assertSame(['b1', 'b2'], $ids);

        $artist = $database->getDocument('albums', 'b2')->getAttribute('artist');
        $this->assertInstanceOf(Document::class, $artist);
        $this->assertSame('a1', $artist->getId());
        $this->assertSame('Ada', $artist->getAttribute('name'));
    }

    public function testAOneWayChildSideIsNotPopulated(): void
    {
        $database = $this->database(new Memory());
        $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'albums', twoWayKey: 'artist'));
        $database->createDocument('albums', new Document(['$id' => 'b1', 'title' => 'First']));
        $database->createDocument('artists', new Document(['$id' => 'a1', 'name' => 'Ada', 'albums' => ['b1']]));

        $album = $database->getDocument('albums', 'b1');

        $this->assertFalse($album->isEmpty());
        $this->assertArrayNotHasKey('artist', $album->getArrayCopy());
    }

    /**
     * @return array<string, mixed>
     */
    private function storedOptions(Database $database, string $collection, string $key): array
    {
        $metadata = $database->getAuthorization()->skip(fn (): Document => $database->getDocument(Database::METADATA, $collection));

        /** @var array<Document|array<string, mixed>> $stored */
        $stored = $metadata->getAttribute('attributes', []);
        foreach ($stored as $attribute) {
            $attribute = $attribute instanceof Document ? $attribute : new Document($attribute);
            if ($attribute->getId() !== $key) {
                continue;
            }

            $options = $attribute->getAttribute('options', []);
            /** @var array<string, mixed> $options */
            $options = $options instanceof Document ? $options->getArrayCopy() : $options;

            return $this->sorted($options);
        }

        $this->fail('Collection "'.$collection.'" stores no attribute "'.$key.'"');
    }

    /**
     * @param  array<string, mixed>  $options
     * @return array<string, mixed>
     */
    private function sorted(array $options): array
    {
        \ksort($options);

        return $options;
    }

    private function database(Adapter $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('typed_options')
            ->setNamespace('typed_options_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships($database));

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())];
        $database->createCollection(Collection::create('artists', attributes: [Attribute::string('name', 64)], permissions: $permissions, documentSecurity: false));
        $database->createCollection(Collection::create('albums', attributes: [Attribute::string('title', 64)], permissions: $permissions, documentSecurity: false));

        return $database;
    }
}
