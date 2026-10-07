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
use Utopia\Database\Exception\Duplicate as DuplicateException;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Hook\Relationships;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipDeleteAction;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\Validator\Authorization;

final class CreateReturnsStoredTest extends TestCase
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
    public function testNullKeysAreDerivedFromTheCollectionIds(Closure $adapter): void
    {
        $database = $this->database($adapter());

        $created = $database->createRelationship('artists', Relationship::oneToMany('albums', twoWay: true));

        $this->assertSame('albums', $created->relatedCollection);
        $this->assertSame(RelationshipType::OneToMany, $created->type);
        $this->assertTrue($created->twoWay);
        $this->assertSame('albums', $created->key);
        $this->assertSame('artists', $created->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::Restrict, $created->onDelete);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testGivenKeysAreKept(Closure $adapter): void
    {
        $database = $this->database($adapter());

        $created = $database->createRelationship('artists', Relationship::manyToMany('albums', key: 'records', twoWay: true, twoWayKey: 'performers', onDelete: RelationshipDeleteAction::Cascade));

        $this->assertSame('records', $created->key);
        $this->assertSame('performers', $created->twoWayKey);
        $this->assertSame(RelationshipDeleteAction::Cascade, $created->onDelete);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testTheReturnedRelationshipIsTheStoredParentSide(Closure $adapter): void
    {
        $database = $this->database($adapter());

        $created = $database->createRelationship('artists', Relationship::manyToOne('albums', twoWay: true, onDelete: RelationshipDeleteAction::SetNull));

        $parent = $this->attribute($database, 'artists', 'albums');
        $this->assertSame(RelationshipSide::Parent, $parent->side);
        $this->assertSame($created->toDocument()->getArrayCopy(), $parent->relationship?->toDocument()->getArrayCopy());
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testTheChildSideIsStoredAsTheInverse(Closure $adapter): void
    {
        $database = $this->database($adapter());

        $created = $database->createRelationship('artists', Relationship::oneToOne('albums', key: 'debut', twoWay: true, twoWayKey: 'artist'));

        $child = $this->attribute($database, 'albums', 'artist');
        $relationship = $child->relationship;
        $this->assertNotNull($relationship);
        $this->assertSame(RelationshipSide::Child, $child->side);
        $this->assertSame($created->inverse('artists')->toDocument()->getArrayCopy(), $relationship->toDocument()->getArrayCopy());
        $this->assertSame('artists', $relationship->relatedCollection);
        $this->assertSame('artist', $relationship->key);
        $this->assertSame('debut', $relationship->twoWayKey);
    }

    /**
     * @param  Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testAOneWayRelationshipStillStoresItsChildSide(Closure $adapter): void
    {
        $database = $this->database($adapter());

        $created = $database->createRelationship('artists', Relationship::oneToMany('albums'));

        $this->assertFalse($created->twoWay);
        $child = $this->attribute($database, 'albums', 'artists');
        $this->assertSame(RelationshipSide::Child, $child->side);
        $this->assertFalse($child->relationship?->twoWay);
    }

    public function testAMissingCollectionIsNotFound(): void
    {
        $database = $this->database(new Memory());

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Collection not found');

        $database->createRelationship('missing', Relationship::oneToMany('albums'));
    }

    public function testAMissingRelatedCollectionIsNotFound(): void
    {
        $database = $this->database(new Memory());

        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Related collection not found');

        $database->createRelationship('artists', Relationship::oneToMany('missing'));
    }

    public function testADerivedKeyThatIsTakenIsADuplicate(): void
    {
        $database = $this->database(new Memory());
        $database->createAttribute('artists', Attribute::string('albums'));

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Attribute already exists');

        $database->createRelationship('artists', Relationship::oneToMany('albums'));
    }

    public function testASecondRelationshipToTheSameTwoWayKeyIsADuplicate(): void
    {
        $database = $this->database(new Memory());
        $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'albums', twoWayKey: 'artist'));

        $this->expectException(DuplicateException::class);
        $this->expectExceptionMessage('Related attribute already exists');

        $database->createRelationship('artists', Relationship::oneToMany('albums', key: 'records', twoWayKey: 'artist'));
    }

    private function attribute(Database $database, string $collection, string $key): Attribute
    {
        foreach ($database->getCollection($collection)->attributes() as $attribute) {
            if ($attribute->key === $key) {
                return $attribute;
            }
        }

        $this->fail('Collection "'.$collection.'" stores no attribute "'.$key.'"');
    }

    private function database(Adapter $adapter): Database
    {
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());

        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization($authorization)
            ->setDatabase('create_returns_stored')
            ->setNamespace('create_returns_stored_'.\uniqid());
        $database->create();
        $database->addHook(new Relationships($database));

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any()), Permission::update(Role::any()), Permission::delete(Role::any())];
        $database->createCollection(Collection::create('artists', attributes: [Attribute::string('name', 64)], permissions: $permissions));
        $database->createCollection(Collection::create('albums', attributes: [Attribute::string('title', 64)], permissions: $permissions));

        return $database;
    }
}
