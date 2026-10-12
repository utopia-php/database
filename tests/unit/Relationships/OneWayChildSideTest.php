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
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipUpdate;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;

final class OneWayChildSideTest extends TestCase
{
    /**
     * @return array<string, array{Closure(): Database}>
     */
    public static function databases(): array
    {
        return [
            'memory' => [static fn (): Database => self::database(new Memory())],
            'sqlite' => [static fn (): Database => self::database(new SQLite(new PDO('sqlite::memory:')))],
        ];
    }

    /**
     * @param  Closure(): Database  $factory
     */
    #[DataProvider('databases')]
    public function testAChildSideUpdateRenamesTheParentKey(Closure $factory): void
    {
        $database = $factory();

        $updated = $database->updateRelationship('passports', 'holder', new RelationshipUpdate(key: 'owner', twoWayKey: 'document'));

        $this->assertSame('owner', $updated->key);
        $this->assertSame('document', $updated->twoWayKey);
        $this->assertSame(RelationshipSide::Child, $this->attribute($database, 'passports', 'owner')?->side);
        $this->assertSame(RelationshipSide::Parent, $this->attribute($database, 'people', 'document')?->side);
        $this->assertNull($this->attribute($database, 'people', 'passport'));
        $this->assertSame(['_index_document'], $this->indexKeys($database, 'people'));
        $this->assertSame([], $this->indexKeys($database, 'passports'));

        $person = $database->getDocument('people', 'p1');
        $this->assertSame('x1', $this->id($person->getAttribute('document')));
        $this->assertFalse($person->offsetExists('passport'));
    }

    /**
     * @param  Closure(): Database  $factory
     */
    #[DataProvider('databases')]
    public function testAChildSideDeleteRemovesTheParentKey(Closure $factory): void
    {
        $database = $factory();

        $database->deleteRelationship('passports', 'holder');

        $this->assertNull($this->attribute($database, 'passports', 'holder'));
        $this->assertNull($this->attribute($database, 'people', 'passport'));
        $this->assertSame([], $this->indexKeys($database, 'people'));

        $person = $database->getDocument('people', 'p1');
        $this->assertSame('Ada', $person->getAttribute('name'));
        $this->assertNull($person->getAttribute('passport'));

        $database->createRelationship('people', Relationship::oneToOne('passports', key: 'passport', twoWayKey: 'holder'));
        $database->updateDocument('people', 'p1', new Document(['passport' => 'x1']));

        $this->assertSame('x1', $this->id($database->getDocument('people', 'p1')->getAttribute('passport')));
    }

    private static function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database
            ->setAuthorization(new Authorization())
            ->setDatabase('one_way')
            ->setNamespace('one_way_'.\uniqid());
        $database->create();

        $permissions = [
            Permission::create(Role::any()),
            Permission::read(Role::any()),
            Permission::update(Role::any()),
            Permission::delete(Role::any()),
        ];
        $database->createCollection(Collection::create('people', attributes: [Attribute::string('name', 64)], permissions: $permissions, documentSecurity: false));
        $database->createCollection(Collection::create('passports', attributes: [Attribute::string('number', 64)], permissions: $permissions, documentSecurity: false));
        $database->createRelationship('people', Relationship::oneToOne('passports', key: 'passport', twoWayKey: 'holder'));

        $database->createDocument('passports', new Document(['$id' => 'x1', 'number' => 'A-1']));
        $database->createDocument('people', new Document(['$id' => 'p1', 'name' => 'Ada', 'passport' => 'x1']));

        return $database;
    }

    private function attribute(Database $database, string $collection, string $key): ?Attribute
    {
        foreach ($database->getCollection($collection)->attributes() as $attribute) {
            if ($attribute->key === $key) {
                return $attribute;
            }
        }

        return null;
    }

    private function id(mixed $related): ?string
    {
        return match (true) {
            $related instanceof Document => $related->getId(),
            \is_string($related) => $related,
            default => null,
        };
    }

    /**
     * @return list<string>
     */
    private function indexKeys(Database $database, string $collection): array
    {
        return \array_map(static fn (Index $index): string => $index->key, $database->getCollection($collection)->indexes());
    }
}
