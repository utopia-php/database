<?php

namespace Tests\Unit;

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
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

final class DefaultFilterDecodeTest extends TestCase
{
    private const string COLLECTION = 'payloads';

    /**
     * @return array<string, array{\Closure(): Adapter}>
     */
    public static function adapters(): array
    {
        return [
            'memory' => [static fn (): Adapter => new Memory()],
            'sqlite' => [static fn (): Adapter => new SQLite(new PDO('sqlite::memory:'))],
        ];
    }

    /**
     * @return array<string, array{string, mixed}>
     */
    public static function storedScalars(): array
    {
        return [
            'integer' => ['5', 5],
            'boolean' => ['true', true],
            'string' => ['"x"', 'x'],
            'float' => ['1.5', 1.5],
        ];
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('adapters')]
    public function testJsonAttributeHoldingAScalarDecodesToTheScalar(\Closure $adapter): void
    {
        $database = $this->database($adapter());
        $database->createCollection(new Collection(
            id: self::COLLECTION,
            attributes: [Attribute::string(key: 'payload', size: 64, filters: ['json'])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));

        foreach (self::storedScalars() as $id => [$stored]) {
            $database->createDocument(self::COLLECTION, new Document([Document::ID => $id, 'payload' => $stored]));
        }

        foreach (self::storedScalars() as $id => [, $decoded]) {
            $this->assertSame($decoded, $database->getDocument(self::COLLECTION, $id)->getAttribute('payload'), "getDocument() of {$id}");
            $found = $database->findOne(self::COLLECTION, [Query::equal(Document::ID, [$id])]);
            $this->assertSame($decoded, $found->getAttribute('payload'), "find() of {$id}");
        }
    }

    /**
     * @return array<string, array{\Closure(): Adapter, ColumnType, string}>
     */
    public static function spatialFilters(): array
    {
        $cases = [];
        foreach (self::adapters() as $name => [$adapter]) {
            $cases["{$name} point"] = [$adapter, ColumnType::Point, 'POINT(1 2)'];
            $cases["{$name} linestring"] = [$adapter, ColumnType::Linestring, 'LINESTRING(1 2, 3 4)'];
            $cases["{$name} polygon"] = [$adapter, ColumnType::Polygon, 'POLYGON((0 0, 0 1, 1 1, 0 0))'];
        }

        return $cases;
    }

    /**
     * @param  \Closure(): Adapter  $adapter
     */
    #[DataProvider('spatialFilters')]
    public function testSpatialFilterOnANonSpatialAdapterDecodesToNull(\Closure $adapter, ColumnType $type, string $stored): void
    {
        $database = $this->database($adapter());
        $collection = new Document([
            Document::ID => self::COLLECTION,
            'attributes' => [new Document([
                Document::ID => 'shape',
                'type' => ColumnType::String->value,
                'array' => false,
                'filters' => [$type->value],
            ])],
        ]);

        $decoded = $database->decode($collection, new Document([Document::ID => 'shape', 'shape' => $stored]));

        $this->assertNull($decoded->getAttribute('shape'), "{$type->value} decode without spatial support must read back as null");
    }

    private function database(Adapter $adapter): Database
    {
        $database = new Database($adapter, new Cache(new None()));
        $database->setDatabase('filters')->setNamespace('decode_'.\uniqid());
        $database->create();

        return $database;
    }
}
