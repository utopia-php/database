<?php

namespace Tests\Unit\Adapter;

use PDO;
use PDOStatement;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\MariaDB;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Relationship;
use Utopia\Database\RelationshipSide;
use Utopia\Database\RelationshipType;
use Utopia\Database\RelationshipUpdate;

final class SQLChildSideRelationshipRenameTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return iterable<string, array{class-string<SQL>, RelationshipType, RelationshipSide, string|null, string|null, string}>
     */
    public static function renames(): iterable
    {
        $engines = [
            'MariaDB' => [MariaDB::class, 'ALTER TABLE `database`.`namespace_%s` RENAME COLUMN `%s` TO `%s`;'],
            'Postgres' => [Postgres::class, 'ALTER TABLE "database"."namespace_%s" RENAME COLUMN "%s" TO "%s";'],
        ];

        foreach ($engines as $engine => [$class, $statement]) {
            $stored = \sprintf($statement, 'books', 'author', 'writer');
            yield $engine . ' one-to-many key from the child' => [$class, RelationshipType::OneToMany, RelationshipSide::Child, 'writer', null, $stored];
            yield $engine . ' one-to-many two-way key from the parent' => [$class, RelationshipType::OneToMany, RelationshipSide::Parent, null, 'writer', $stored];
            yield $engine . ' many-to-one two-way key from the child' => [$class, RelationshipType::ManyToOne, RelationshipSide::Child, null, 'writer', $stored];
            yield $engine . ' many-to-one key from the parent' => [$class, RelationshipType::ManyToOne, RelationshipSide::Parent, 'writer', null, $stored];
        }
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('renames')]
    public function testARenameTouchesTheColumnTheSideStores(string $class, RelationshipType $type, RelationshipSide $side, ?string $newKey, ?string $newTwoWayKey, string $expected): void
    {
        [$collection, $relationship] = $this->relationship($type, $side);

        $this->assertTrue($this->adapter($class)->updateRelationship($collection, $relationship, $side, new RelationshipUpdate(key: $newKey, twoWayKey: $newTwoWayKey)));

        $this->assertSame([$expected], $this->statements);
    }

    /**
     * @return iterable<string, array{class-string<SQL>, RelationshipType, RelationshipSide, string|null, string|null}>
     */
    public static function renamesOfColumnsTheSideDoesNotStore(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'Postgres' => Postgres::class] as $engine => $class) {
            yield $engine . ' one-to-many two-way key from the child' => [$class, RelationshipType::OneToMany, RelationshipSide::Child, null, 'writer'];
            yield $engine . ' one-to-many key from the parent' => [$class, RelationshipType::OneToMany, RelationshipSide::Parent, 'writer', null];
            yield $engine . ' many-to-one key from the child' => [$class, RelationshipType::ManyToOne, RelationshipSide::Child, 'writer', null];
            yield $engine . ' many-to-one two-way key from the parent' => [$class, RelationshipType::ManyToOne, RelationshipSide::Parent, null, 'writer'];
            yield $engine . ' unchanged key from the child' => [$class, RelationshipType::OneToMany, RelationshipSide::Child, 'author', null];
            yield $engine . ' unchanged two-way key from the parent' => [$class, RelationshipType::OneToMany, RelationshipSide::Parent, null, 'author'];
        }
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('renamesOfColumnsTheSideDoesNotStore')]
    public function testARenameOfAColumnTheSideDoesNotStoreSendsNothing(string $class, RelationshipType $type, RelationshipSide $side, ?string $newKey, ?string $newTwoWayKey): void
    {
        [$collection, $relationship] = $this->relationship($type, $side);

        $this->assertTrue($this->adapter($class)->updateRelationship($collection, $relationship, $side, new RelationshipUpdate(key: $newKey, twoWayKey: $newTwoWayKey)));

        $this->assertSame([], $this->statements);
    }

    /**
     * @return array{string, Relationship}
     */
    private function relationship(RelationshipType $type, RelationshipSide $side): array
    {
        $booksStoreTheKey = ($type === RelationshipType::OneToMany) === ($side === RelationshipSide::Child);

        return $booksStoreTheKey
            ? ['books', self::define($type, relatedCollection: 'authors', key: 'author', twoWayKey: 'books')]
            : ['authors', self::define($type, relatedCollection: 'books', key: 'books', twoWayKey: 'author')];
    }

    private static function define(RelationshipType $type, string $relatedCollection, string $key, string $twoWayKey): Relationship
    {
        return match ($type) {
            RelationshipType::OneToMany => Relationship::oneToMany($relatedCollection, $key, twoWay: true, twoWayKey: $twoWayKey),
            RelationshipType::ManyToOne => Relationship::manyToOne($relatedCollection, $key, twoWay: true, twoWayKey: $twoWayKey),
            default => throw new \LogicException('Only one-to-many and many-to-one relationships are renamed here'),
        };
    }

    /**
     * @param class-string<SQL> $class
     */
    private function adapter(string $class): SQL
    {
        $pdo = $this->createStub(PDO::class);
        $pdo->method('prepare')->willReturnCallback(function (string $query): PDOStatement {
            $this->statements[] = $query;
            $statement = $this->createStub(PDOStatement::class);
            $statement->method('execute')->willReturn(true);

            return $statement;
        });

        $adapter = new $class($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        return $adapter;
    }
}
