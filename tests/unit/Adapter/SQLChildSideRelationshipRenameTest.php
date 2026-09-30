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
use Utopia\Database\RelationSide;
use Utopia\Database\RelationType;

final class SQLChildSideRelationshipRenameTest extends TestCase
{
    /** @var list<string> */
    private array $statements = [];

    /**
     * @return iterable<string, array{class-string<SQL>, RelationType, RelationSide, string|null, string|null, string}>
     */
    public static function renames(): iterable
    {
        $engines = [
            'MariaDB' => [MariaDB::class, 'ALTER TABLE `database`.`namespace_%s` RENAME COLUMN `%s` TO `%s`;'],
            'Postgres' => [Postgres::class, 'ALTER TABLE "database"."namespace_%s" RENAME COLUMN "%s" TO "%s";'],
        ];

        foreach ($engines as $engine => [$class, $statement]) {
            $stored = \sprintf($statement, 'books', 'author', 'writer');
            yield $engine . ' one-to-many key from the child' => [$class, RelationType::OneToMany, RelationSide::Child, 'writer', null, $stored];
            yield $engine . ' one-to-many two-way key from the parent' => [$class, RelationType::OneToMany, RelationSide::Parent, null, 'writer', $stored];
            yield $engine . ' many-to-one two-way key from the child' => [$class, RelationType::ManyToOne, RelationSide::Child, null, 'writer', $stored];
            yield $engine . ' many-to-one key from the parent' => [$class, RelationType::ManyToOne, RelationSide::Parent, 'writer', null, $stored];
        }
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('renames')]
    public function testARenameTouchesTheColumnTheSideStores(string $class, RelationType $type, RelationSide $side, ?string $newKey, ?string $newTwoWayKey, string $expected): void
    {
        $this->assertTrue($this->adapter($class)->updateRelationship($this->relationship($type, $side), $newKey, $newTwoWayKey));

        $this->assertSame([$expected], $this->statements);
    }

    /**
     * @return iterable<string, array{class-string<SQL>, RelationType, RelationSide, string|null, string|null}>
     */
    public static function renamesOfColumnsTheSideDoesNotStore(): iterable
    {
        foreach (['MariaDB' => MariaDB::class, 'Postgres' => Postgres::class] as $engine => $class) {
            yield $engine . ' one-to-many two-way key from the child' => [$class, RelationType::OneToMany, RelationSide::Child, null, 'writer'];
            yield $engine . ' one-to-many key from the parent' => [$class, RelationType::OneToMany, RelationSide::Parent, 'writer', null];
            yield $engine . ' many-to-one key from the child' => [$class, RelationType::ManyToOne, RelationSide::Child, 'writer', null];
            yield $engine . ' many-to-one two-way key from the parent' => [$class, RelationType::ManyToOne, RelationSide::Parent, null, 'writer'];
            yield $engine . ' unchanged key from the child' => [$class, RelationType::OneToMany, RelationSide::Child, 'author', null];
            yield $engine . ' unchanged two-way key from the parent' => [$class, RelationType::OneToMany, RelationSide::Parent, null, 'author'];
        }
    }

    /**
     * @param class-string<SQL> $class
     */
    #[DataProvider('renamesOfColumnsTheSideDoesNotStore')]
    public function testARenameOfAColumnTheSideDoesNotStoreSendsNothing(string $class, RelationType $type, RelationSide $side, ?string $newKey, ?string $newTwoWayKey): void
    {
        $this->assertTrue($this->adapter($class)->updateRelationship($this->relationship($type, $side), $newKey, $newTwoWayKey));

        $this->assertSame([], $this->statements);
    }

    private function relationship(RelationType $type, RelationSide $side): Relationship
    {
        $booksStoreTheKey = ($type === RelationType::OneToMany) === ($side === RelationSide::Child);

        return $booksStoreTheKey
            ? new Relationship(collection: 'books', relatedCollection: 'authors', type: $type, twoWay: true, key: 'author', twoWayKey: 'books', side: $side)
            : new Relationship(collection: 'authors', relatedCollection: 'books', type: $type, twoWay: true, key: 'books', twoWayKey: 'author', side: $side);
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
