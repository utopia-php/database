<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Throwable;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Relationship;
use Utopia\Database\Role;

/**
 * A one-to-many parent, a many-to-one child and either side of a many-to-many keep their related ids on the other
 * collection or in a junction table, so their collection has no column to order by.
 */
final class RelationshipOrderTest extends TestCase
{
    private Database $database;

    #[\Override]
    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $this->database
            ->setDatabase('relationship_order')
            ->setNamespace('relationship_order_'.\uniqid());
        $this->database->getAuthorization()->addRole(Role::any()->toString());
        $this->database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        foreach (['authors', 'books', 'tags', 'covers', 'shelves'] as $collection) {
            $this->database->createCollection(Collection::create(
                id: $collection,
                attributes: [Attribute::string(key: 'name', size: 16)],
                permissions: $permissions,
            ));
        }

        $this->database->createRelationship('authors', Relationship::oneToMany(relatedCollection: 'books', key: 'books', twoWay: true, twoWayKey: 'author'));
        $this->database->createRelationship('books', Relationship::manyToMany(relatedCollection: 'tags', key: 'tags', twoWay: true, twoWayKey: 'books'));
        $this->database->createRelationship('books', Relationship::oneToOne(relatedCollection: 'covers', key: 'cover', twoWay: true, twoWayKey: 'book'));
        $this->database->createRelationship('books', Relationship::manyToOne(relatedCollection: 'shelves', key: 'shelf', twoWay: true, twoWayKey: 'books'));

        $this->database->createDocument('authors', new Document(['$id' => 'ann', 'name' => 'Ann', '$permissions' => [Permission::read(Role::any())]]));
        $this->database->createDocument('books', new Document(['$id' => 'b1', 'name' => 'One', 'author' => 'ann', '$permissions' => [Permission::read(Role::any())]]));
    }

    /**
     * @return iterable<string, array{string, string}>
     */
    public static function columnless(): iterable
    {
        yield 'one-to-many parent' => ['authors', 'books'];
        yield 'many-to-many parent' => ['books', 'tags'];
        yield 'many-to-many child' => ['tags', 'books'];
        yield 'many-to-one child' => ['shelves', 'books'];
    }

    #[DataProvider('columnless')]
    public function testOrderByARelationshipWithoutAColumnIsAnInvalidQuery(string $collection, string $attribute): void
    {
        foreach ([Query::orderAsc($attribute), Query::orderDesc($attribute)] as $order) {
            foreach ([
                'find()' => fn (): mixed => $this->database->find($collection, [$order]),
                'count()' => fn (): mixed => $this->database->count($collection, [$order]),
            ] as $read => $call) {
                $error = null;
                try {
                    $call();
                } catch (Throwable $caught) {
                    $error = $caught;
                }

                $this->assertInstanceOf(QueryException::class, $error, $read.' '.$order->getMethod()->value.': '.($error === null ? 'accepted' : $error::class.': '.$error->getMessage()));
                $this->assertSame('Invalid query: Cannot order by relationship attribute: '.$attribute, $error->getMessage(), $read);
            }
        }
    }

    /**
     * @return iterable<string, array{string, string, list<string>}>
     */
    public static function stored(): iterable
    {
        yield 'one-to-many child' => ['books', 'author', ['b1']];
        yield 'one-to-one parent' => ['books', 'cover', ['b1']];
        yield 'two-way one-to-one child' => ['covers', 'book', []];
        yield 'many-to-one parent' => ['books', 'shelf', ['b1']];
    }

    /**
     * @param  list<string>  $ids
     */
    #[DataProvider('stored')]
    public function testOrderByARelationshipThatHoldsAColumnIsRead(string $collection, string $attribute, array $ids): void
    {
        foreach ([Query::orderAsc($attribute), Query::orderDesc($attribute)] as $order) {
            $documents = $this->database->find($collection, [$order]);
            $this->assertSame($ids, \array_map(static fn (Document $document): string => $document->getId(), $documents));
        }
    }
}
