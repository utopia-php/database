<?php

namespace Tests\Unit;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;

final class JoinAliasTest extends TestCase
{
    public function testGeneratedAliasSkipsAnAliasDeclaredBeforeIt(): void
    {
        $database = $this->database();

        $rows = $database->find('main', [
            Query::join('b', '$id', 'mainId', '=', 'j1'),
            Query::join('c', '$id', 'mainId'),
            Query::select(['name', 'j1.score']),
        ]);

        $this->assertCount(1, $rows);
        $this->assertSame('m1', $rows[0]->getAttribute('name'));
        $this->assertSame(1, $rows[0]->getAttribute('j1.score'));
    }

    public function testGeneratedAliasSkipsAnAliasDeclaredAfterIt(): void
    {
        $database = $this->database();

        $rows = $database->find('main', [
            Query::join('b', '$id', 'mainId'),
            Query::join('c', '$id', 'mainId', '=', 'j0'),
            Query::select(['name', 'j0.score']),
        ]);

        $this->assertCount(1, $rows);
        $this->assertSame(10, $rows[0]->getAttribute('j0.score'));
        $this->assertSame(1, $database->count('main', [
            Query::join('b', '$id', 'mainId'),
            Query::join('c', '$id', 'mainId', '=', 'j0'),
        ]));
    }

    /**
     * @param  list<Query>  $joins
     */
    #[DataProvider('collidingAliases')]
    public function testCollidingAliasIsRejectedByFind(array $joins): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);

        $database->find('main', $joins);
    }

    /**
     * @param  list<Query>  $joins
     */
    #[DataProvider('collidingAliases')]
    public function testCollidingAliasIsRejectedByCount(array $joins): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);

        $database->count('main', $joins);
    }

    /**
     * @param  list<Query>  $joins
     */
    #[DataProvider('collidingAliases')]
    public function testCollidingAliasIsRejectedWithoutQueryValidation(array $joins): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);

        $database->skipValidation(fn () => $database->find('main', $joins));
    }

    /**
     * @param  list<Query>  $joins
     */
    #[DataProvider('collidingAliases')]
    public function testCollidingAliasIsRejectedByGetDocument(array $joins): void
    {
        $database = $this->database();

        $this->expectException(QueryException::class);

        $database->skipValidation(fn () => $database->getDocument('main', 'm1', $joins));
    }

    /**
     * @return iterable<string, array{list<Query>}>
     */
    public static function collidingAliases(): iterable
    {
        yield 'the same alias twice' => [[
            Query::join('b', '$id', 'mainId', '=', 'x'),
            Query::join('c', '$id', 'mainId', '=', 'x'),
        ]];
        yield 'aliases that differ only in case' => [[
            Query::join('b', '$id', 'mainId', '=', 'x'),
            Query::leftJoin('c', '$id', 'mainId', '=', 'X'),
        ]];
        yield 'the main collection alias' => [[
            Query::join('b', '$id', 'mainId', '=', Query::DEFAULT_ALIAS),
        ]];
        yield 'the main collection alias in another case' => [[
            Query::join('b', '$id', 'mainId', '=', \strtoupper(Query::DEFAULT_ALIAS)),
        ]];
        yield 'a cross join alias that repeats an earlier one' => [[
            Query::join('b', '$id', 'mainId', '=', 'x'),
            Query::crossJoin('c', 'x'),
        ]];
        yield 'an alias that is not an identifier' => [[
            Query::join('b', '$id', 'mainId', '=', 'my-alias'),
        ]];
        yield 'a nested join alias that repeats an earlier one' => [[
            Query::join('b', '$id', 'mainId', '=', 'x'),
            Query::leftJoin('c', 'x', [Query::on('$id', 'mainId')]),
        ]];
    }

    private function database(): Database
    {
        $database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new None()));
        $authorization = new Authorization();
        $authorization->addRole(Role::any()->toString());
        $database
            ->setAuthorization($authorization)
            ->setDatabase('aliases')
            ->setNamespace('aliases_'.\uniqid());
        $database->create();

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(new Collection(
            id: 'main',
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: $permissions,
            documentSecurity: false,
        ));
        foreach (['b', 'c'] as $collection) {
            $database->createCollection(new Collection(
                id: $collection,
                attributes: [
                    Attribute::string(key: 'mainId', size: 64, required: true),
                    Attribute::integer(key: 'score', required: true),
                ],
                permissions: $permissions,
                documentSecurity: false,
            ));
        }

        $database->createDocument('main', new Document(['$id' => 'm1', 'name' => 'm1']));
        $database->createDocument('b', new Document(['$id' => 'b1', 'mainId' => 'm1', 'score' => 1]));
        $database->createDocument('c', new Document(['$id' => 'c1', 'mainId' => 'm1', 'score' => 10]));

        return $database;
    }
}
