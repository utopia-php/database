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
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

final class JoinAliasTest extends TestCase
{
    public function testEachJoinIsReadUnderTheAliasItDeclares(): void
    {
        $database = $this->database();

        $rows = $database->find('main', [
            Query::join('b', 'j1', [Query::on('$id', 'mainId')]),
            Query::join('c', 'j2', [Query::on('$id', 'mainId')]),
            Query::select(['name', 'j1.score']),
        ]);

        $this->assertCount(1, $rows);
        $this->assertSame('m1', $rows[0]->getAttribute('name'));
        $this->assertSame(1, $rows[0]->getAttribute('j1.score'));
    }

    public function testAnAliasSortingBeforeAnEarlierOneStillReadsItsOwnJoin(): void
    {
        $database = $this->database();

        $rows = $database->find('main', [
            Query::join('b', 'j1', [Query::on('$id', 'mainId')]),
            Query::join('c', 'j0', [Query::on('$id', 'mainId')]),
            Query::select(['name', 'j0.score']),
        ]);

        $this->assertCount(1, $rows);
        $this->assertSame(10, $rows[0]->getAttribute('j0.score'));
        $this->assertSame(1, $database->count('main', [
            Query::join('b', 'j1', [Query::on('$id', 'mainId')]),
            Query::join('c', 'j0', [Query::on('$id', 'mainId')]),
        ]));
    }

    public function testAJoinWithoutAnAliasIsReadUnderItsCollection(): void
    {
        $database = $this->database();

        foreach ([
            'factory' => Query::join('b', '', [Query::on('$id', 'mainId')]),
            'constructor' => new Query(Method::Join, 'b', [Query::on('$id', 'mainId')]),
            'parsed' => Query::parseQuery(['method' => 'join', 'attribute' => 'b', 'values' => ['$id', '=', 'mainId']]),
        ] as $case => $join) {
            $rows = $database->find('main', [$join, Query::select(['name', 'b.score'])]);
            $this->assertCount(1, $rows, $case);
            $this->assertSame(1, $rows[0]->getAttribute('b.score'), $case);

            $rows = $database->skipValidation(fn (): array => $database->find('main', [$join, Query::select(['name', 'b.score'])]));
            $this->assertSame(1, $rows[0]->getAttribute('b.score'), $case.' without query validation');
        }
    }

    public function testJoiningACollectionTwiceWithoutAliasesNamesTheCollection(): void
    {
        $database = $this->database();
        $joins = [
            Query::join('b', '', [Query::on('$id', 'mainId')]),
            Query::leftJoin('b', '', [Query::on('$id', 'mainId')]),
        ];

        foreach (['with query validation' => fn (): array => $database->find('main', $joins), 'without query validation' => fn (): array => $database->skipValidation(fn (): array => $database->find('main', $joins))] as $case => $read) {
            try {
                $read();
                $this->fail($case.': the joins ran');
            } catch (QueryException $error) {
                $this->assertSame('Table ID "b" cannot be used as the join alias: another join of the query already uses it as its alias. Pass an alias for the join', $error->getMessage(), $case);
            }
        }
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
            Query::join('b', 'x', [Query::on('$id', 'mainId')]),
            Query::join('c', 'x', [Query::on('$id', 'mainId')]),
        ]];
        yield 'aliases that differ only in case' => [[
            Query::join('b', 'x', [Query::on('$id', 'mainId')]),
            Query::leftJoin('c', 'X', [Query::on('$id', 'mainId')]),
        ]];
        yield 'the main collection alias' => [[
            Query::join('b', Query::DEFAULT_ALIAS, [Query::on('$id', 'mainId')]),
        ]];
        yield 'the main collection alias in another case' => [[
            Query::join('b', \strtoupper(Query::DEFAULT_ALIAS), [Query::on('$id', 'mainId')]),
        ]];
        yield 'a cross join alias that repeats an earlier one' => [[
            Query::join('b', 'x', [Query::on('$id', 'mainId')]),
            Query::crossJoin('c', 'x'),
        ]];
        yield 'an alias that is not an identifier' => [[
            Query::join('b', 'my-alias', [Query::on('$id', 'mainId')]),
        ]];
        yield 'a collection joined twice without aliases' => [[
            Query::join('b', '', [Query::on('$id', 'mainId')]),
            Query::crossJoin('b', ''),
        ]];
        yield 'a join without an alias that repeats an earlier alias' => [[
            Query::join('c', 'b', [Query::on('$id', 'mainId')]),
            Query::join('b', '', [Query::on('$id', 'mainId')]),
        ]];
        yield 'a nested join alias that repeats an earlier one' => [[
            Query::join('b', 'x', [Query::on('$id', 'mainId')]),
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
        $database->createCollection(Collection::create(
            id: 'main',
            attributes: [Attribute::string(key: 'name', size: 64, required: true)],
            permissions: $permissions,
            documentSecurity: false,
        ));
        foreach (['b', 'c'] as $collection) {
            $database->createCollection(Collection::create(
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
