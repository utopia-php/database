<?php

namespace Tests\Unit\Adapter;

use PDO;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None as NoCache;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\NotFound as NotFoundException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Method;

final class SQLJoinWithoutValidationTest extends TestCase
{
    private const string NAMESPACE = 'join_without_validation';

    private Database $database;

    protected function setUp(): void
    {
        $this->database = new Database(new SQLite(new PDO('sqlite::memory:')), new Cache(new NoCache()));
        $this->database
            ->setDatabase(self::NAMESPACE)
            ->setNamespace(self::NAMESPACE)
            ->setAuthorization(new Authorization());
        $this->database->create();

        $this->createCollection('customers', [Attribute::string('name', size: 64)]);
        $this->createCollection('notes', [Attribute::string('customerId', size: 64), Attribute::string('body', size: 64)]);

        foreach (['c1', 'c2', 'c3'] as $customer) {
            $this->database->createDocument('customers', new Document(['$id' => $customer, 'name' => $customer]));
        }
        foreach (['n1' => 'c1', 'n2' => 'c1', 'n3' => 'c2'] as $note => $customer) {
            $this->database->createDocument('notes', new Document(['$id' => $note, 'customerId' => $customer, 'body' => $note]));
        }

        $this->database->disableValidation();
    }

    public function testAJoinedSumOfAnInternalAttributeReadsTheMainTable(): void
    {
        $sequences = [];
        foreach ($this->database->find('customers') as $customer) {
            $sequences[$customer->getId()] = (int) $customer->getSequence();
        }

        $sum = $this->database->sum('customers', '$sequence', [Query::join('notes', '$id', 'customerId', '=', 'note')]);

        $this->assertSame($sequences['c1'] * 2 + $sequences['c2'], $sum);
    }

    public function testAJoinWithANonStringColumnIsAQueryError(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Join columns must be strings');

        $this->database->find('customers', [new Query(Method::LeftJoin, 'notes', ['$id', '=', 5, 'note'])]);
    }

    public function testANestedJoinConditionWithoutAColumnIsAQueryError(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Join ON requires left and right columns');

        $this->database->find('customers', [Query::leftJoin('notes', 'note', [Query::on('', 'customerId')])]);
    }

    public function testANestedJoinConditionWithBothColumnsJoins(): void
    {
        $rows = $this->database->find('customers', [
            Query::join('notes', 'note', [Query::on('$id', 'customerId')]),
            Query::select(['$id', 'note.body']),
        ]);

        $this->assertCount(3, $rows);
    }

    public function testAJoinWithoutACollectionIsAQueryError(): void
    {
        $this->expectException(QueryException::class);
        $this->expectExceptionMessage("Joined collection '' not found");

        $this->database->find('customers', [Query::join('', '$id', 'customerId')]);
    }

    /**
     * @return array<string, array{string, list<Query>}>
     */
    public static function sumsOverAnUnknownPrefix(): array
    {
        $join = [Query::join('notes', '$id', 'customerId', '=', 'note')];

        return [
            'plain name beside a join' => ['other.body', $join],
            'name holding the quote char beside a join' => ['other`.body', $join],
            'name holding the quote char without a join' => ['other`.body', []],
        ];
    }

    /**
     * @param  list<Query>  $queries
     */
    #[DataProvider('sumsOverAnUnknownPrefix')]
    public function testASumOverANameWhosePrefixIsNotAJoinAliasIsAnUnknownAttribute(string $attribute, array $queries): void
    {
        $this->expectException(NotFoundException::class);
        $this->expectExceptionMessage('Attribute not found');

        $this->database->sum('customers', $attribute, $queries);
    }

    /**
     * @param list<Attribute> $attributes
     */
    private function createCollection(string $id, array $attributes): void
    {
        $this->database->createCollection(Collection::create(
            id: $id,
            attributes: $attributes,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
    }
}
