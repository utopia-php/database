<?php

namespace Tests\Unit\Documents;

use PHPUnit\Framework\TestCase;
use Utopia\Cache\Adapter\None;
use Utopia\Cache\Cache;
use Utopia\Database\Adapter\Memory;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Operator;
use Utopia\Database\Permission;
use Utopia\Database\Role;

final class FractionalOperatorLimitTest extends TestCase
{
    private const string COLLECTION = 'counters';


    public function testUpdateDocumentAppliesAFractionalLimitOnAnIntegerAs7xDid(): void
    {
        $database = $this->database();

        $updated = $database->updateDocument(self::COLLECTION, 'counter', new Document(['count' => Operator::increment(5, 102.4)]));

        $this->assertSame($database->getDocument(self::COLLECTION, 'counter')->getAttribute('count'), $updated->getAttribute('count'));
    }

    public function testUpdateDocumentsAppliesAFractionalLimitOnAnIntegerAs7xDid(): void
    {
        $database = $this->database();

        $this->assertSame(1, $database->updateDocuments(self::COLLECTION, new Document(['count' => Operator::increment(1, 102.4)])));
        $this->assertSame(101, $database->getDocument(self::COLLECTION, 'counter')->getAttribute('count'));
    }

    public function testUpdateDocumentRefusesAFractionalLimitOnABigIntegerBeforeTheWrite(): void
    {
        $database = $this->database();

        try {
            $database->updateDocument(self::COLLECTION, 'counter', new Document(['big' => Operator::increment(5, 4.0e15 + 0.5)]));
            $this->fail('A fractional limit on a bigint attribute must be refused');
        } catch (StructureException $exception) {
            $this->assertStringStartsWith("Invalid document structure: Cannot apply increment operator: max/min limit must be a whole number for integer attribute 'big'", $exception->getMessage());
        }

        $this->assertSame(0, $database->getDocument(self::COLLECTION, 'counter')->getAttribute('big'));
    }

    public function testAWholeFloatLimitHoldsABigIntegerAtTheSignedEdge(): void
    {
        $database = $this->database();
        $database->updateDocument(self::COLLECTION, 'counter', new Document(['big' => PHP_INT_MAX - 5]));

        $updated = $database->updateDocument(self::COLLECTION, 'counter', new Document(['big' => Operator::increment(10, 9.0e18)]));

        $this->assertSame(PHP_INT_MAX - 5, $updated->getAttribute('big'));
        $this->assertSame(PHP_INT_MAX - 5, $database->getDocument(self::COLLECTION, 'counter')->getAttribute('big'));
    }

    private function database(): Database
    {
        $database = new Database(new Memory(), new Cache(new None()));
        $database
            ->setDatabase('fractional_limits')
            ->setNamespace('fractional_limits_'.\uniqid());
        $database->getAuthorization()->addRole(Role::any()->toString());
        $database->create();
        $database->createCollection(Collection::create(
            id: self::COLLECTION,
            attributes: [
                Attribute::integer(key: 'count'),
                Attribute::bigInteger(key: 'big'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
        ));
        $database->createDocument(self::COLLECTION, new Document(['$id' => 'counter', 'count' => 100, 'big' => 0]));

        return $database;
    }
}
