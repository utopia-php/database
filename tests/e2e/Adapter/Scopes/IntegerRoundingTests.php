<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Limit as LimitException;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Operator;
use Utopia\Database\Permission;
use Utopia\Database\Role;

trait IntegerRoundingTests
{
    public function testIntegerRoundingOfCounterAmounts(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $collection = $this->roundingCollection($database, 'rounding_amounts');

        try {
            $this->assertSame(102, $database->increaseDocumentAttribute($collection, 'counter', 'count', 1.5)->getAttribute('count'));
            $this->assertSame(105, $database->increaseDocumentAttribute($collection, 'counter', 'count', '2.5')->getAttribute('count'));
            $this->assertSame(102, $database->decreaseDocumentAttribute($collection, 'counter', 'count', 2.5)->getAttribute('count'));
            $this->assertSame(102, $database->increaseDocumentAttribute($collection, 'counter', 'count', 0.4)->getAttribute('count'));
            $this->assertSame(102, $database->decreaseDocumentAttribute($collection, 'counter', 'count', '0.49')->getAttribute('count'));
            $this->assertSame(102, $database->getDocument($collection, 'counter')->getAttribute('count'));

            $this->assertSame(9007199254740994, $database->increaseDocumentAttribute($collection, 'counter', 'big', '9007199254740993.5')->getAttribute('big'));
            $this->assertSame(0, $database->decreaseDocumentAttribute($collection, 'counter', 'big', '9007199254740993.5')->getAttribute('big'));
            $this->assertSame(0, $database->getDocument($collection, 'counter')->getAttribute('big'));

            $this->assertSame(2.75, $database->increaseDocumentAttribute($collection, 'counter', 'ratio', 1.25)->getAttribute('ratio'));
            $this->assertSame(2.75, $database->getDocument($collection, 'counter')->getAttribute('ratio'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testIntegerRoundingOfCounterBounds(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $collection = $this->roundingCollection($database, 'rounding_bounds');

        try {
            $this->assertSame(101, $database->increaseDocumentAttribute($collection, 'counter', 'count', 1, 101.9)->getAttribute('count'));
            $this->assertRoundingLimited('Attribute value exceeds maximum limit: 101', fn () => $database->increaseDocumentAttribute($collection, 'counter', 'count', 1, '101.9'));
            $this->assertSame(100, $database->decreaseDocumentAttribute($collection, 'counter', 'count', 1, 99.1)->getAttribute('count'));
            $this->assertRoundingLimited('Attribute value exceeds minimum limit: 100', fn () => $database->decreaseDocumentAttribute($collection, 'counter', 'count', 1, '99.1'));
            $this->assertSame(100, $database->getDocument($collection, 'counter')->getAttribute('count'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testIntegerRoundingOfOperatorsInUpdates(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Operators)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->roundingCollection($database, 'rounding_operators');

        try {
            $update = fn (array $changes): Document => $database->updateDocument($collection, 'counter', new Document($changes));

            $this->assertSame(102, $update(['count' => Operator::increment(1.5)])->getAttribute('count'));
            $this->assertSame(99, $update(['count' => Operator::decrement('2.5')])->getAttribute('count'));
            $this->assertSame(96, $update(['count' => Operator::increment(-2.5)])->getAttribute('count'));
            $this->assertSame(98, $update(['count' => Operator::increment(2, 98.7)])->getAttribute('count'));
            $this->assertSame(98, $update(['count' => Operator::increment(1, 98.7)])->getAttribute('count'));
            $this->assertSame(98, $update(['count' => Operator::decrement(4, '94.2')])->getAttribute('count'));
            $this->assertSame(95, $update(['count' => Operator::decrement(3, '94.2')])->getAttribute('count'));
            $this->assertSame(9007199254740994, $update(['big' => Operator::increment('9007199254740993.5')])->getAttribute('big'));
            $this->assertSame(2.0, $update(['ratio' => Operator::increment(0.5)])->getAttribute('ratio'));

            $database->createDocument($collection, new Document([Document::ID => 'second', 'count' => 10, 'big' => 0, 'ratio' => 0.0]));
            $this->assertSame(2, $database->updateDocuments($collection, new Document(['count' => Operator::increment(2.5)])));
            $this->assertSame(98, $database->getDocument($collection, 'counter')->getAttribute('count'));
            $this->assertSame(13, $database->getDocument($collection, 'second')->getAttribute('count'));

            $database->updateDocuments($collection, new Document(['count' => Operator::increment(7, 20.9)]));
            $this->assertSame(98, $database->getDocument($collection, 'counter')->getAttribute('count'));
            $this->assertSame(20, $database->getDocument($collection, 'second')->getAttribute('count'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testIntegerRoundingOfOperatorsInUpserts(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Operators) || ! $database->getAdapter()->hasFeature(Feature\Upserts::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->roundingCollection($database, 'rounding_upserts');

        try {
            $database->createDocument($collection, new Document([Document::ID => 'second', 'count' => 10, 'big' => 0, 'ratio' => 0.0]));

            $upserted = $database->upsertDocument($collection, new Document([Document::ID => 'counter', 'count' => Operator::increment(1.5)]));
            $count = $database->upsertDocuments($collection, [
                new Document([Document::ID => 'counter', 'count' => Operator::decrement(2.5, 98.5)]),
                new Document([Document::ID => 'second', 'count' => Operator::increment('1.5', '12.7')]),
            ]);

            $this->assertSame(102, $upserted->getAttribute('count'));
            $this->assertSame(2, $count);
            $this->assertSame(99, $database->getDocument($collection, 'counter')->getAttribute('count'));
            $this->assertSame(12, $database->getDocument($collection, 'second')->getAttribute('count'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testIntegerRoundingLeavesAFractionalMultiplyOrDivideRefused(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Operators)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->roundingCollection($database, 'rounding_refused');

        try {
            foreach (['multiply' => Operator::multiply(1.5), 'divide' => Operator::divide(2.5)] as $method => $operator) {
                try {
                    $database->updateDocument($collection, 'counter', new Document(['count' => $operator]));
                    $this->fail("A fractional {$method} on an integer attribute was accepted");
                } catch (StructureException $error) {
                    $this->assertSame("Invalid document structure: Cannot apply {$method} operator: value must be numeric, got double", $error->getMessage());
                }
            }

            $this->assertSame(100, $database->getDocument($collection, 'counter')->getAttribute('count'));
            $this->assertSame(0.75, $database->updateDocument($collection, 'counter', new Document(['ratio' => Operator::multiply(0.5)]))->getAttribute('ratio'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    private function roundingCollection(Database $database, string $id): string
    {
        $id .= '_'.\uniqid();
        $database->createCollection(Collection::create(
            id: $id,
            attributes: [
                Attribute::integer(key: 'count'),
                Attribute::bigInteger(key: 'big'),
                Attribute::double(key: 'ratio'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));
        $database->createDocument($id, new Document([Document::ID => 'counter', 'count' => 100, 'big' => 0, 'ratio' => 1.5]));

        return $id;
    }

    private function assertRoundingLimited(string $message, callable $change): void
    {
        try {
            $change();
            $this->fail("The change was not limited: {$message}");
        } catch (LimitException $error) {
            $this->assertSame($message, $error->getMessage());
        }
    }
}
