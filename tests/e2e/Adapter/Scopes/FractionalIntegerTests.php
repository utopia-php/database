<?php

namespace Tests\E2E\Adapter\Scopes;

use Tests\Unit\Support\FractionalEngine;
use Tests\Unit\Support\FractionalResults;
use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Structure as StructureException;
use Utopia\Database\Exception\Type as TypeException;
use Utopia\Database\Operator;
use Utopia\Database\Permission;
use Utopia\Database\Role;

trait FractionalIntegerTests
{
    public function testFractionalOperatorsStoreWhatTheEngineStored(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Operators)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->fractionalCollection($database, 'fractional_operators');
        $engine = static::fractionalEngine();
        $row = 0;

        try {
            $database->withRoundFractions(true, function () use ($database, $collection, $engine, &$row): void {
                foreach (FractionalResults::operators() as $name => [$family, $stored, $method, $values, $expected]) {
                    if ($family !== $engine) {
                        continue;
                    }

                    $id = 'operator'.$row++;
                    $database->createDocument($collection, new Document([Document::ID => $id, 'count' => $stored]));
                    $update = fn (): Document => $database->updateDocument($collection, $id, new Document(['count' => new Operator($method, 'count', $values)]));

                    if ($expected === null) {
                        try {
                            $update();
                            $this->fail("{$name}: the engine refused it, so it must be refused");
                        } catch (StructureException) {
                        }

                        $this->assertSame($stored, $database->getDocument($collection, $id)->getAttribute('count'), $name);

                        continue;
                    }

                    $this->assertSame($expected, $update()->getAttribute('count'), $name);
                    $this->assertSame($expected, $database->getDocument($collection, $id)->getAttribute('count'), $name);
                }
            });

            $this->assertSame(68, $row, 'Every operator row of the engine family ran');
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testFractionalCountersStoreWhatTheEngineStored(): void
    {
        $database = $this->getDatabase();
        $collection = $this->fractionalCollection($database, 'fractional_counters');
        $engine = static::fractionalEngine();
        $row = 0;

        try {
            $database->withRoundFractions(true, function () use ($database, $collection, $engine, &$row): void {
                foreach (FractionalResults::counters() as $name => [$family, $increase, $start, $value, $bound, $stored]) {
                    if ($family !== $engine) {
                        continue;
                    }

                    $id = 'counter'.$row++;
                    $database->createDocument($collection, new Document([Document::ID => $id, 'big' => $start]));
                    $change = fn (): Document => $increase
                        ? $database->increaseDocumentAttribute($collection, $id, 'big', $value, $bound)
                        : $database->decreaseDocumentAttribute($collection, $id, 'big', $value, $bound);

                    if ($stored === null) {
                        try {
                            $change();
                            $this->fail("{$name}: the engine refused it, so it must be refused");
                        } catch (TypeException $exception) {
                            $this->assertSame('Change value must be an integer.', $exception->getMessage(), $name);
                        }

                        $this->assertSame($start, $database->getDocument($collection, $id)->getAttribute('big'), $name);

                        continue;
                    }

                    $this->assertSame($increase ? $start + $value : $start - $value, $change()->getAttribute('big'), $name);
                    $this->assertSame($stored, $database->getDocument($collection, $id)->getAttribute('big'), $name);
                }
            });

            $this->assertSame(21, $row, 'Every counter row of the engine family ran');
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testFractionalOperatorOnABigIntegerBeyondFloatPrecisionStaysExact(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Operators)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->fractionalCollection($database, 'fractional_big');
        $expected = match (static::fractionalEngine()) {
            FractionalEngine::MariaDB, FractionalEngine::MongoDB, FractionalEngine::Memory => 9007199254740994,
            FractionalEngine::PostgreSQL => 9007199254740995,
        };

        try {
            $database->createDocument($collection, new Document([Document::ID => 'huge', 'big' => 9007199254740993]));

            $database->withRoundFractions(true, fn (): Document => $database->updateDocument($collection, 'huge', new Document([
                'big' => Operator::increment(1.5, 9007199254741000),
            ])));

            $this->assertSame($expected, $database->getDocument($collection, 'huge')->getAttribute('big'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testFractionalBulkUpdateRoundsEachDocumentFromItsOwnStoredValue(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Operators)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->fractionalCollection($database, 'fractional_bulk');
        $expected = match (static::fractionalEngine()) {
            FractionalEngine::MariaDB => ['first' => 12, 'second' => 12, 'third' => 14],
            FractionalEngine::PostgreSQL => ['first' => 12, 'second' => 13, 'third' => 14],
            FractionalEngine::MongoDB, FractionalEngine::Memory => ['first' => 11, 'second' => 12, 'third' => 13],
        };

        try {
            foreach (['first' => 10, 'second' => 11, 'third' => 12] as $id => $count) {
                $database->createDocument($collection, new Document([Document::ID => $id, 'count' => $count]));
            }

            $updated = $database->withRoundFractions(true, fn (): int => $database->updateDocuments(
                $collection,
                new Document(['count' => Operator::increment(1.5, 100)]),
            ));

            $stored = [];
            foreach (\array_keys($expected) as $id) {
                $stored[$id] = $database->getDocument($collection, $id)->getAttribute('count');
            }

            $this->assertSame(3, $updated);
            $this->assertSame($expected, $stored);
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testFractionalUpsertRoundsAStoredDocumentFromItsValueAndANewOneFromZero(): void
    {
        $database = $this->getDatabase();

        if (! $database->getAdapter()->supports(Capability::Operators) || ! $database->getAdapter()->hasFeature(Feature\Upserts::class)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = $this->fractionalCollection($database, 'fractional_upsert');
        $expected = match (static::fractionalEngine()) {
            FractionalEngine::MariaDB, FractionalEngine::MongoDB, FractionalEngine::Memory => ['first' => 12, 'second' => 2],
            FractionalEngine::PostgreSQL => ['first' => 13, 'second' => 3],
        };

        try {
            $database->createDocument($collection, new Document([Document::ID => 'first', 'count' => 10]));

            $database->withRoundFractions(true, fn (): int => $database->upsertDocuments($collection, [
                new Document([Document::ID => 'first', 'count' => Operator::increment(2.5, 100)]),
                new Document([Document::ID => 'second', 'count' => Operator::increment(2.5, 100)]),
            ]));

            $this->assertSame($expected['first'], $database->getDocument($collection, 'first')->getAttribute('count'));
            $this->assertSame($expected['second'], $database->getDocument($collection, 'second')->getAttribute('count'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    public function testFractionsOnIntegersAreRefusedWithoutTheMode(): void
    {
        $database = $this->getDatabase();
        $collection = $this->fractionalCollection($database, 'fractional_refused');

        try {
            $database->createDocument($collection, new Document([Document::ID => 'meter', 'count' => 10]));

            try {
                $database->increaseDocumentAttribute($collection, 'meter', 'count', 1.5);
                $this->fail('A fractional change on an integer must be refused without the mode');
            } catch (TypeException $exception) {
                $this->assertSame('Change value must be an integer.', $exception->getMessage());
            }

            if ($database->getAdapter()->supports(Capability::Operators)) {
                try {
                    $database->updateDocument($collection, 'meter', new Document(['count' => Operator::increment(1.5, 100)]));
                    $this->fail('A fractional operator on an integer must be refused without the mode');
                } catch (StructureException) {
                }
            }

            $this->assertFalse($database->isRoundingFractions());
            $this->assertSame(10, $database->getDocument($collection, 'meter')->getAttribute('count'));
        } finally {
            $database->deleteCollection($collection);
        }
    }

    private function fractionalCollection(Database $database, string $id): string
    {
        $database->createCollection(Collection::create(
            id: $id,
            attributes: [
                Attribute::integer(key: 'count'),
                Attribute::bigInteger(key: 'big'),
            ],
            permissions: [
                Permission::create(Role::any()),
                Permission::read(Role::any()),
                Permission::update(Role::any()),
            ],
            documentSecurity: false,
        ));

        return $id;
    }
}
