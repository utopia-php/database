<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Database\Operator;
use Utopia\Database\Validator\Authorization;
use Utopia\Query\Schema\ColumnType;

final class SQLiteOperatorBehaviorTest extends TestCase
{
    private SQLite $adapter;

    private Document $collection;

    protected function setUp(): void
    {
        $this->adapter = new SQLite(new \PDO('sqlite::memory:'));
        $this->adapter->setNamespace('namespace');
        $authorization = new Authorization();
        $authorization->disable();
        $this->adapter->setAuthorization($authorization);

        $this->collection = new Document([
            '$id' => 'operators',
            'attributes' => [new Document(['$id' => 'value', 'type' => ColumnType::Double->value])],
        ]);
        $this->adapter->createCollection('operators', [
            Attribute::double(key: 'value'),
        ]);
    }

    public function testInclusiveIncrementBoundAppliesExactMaximum(): void
    {
        $this->create('increment', 5.0);

        $this->update('increment', Operator::increment(5, 10));

        $this->assertSame(10.0, $this->value('increment'));
    }

    public function testPowerAboveMaximumLeavesValueUnchanged(): void
    {
        $this->create('power', 5.0);

        $this->update('power', Operator::power(3, 100));

        $this->assertSame(5.0, $this->value('power'));
    }

    public function testFractionalPowerBelowMaximumIsApplied(): void
    {
        $this->create('root', 100.0);

        $this->update('root', Operator::power(0.5, 50));

        $this->assertSame(10.0, $this->value('root'));
    }

    private function create(string $id, float $value): void
    {
        $this->adapter->createDocument($this->collection, new Document([
            '$id' => $id,
            '$permissions' => [],
            'value' => $value,
        ]));
    }

    private function update(string $id, Operator $operator): void
    {
        $this->adapter->updateDocument($this->collection, $id, new Document([
            '$id' => $id,
            'value' => $operator,
        ]), true);
    }

    private function value(string $id): float
    {
        $value = $this->adapter->getDocument($this->collection, $id)->getAttribute('value');
        $this->assertIsNumeric($value);

        return (float) $value;
    }
}
