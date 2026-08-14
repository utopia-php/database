<?php

namespace Tests\Unit;

use PDOException;
use PHPUnit\Framework\TestCase;
use ReflectionMethod;
use ReflectionProperty;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQL;
use Utopia\Database\Attribute;
use Utopia\Database\Document;
use Utopia\Query\Schema\ColumnType;

final class PostgresSpatialCacheTest extends TestCase
{
    public function testRenameInvalidatesSpatialCacheWhenTypeAlterFails(): void
    {
        $rename = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $rename->expects($this->once())
            ->method('execute')
            ->willReturn(true);

        $alter = $this->getMockBuilder(\PDOStatement::class)
            ->disableOriginalConstructor()
            ->getMock();
        $alter->expects($this->once())
            ->method('execute')
            ->willThrowException(new PDOException('Unable to alter spatial column type'));

        $pdo = $this->getMockBuilder(\PDO::class)
            ->disableOriginalConstructor()
            ->getMock();
        $pdo->expects($this->exactly(2))
            ->method('prepare')
            ->willReturnOnConsecutiveCalls($rename, $alter);

        $adapter = new Postgres($pdo);
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');

        $collection = new Document([
            '$id' => 'locations',
            'attributes' => [new Document(['$id' => 'position', 'type' => ColumnType::Point->value])],
        ]);
        $getSpatialAttributes = new ReflectionMethod($adapter, 'getSpatialAttributes');
        $this->assertSame(['position'], $getSpatialAttributes->invoke($adapter, $collection));

        try {
            $adapter->updateAttribute(
                'locations',
                new Attribute(key: 'position', type: ColumnType::Point),
                'coordinates'
            );
            $this->fail('Expected the type alteration to fail.');
        } catch (PDOException $exception) {
            $this->assertSame('Unable to alter spatial column type', $exception->getMessage());
        }

        $cache = new ReflectionProperty(SQL::class, 'spatialAttributesCache');
        $this->assertSame([], $cache->getValue($adapter));
    }

    public function testSpatialCacheRescansWhenAttributeSetChanges(): void
    {
        $adapter = new Postgres($this->createStub(\PDO::class));
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $getSpatialAttributes = new ReflectionMethod($adapter, 'getSpatialAttributes');

        $before = new Document([
            '$id' => 'places',
            'attributes' => [new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value])],
        ]);
        $this->assertSame([], $getSpatialAttributes->invoke($adapter, $before));

        $after = new Document([
            '$id' => 'places',
            'attributes' => [
                new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value]),
                new Document(['$id' => 'loc', 'key' => 'loc', 'type' => ColumnType::Point->value]),
            ],
        ]);
        $this->assertSame(['loc'], $getSpatialAttributes->invoke($adapter, $after));
    }

    public function testSpatialAttributesFromTypedObjectsAndEnums(): void
    {
        $adapter = new Postgres($this->createStub(\PDO::class));
        $adapter->setDatabase('database');
        $adapter->setNamespace('namespace');
        $getSpatialAttributes = new ReflectionMethod($adapter, 'getSpatialAttributes');

        $collection = new Document([
            '$id' => 'mixed',
            'attributes' => [
                new Attribute(key: 'loc', type: ColumnType::Point),
                new Document(['$id' => 'route', 'key' => 'route', 'type' => ColumnType::Linestring]),
                ['$id' => 'area', 'key' => 'area', 'type' => ColumnType::Polygon->value],
                new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value]),
            ],
        ]);

        $this->assertSame(['loc', 'route', 'area'], $getSpatialAttributes->invoke($adapter, $collection));
    }

    public function testSpatialWriteValueDetection(): void
    {
        $adapter = new Postgres($this->createStub(\PDO::class));
        $isSpatialWriteValue = new ReflectionMethod($adapter, 'isSpatialWriteValue');
        $encodeSpatialWriteValue = new ReflectionMethod($adapter, 'encodeSpatialWriteValue');

        $this->assertTrue($isSpatialWriteValue->invoke($adapter, 'POINT(0 0)'));
        $this->assertTrue($isSpatialWriteValue->invoke($adapter, [0.0, 0.0]));
        $this->assertTrue($isSpatialWriteValue->invoke($adapter, [[0.0, 0.0], [1.0, 1.0]]));
        $this->assertFalse($isSpatialWriteValue->invoke($adapter, 'description'));
        $this->assertFalse($isSpatialWriteValue->invoke($adapter, ['foo' => 'bar']));

        $this->assertSame('POINT(0 0)', $encodeSpatialWriteValue->invoke($adapter, [0.0, 0.0]));
        $this->assertSame('POINT(0 0)', $encodeSpatialWriteValue->invoke($adapter, 'POINT(0 0)'));
    }

    public function testAttributeWidthAcceptsDocumentAndTypedAttributes(): void
    {
        $adapter = new Postgres($this->createStub(\PDO::class));
        $collection = new Document([
            '$id' => 'export',
            'attributes' => [
                new Document(['$id' => 'name', 'key' => 'name', 'type' => ColumnType::String->value, 'size' => 255, 'array' => false]),
                new Document(['$id' => 'body', 'key' => 'body', 'type' => ColumnType::MediumText->value, 'size' => 0, 'array' => false]),
                new Document(['$id' => 'notes', 'key' => 'notes', 'type' => ColumnType::LongText->value, 'size' => 0, 'array' => false]),
                new Document(['$id' => 'count', 'key' => 'count', 'type' => ColumnType::BigInteger, 'size' => 0, 'array' => false]),
                new Attribute(key: 'loc', type: ColumnType::Point),
            ],
        ]);

        $this->assertGreaterThan(0, $adapter->getAttributeWidth($collection));
    }
}
