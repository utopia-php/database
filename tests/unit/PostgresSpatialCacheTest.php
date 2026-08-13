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
}
