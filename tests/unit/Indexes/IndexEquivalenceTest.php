<?php

namespace Tests\Unit\Indexes;

use PHPUnit\Framework\TestCase;
use Utopia\Database\Index;
use Utopia\Query\Schema\Order;

final class IndexEquivalenceTest extends TestCase
{
    public function testTheKeyLengthsAndOrdersDoNotMatter(): void
    {
        $index = Index::key(key: 'byName', attributes: ['name', 'age'], lengths: [16, null], orders: [Order::Asc, Order::Desc]);

        $this->assertTrue($index->isEquivalentTo(Index::key(key: 'other', attributes: ['NAME', 'age'])));
    }

    public function testTheTypeAndAttributesInOrderDo(): void
    {
        $index = Index::key(key: 'byName', attributes: ['name', 'age']);

        $this->assertFalse($index->isEquivalentTo(Index::unique(key: 'byName', attributes: ['name', 'age'])));
        $this->assertFalse($index->isEquivalentTo(Index::key(key: 'byName', attributes: ['age', 'name'])));
        $this->assertFalse($index->isEquivalentTo(Index::key(key: 'byName', attributes: ['name'])));
    }

    public function testATimeToLiveMatters(): void
    {
        $index = Index::ttl(key: 'expiry', attributes: ['expiresAt'], ttl: 60);

        $this->assertTrue($index->isEquivalentTo(Index::ttl(key: 'expiry', attributes: ['expiresAt'], ttl: 60)));
        $this->assertFalse($index->isEquivalentTo(Index::ttl(key: 'expiry', attributes: ['expiresAt'], ttl: 120)));
    }

    public function testFindByKeyReadsTheMetadataList(): void
    {
        $indexes = '[{"$id":"byName","key":"byName","type":"key","attributes":["name"],"lengths":[],"orders":[],"ttl":1}]';

        $this->assertSame(['name'], Index::findByKey($indexes, 'BYNAME')?->attributes);
        $this->assertNull(Index::findByKey($indexes, 'byAge'));
        $this->assertNull(Index::findByKey('not json', 'byName'));
    }
}
