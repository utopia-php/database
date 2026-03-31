<?php

namespace Tests\E2E\Adapter\Scopes;

use PHPUnit\Framework\Attributes\DataProvider;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

trait JoinTests
{
    public function testLeftJoinNoMatchesReturnsAllMainRows(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $pCol = 'ljnm_p';
        $rCol = 'ljnm_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($pCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($rCol, new Attribute(key: 'prod_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($rCol, new Attribute(key: 'score', type: ColumnType::Integer, size: 0, required: true));

        foreach (['Alpha', 'Beta', 'Gamma'] as $name) {
            $database->createDocument($pCol, new Document([
                '$id' => strtolower($name),
                'name' => $name,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($pCol, [
            Query::leftJoin($rCol, '$id', 'prod_uid'),
            Query::count('*', 'cnt'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(3, $results);
        foreach ($results as $doc) {
            $this->assertEquals(1, $doc->getAttribute('cnt'));
        }

        $this->cleanupAggCollections($database, $cols);
    }

    public function testLeftJoinPartialMatches(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $pCol = 'ljpm_p';
        $rCol = 'ljpm_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($pCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($rCol, new Attribute(key: 'prod_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($rCol, new Attribute(key: 'score', type: ColumnType::Integer, size: 0, required: true));

        foreach (['p1', 'p2', 'p3'] as $id) {
            $database->createDocument($pCol, new Document([
                '$id' => $id,
                'name' => 'Product ' . $id,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $reviews = [
            ['prod_uid' => 'p1', 'score' => 5],
            ['prod_uid' => 'p1', 'score' => 3],
            ['prod_uid' => 'p1', 'score' => 4],
            ['prod_uid' => 'p2', 'score' => 2],
            ['prod_uid' => 'p2', 'score' => 4],
        ];
        foreach ($reviews as $r) {
            $database->createDocument($rCol, new Document(array_merge($r, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($pCol, [
            Query::leftJoin($rCol, '$id', 'prod_uid'),
            Query::count('*', 'cnt'),
            Query::avg('score', 'avg_score'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(3, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('name')] = $doc;
        }
        $this->assertEquals(3, $mapped['Product p1']->getAttribute('cnt'));
        $this->assertEqualsWithDelta(4.0, (float) $mapped['Product p1']->getAttribute('avg_score'), 0.1);
        $this->assertEquals(2, $mapped['Product p2']->getAttribute('cnt'));
        $this->assertEqualsWithDelta(3.0, (float) $mapped['Product p2']->getAttribute('avg_score'), 0.1);
        $this->assertEquals(1, $mapped['Product p3']->getAttribute('cnt'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinMultipleAggregationAliases(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jma_o';
        $cCol = 'jma_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        foreach ([100, 200, 300, 400, 500] as $amt) {
            $database->createDocument($oCol, new Document([
                'cust_uid' => 'c1', 'amount' => $amt,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'order_count'),
            Query::sum('amount', 'total_amount'),
            Query::avg('amount', 'avg_amount'),
            Query::min('amount', 'min_amount'),
            Query::max('amount', 'max_amount'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(5, $results[0]->getAttribute('order_count'));
        $this->assertEquals(1500, $results[0]->getAttribute('total_amount'));
        $this->assertEqualsWithDelta(300.0, (float) $results[0]->getAttribute('avg_amount'), 0.1);
        $this->assertEquals(100, $results[0]->getAttribute('min_amount'));
        $this->assertEquals(500, $results[0]->getAttribute('max_amount'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinMultipleGroupByColumns(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jmg_o';
        $cCol = 'jmg_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 100],
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 200],
            ['cust_uid' => 'c1', 'status' => 'pending', 'amount' => 50],
            ['cust_uid' => 'c2', 'status' => 'done', 'amount' => 300],
            ['cust_uid' => 'c2', 'status' => 'pending', 'amount' => 75],
            ['cust_uid' => 'c2', 'status' => 'pending', 'amount' => 25],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid', 'status']),
        ]);

        $this->assertCount(4, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $key = $doc->getAttribute('cust_uid') . '_' . $doc->getAttribute('status');
            $mapped[$key] = $doc;
        }
        $this->assertEquals(2, $mapped['c1_done']->getAttribute('cnt'));
        $this->assertEquals(300, $mapped['c1_done']->getAttribute('total'));
        $this->assertEquals(1, $mapped['c1_pending']->getAttribute('cnt'));
        $this->assertEquals(50, $mapped['c1_pending']->getAttribute('total'));
        $this->assertEquals(1, $mapped['c2_done']->getAttribute('cnt'));
        $this->assertEquals(300, $mapped['c2_done']->getAttribute('total'));
        $this->assertEquals(2, $mapped['c2_pending']->getAttribute('cnt'));
        $this->assertEquals(100, $mapped['c2_pending']->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinWithHavingOnCount(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jhc_o';
        $cCol = 'jhc_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c2', 'amount' => 20],
            ['cust_uid' => 'c2', 'amount' => 30],
            ['cust_uid' => 'c3', 'amount' => 40],
            ['cust_uid' => 'c3', 'amount' => 50],
            ['cust_uid' => 'c3', 'amount' => 60],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('cnt', 1)]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d->getAttribute('cust_uid'), $results);
        $this->assertContains('c2', $ids);
        $this->assertContains('c3', $ids);
        $this->assertNotContains('c1', $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinWithHavingOnAvg(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jha_o';
        $cCol = 'jha_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c1', 'amount' => 20],
            ['cust_uid' => 'c2', 'amount' => 500],
            ['cust_uid' => 'c2', 'amount' => 600],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::avg('amount', 'avg_amt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('avg_amt', 100)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c2', $results[0]->getAttribute('cust_uid'));
        $this->assertEqualsWithDelta(550.0, (float) $results[0]->getAttribute('avg_amt'), 0.1);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinWithHavingOnSum(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jhs_o';
        $cCol = 'jhs_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 50],
            ['cust_uid' => 'c2', 'amount' => 300],
            ['cust_uid' => 'c2', 'amount' => 400],
            ['cust_uid' => 'c3', 'amount' => 100],
            ['cust_uid' => 'c3', 'amount' => 100],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('total', 250)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c2', $results[0]->getAttribute('cust_uid'));
        $this->assertEquals(700, $results[0]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinWithHavingBetween(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jhb_o';
        $cCol = 'jhb_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c2', 'amount' => 100],
            ['cust_uid' => 'c2', 'amount' => 200],
            ['cust_uid' => 'c3', 'amount' => 500],
            ['cust_uid' => 'c3', 'amount' => 600],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::between('total', 100, 500)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c2', $results[0]->getAttribute('cust_uid'));
        $this->assertEquals(300, $results[0]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinCountDistinct(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jcd_o';
        $cCol = 'jcd_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'product', type: ColumnType::String, size: 50, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'product' => 'A'],
            ['cust_uid' => 'c1', 'product' => 'A'],
            ['cust_uid' => 'c1', 'product' => 'B'],
            ['cust_uid' => 'c2', 'product' => 'C'],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::countDistinct('product', 'uniq_prod'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(3, $results[0]->getAttribute('uniq_prod'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinMinMax(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jmm_o';
        $cCol = 'jmm_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c1', 'amount' => 50],
            ['cust_uid' => 'c1', 'amount' => 30],
            ['cust_uid' => 'c2', 'amount' => 200],
            ['cust_uid' => 'c2', 'amount' => 100],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::min('amount', 'min_amt'),
            Query::max('amount', 'max_amt'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('cust_uid')] = $doc;
        }
        $this->assertEquals(10, $mapped['c1']->getAttribute('min_amt'));
        $this->assertEquals(50, $mapped['c1']->getAttribute('max_amt'));
        $this->assertEquals(100, $mapped['c2']->getAttribute('min_amt'));
        $this->assertEquals(200, $mapped['c2']->getAttribute('max_amt'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinFilterOnMainTable(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jfm_o';
        $cCol = 'jfm_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 100],
            ['cust_uid' => 'c1', 'status' => 'open', 'amount' => 200],
            ['cust_uid' => 'c2', 'status' => 'done', 'amount' => 300],
            ['cust_uid' => 'c2', 'status' => 'done', 'amount' => 400],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::equal('status', ['done']),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('cust_uid')] = $doc;
        }
        $this->assertEquals(1, $mapped['c1']->getAttribute('cnt'));
        $this->assertEquals(100, $mapped['c1']->getAttribute('total'));
        $this->assertEquals(2, $mapped['c2']->getAttribute('cnt'));
        $this->assertEquals(700, $mapped['c2']->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinBetweenFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jbf_o';
        $cCol = 'jbf_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        foreach ([50, 150, 250, 350, 450] as $amt) {
            $database->createDocument($oCol, new Document([
                'cust_uid' => 'c1', 'amount' => $amt,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::between('amount', 100, 300),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(2, $results[0]->getAttribute('cnt'));
        $this->assertEquals(400, $results[0]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinGreaterLessThanFilters(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jgl_o';
        $cCol = 'jgl_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        foreach ([10, 20, 30, 40, 50] as $amt) {
            $database->createDocument($oCol, new Document([
                'cust_uid' => 'c1', 'amount' => $amt,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::greaterThan('amount', 15),
            Query::lessThanEqual('amount', 40),
            Query::count('*', 'cnt'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(3, $results[0]->getAttribute('cnt'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinEmptyResultSet(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jer_o';
        $cCol = 'jer_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $database->createDocument($oCol, new Document([
            'cust_uid' => 'nonexistent', 'amount' => 100,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(0, $results[0]->getAttribute('cnt'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinFilterYieldsNoResults(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jfnr_o';
        $cCol = 'jfnr_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            'cust_uid' => 'c1', 'status' => 'done', 'amount' => 100,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::equal('status', ['ghost']),
            Query::count('*', 'cnt'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(0, $results[0]->getAttribute('cnt'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testLeftJoinSumNullRightSide(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $pCol = 'ljsn_p';
        $oCol = 'ljsn_o';
        $cols = [$pCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($pCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'prod_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1', 'name' => 'WithOrders',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($pCol, new Document([
            '$id' => 'p2', 'name' => 'NoOrders',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $database->createDocument($oCol, new Document([
            'prod_uid' => 'p1', 'amount' => 100,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            'prod_uid' => 'p1', 'amount' => 200,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($pCol, [
            Query::leftJoin($oCol, '$id', 'prod_uid'),
            Query::sum('amount', 'total'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('name')] = $doc;
        }
        $this->assertEquals(300, $mapped['WithOrders']->getAttribute('total'));
        $noOrderTotal = $mapped['NoOrders']->getAttribute('total');
        $this->assertTrue($noOrderTotal === null || $noOrderTotal === 0 || $noOrderTotal === 0.0);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinMultipleFilterTypes(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jmft_o';
        $cCol = 'jmft_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 500],
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 600],
            ['cust_uid' => 'c1', 'status' => 'open', 'amount' => 100],
            ['cust_uid' => 'c2', 'status' => 'done', 'amount' => 50],
            ['cust_uid' => 'c3', 'status' => 'done', 'amount' => 800],
            ['cust_uid' => 'c3', 'status' => 'done', 'amount' => 900],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::equal('status', ['done']),
            Query::greaterThan('amount', 100),
            Query::sum('amount', 'total'),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('total', 500)]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d->getAttribute('cust_uid'), $results);
        $this->assertContains('c1', $ids);
        $this->assertContains('c3', $ids);
        $this->assertNotContains('c2', $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinLargeDataset(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jld_o';
        $cCol = 'jld_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        for ($i = 1; $i <= 10; $i++) {
            $cid = 'c' . $i;
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $i,
                '$permissions' => [Permission::read(Role::any())],
            ]));

            for ($j = 1; $j <= 10; $j++) {
                $database->createDocument($oCol, new Document([
                    'cust_uid' => $cid, 'amount' => $j * 10,
                    '$permissions' => [Permission::read(Role::any())],
                ]));
            }
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(10, $results);
        foreach ($results as $doc) {
            $this->assertEquals(10, $doc->getAttribute('cnt'));
            $this->assertEquals(550, $doc->getAttribute('total'));
        }

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinNotEqualFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jne_o';
        $cCol = 'jne_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $orders = [
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 100],
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 200],
            ['cust_uid' => 'c1', 'status' => 'cancel', 'amount' => 50],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::notEqual('status', 'cancel'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(2, $results[0]->getAttribute('cnt'));
        $this->assertEquals(300, $results[0]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinStartsWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jsw_o';
        $cCol = 'jsw_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'tag', type: ColumnType::String, size: 50, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $orders = [
            ['cust_uid' => 'c1', 'tag' => 'promo_spring', 'amount' => 100],
            ['cust_uid' => 'c1', 'tag' => 'promo_fall', 'amount' => 200],
            ['cust_uid' => 'c1', 'tag' => 'regular', 'amount' => 50],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::startsWith('tag', 'promo'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(2, $results[0]->getAttribute('cnt'));
        $this->assertEquals(300, $results[0]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinEqualMultipleValues(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jemv_o';
        $cCol = 'jemv_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 100],
            ['cust_uid' => 'c1', 'status' => 'open', 'amount' => 200],
            ['cust_uid' => 'c1', 'status' => 'cancel', 'amount' => 50],
            ['cust_uid' => 'c2', 'status' => 'done', 'amount' => 300],
            ['cust_uid' => 'c2', 'status' => 'cancel', 'amount' => 25],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::equal('status', ['done', 'open']),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('cust_uid')] = $doc;
        }
        $this->assertEquals(2, $mapped['c1']->getAttribute('cnt'));
        $this->assertEquals(300, $mapped['c1']->getAttribute('total'));
        $this->assertEquals(1, $mapped['c2']->getAttribute('cnt'));
        $this->assertEquals(300, $mapped['c2']->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinGroupByHavingLessThan(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jghl_o';
        $cCol = 'jghl_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c2', 'amount' => 500],
            ['cust_uid' => 'c2', 'amount' => 600],
            ['cust_uid' => 'c3', 'amount' => 20],
            ['cust_uid' => 'c3', 'amount' => 30],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::lessThan('total', 100)]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d->getAttribute('cust_uid'), $results);
        $this->assertContains('c1', $ids);
        $this->assertContains('c3', $ids);
        $this->assertNotContains('c2', $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testLeftJoinHavingCountZero(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $pCol = 'ljhz_p';
        $oCol = 'ljhz_o';
        $cols = [$pCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($pCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'prod_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['p1', 'p2', 'p3'] as $pid) {
            $database->createDocument($pCol, new Document([
                '$id' => $pid, 'name' => 'Product ' . $pid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $database->createDocument($oCol, new Document([
            'prod_uid' => 'p1', 'amount' => 100,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            'prod_uid' => 'p1', 'amount' => 200,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($pCol, [
            Query::leftJoin($oCol, '$id', 'prod_uid'),
            Query::count('*', 'cnt'),
            Query::groupBy(['name']),
            Query::having([Query::greaterThan('cnt', 1)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('Product p1', $results[0]->getAttribute('name'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinGroupByAllAggregations(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jgba_o';
        $cCol = 'jgba_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 100],
            ['cust_uid' => 'c1', 'amount' => 200],
            ['cust_uid' => 'c1', 'amount' => 300],
            ['cust_uid' => 'c2', 'amount' => 50],
            ['cust_uid' => 'c2', 'amount' => 150],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::avg('amount', 'avg_amt'),
            Query::min('amount', 'min_amt'),
            Query::max('amount', 'max_amt'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('cust_uid')] = $doc;
        }

        $this->assertEquals(3, $mapped['c1']->getAttribute('cnt'));
        $this->assertEquals(600, $mapped['c1']->getAttribute('total'));
        $this->assertEqualsWithDelta(200.0, (float) $mapped['c1']->getAttribute('avg_amt'), 0.1);
        $this->assertEquals(100, $mapped['c1']->getAttribute('min_amt'));
        $this->assertEquals(300, $mapped['c1']->getAttribute('max_amt'));

        $this->assertEquals(2, $mapped['c2']->getAttribute('cnt'));
        $this->assertEquals(200, $mapped['c2']->getAttribute('total'));
        $this->assertEqualsWithDelta(100.0, (float) $mapped['c2']->getAttribute('avg_amt'), 0.1);
        $this->assertEquals(50, $mapped['c2']->getAttribute('min_amt'));
        $this->assertEquals(150, $mapped['c2']->getAttribute('max_amt'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinSingleRowPerGroup(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jsr_o';
        $cCol = 'jsr_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        foreach (['c1', 'c2', 'c3'] as $i => $cid) {
            $database->createDocument($oCol, new Document([
                'cust_uid' => $cid, 'amount' => ($i + 1) * 100,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(3, $results);
        foreach ($results as $doc) {
            $this->assertEquals(1, $doc->getAttribute('cnt'));
        }

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('cust_uid')] = $doc;
        }
        $this->assertEquals(100, $mapped['c1']->getAttribute('total'));
        $this->assertEquals(200, $mapped['c2']->getAttribute('total'));
        $this->assertEquals(300, $mapped['c3']->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    /**
     * @return array<string, array{string, int}>
     */
    public static function joinTypeProvider(): array
    {
        return [
            'inner join' => ['join', 2],
            'left join' => ['leftJoin', 3],
        ];
    }

    #[DataProvider('joinTypeProvider')]
    public function testJoinTypeCountsCorrectly(string $joinMethod, int $expectedGroups): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $pCol = 'jtc_p';
        $oCol = 'jtc_o';
        $cols = [$pCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($pCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'prod_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'qty', type: ColumnType::Integer, size: 0, required: true));

        foreach (['p1', 'p2', 'p3'] as $pid) {
            $database->createDocument($pCol, new Document([
                '$id' => $pid, 'name' => 'Product ' . $pid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $database->createDocument($oCol, new Document([
            'prod_uid' => 'p1', 'qty' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            'prod_uid' => 'p2', 'qty' => 3,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $joinQuery = match ($joinMethod) {
            'join' => Query::join($oCol, '$id', 'prod_uid'),
            'leftJoin' => Query::leftJoin($oCol, '$id', 'prod_uid'),
        };

        $results = $database->find($pCol, [
            $joinQuery,
            Query::count('*', 'cnt'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount($expectedGroups, $results);

        $this->cleanupAggCollections($database, $cols);
    }

    /**
     * @return array<string, array{string, string, int|float}>
     */
    public static function joinAggregationTypeProvider(): array
    {
        return [
            'count' => ['count', '*', 10],
            'sum' => ['sum', 'amount', 5500],
            'avg' => ['avg', 'amount', 550.0],
            'min' => ['min', 'amount', 100],
            'max' => ['max', 'amount', 1000],
        ];
    }

    #[DataProvider('joinAggregationTypeProvider')]
    public function testJoinWithDifferentAggTypes(string $aggMethod, string $attribute, int|float $expected): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jat_o';
        $cCol = 'jat_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        for ($i = 1; $i <= 10; $i++) {
            $database->createDocument($oCol, new Document([
                'cust_uid' => 'c1', 'amount' => $i * 100,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $aggQuery = match ($aggMethod) {
            'count' => Query::count($attribute, 'result'),
            'sum' => Query::sum($attribute, 'result'),
            'avg' => Query::avg($attribute, 'result'),
            'min' => Query::min($attribute, 'result'),
            'max' => Query::max($attribute, 'result'),
        };

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            $aggQuery,
        ]);

        $this->assertCount(1, $results);
        if ($aggMethod === 'avg') {
            $this->assertEqualsWithDelta($expected, (float) $results[0]->getAttribute('result'), 0.1);
        } else {
            $this->assertEquals($expected, $results[0]->getAttribute('result'));
        }

        $this->cleanupAggCollections($database, $cols);
    }

    /**
     * @return array<string, array{string, string, int|float, int}>
     */
    public static function joinHavingOperatorProvider(): array
    {
        return [
            'gt 2' => ['greaterThan', 'cnt', 2, 2],
            'gte 3' => ['greaterThanEqual', 'cnt', 3, 2],
            'lt 4' => ['lessThan', 'cnt', 4, 2],
            'lte 3' => ['lessThanEqual', 'cnt', 3, 2],
        ];
    }

    #[DataProvider('joinHavingOperatorProvider')]
    public function testJoinHavingOperators(string $operator, string $alias, int|float $threshold, int $expectedGroups): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jho_o';
        $cCol = 'jho_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $database->createDocument($oCol, new Document([
            'cust_uid' => 'c1', 'amount' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        for ($i = 0; $i < 3; $i++) {
            $database->createDocument($oCol, new Document([
                'cust_uid' => 'c2', 'amount' => 20,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        for ($i = 0; $i < 5; $i++) {
            $database->createDocument($oCol, new Document([
                'cust_uid' => 'c3', 'amount' => 30,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $havingQuery = match ($operator) {
            'greaterThan' => Query::greaterThan($alias, $threshold),
            'greaterThanEqual' => Query::greaterThanEqual($alias, $threshold),
            'lessThan' => Query::lessThan($alias, $threshold),
            'lessThanEqual' => Query::lessThanEqual($alias, $threshold),
        };

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', $alias),
            Query::groupBy(['cust_uid']),
            Query::having([$havingQuery]),
        ]);

        $this->assertCount($expectedGroups, $results);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinOrderByAggregation(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'joa_o';
        $cCol = 'joa_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c2', 'amount' => 20],
            ['cust_uid' => 'c2', 'amount' => 30],
            ['cust_uid' => 'c2', 'amount' => 40],
            ['cust_uid' => 'c3', 'amount' => 50],
            ['cust_uid' => 'c3', 'amount' => 60],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('total'),
        ]);

        $this->assertCount(3, $results);
        $totals = array_map(fn ($d) => (int) $d->getAttribute('total'), $results);
        $this->assertEquals([110, 90, 10], $totals);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinWithLimit(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jwl_o';
        $cCol = 'jwl_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        for ($i = 1; $i <= 5; $i++) {
            $cid = 'c' . $i;
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $i,
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($oCol, new Document([
                'cust_uid' => $cid, 'amount' => $i * 100,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('total'),
            Query::limit(2),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals(500, (int) $results[0]->getAttribute('total'));
        $this->assertEquals(400, (int) $results[1]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinWithLimitAndOffset(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jlo_o';
        $cCol = 'jlo_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        for ($i = 1; $i <= 5; $i++) {
            $cid = 'c' . $i;
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $i,
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($oCol, new Document([
                'cust_uid' => $cid, 'amount' => $i * 100,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('total'),
            Query::limit(2),
            Query::offset(1),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals(400, (int) $results[0]->getAttribute('total'));
        $this->assertEquals(300, (int) $results[1]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinMultipleHavingConditions(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jmhc_o';
        $cCol = 'jmhc_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3', 'c4'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c2', 'amount' => 100],
            ['cust_uid' => 'c2', 'amount' => 200],
            ['cust_uid' => 'c3', 'amount' => 50],
            ['cust_uid' => 'c3', 'amount' => 50],
            ['cust_uid' => 'c3', 'amount' => 50],
            ['cust_uid' => 'c4', 'amount' => 500],
            ['cust_uid' => 'c4', 'amount' => 600],
            ['cust_uid' => 'c4', 'amount' => 700],
            ['cust_uid' => 'c4', 'amount' => 800],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        // HAVING count >= 2 AND sum > 200 → c2 (cnt=2, sum=300) and c4 (cnt=4, sum=2600)
        // c1 excluded (cnt=1), c3 excluded (cnt=3, sum=150 < 200)
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([
                Query::greaterThanEqual('cnt', 2),
                Query::greaterThan('total', 200),
            ]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d->getAttribute('cust_uid'), $results);
        $this->assertContains('c2', $ids);
        $this->assertContains('c4', $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinHavingWithEqual(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jhe_o';
        $cCol = 'jhe_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c2', 'amount' => 20],
            ['cust_uid' => 'c2', 'amount' => 30],
            ['cust_uid' => 'c3', 'amount' => 40],
            ['cust_uid' => 'c3', 'amount' => 50],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::equal('cnt', [2])]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d->getAttribute('cust_uid'), $results);
        $this->assertContains('c2', $ids);
        $this->assertContains('c3', $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinEmptyMainTable(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jem_o';
        $cCol = 'jem_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        // Main table (orders) is empty
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(0, $results[0]->getAttribute('cnt'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinOrderByGroupedColumn(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jogc_o';
        $cCol = 'jogc_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['alpha', 'beta', 'gamma'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => ucfirst($cid),
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($oCol, new Document([
                'cust_uid' => $cid, 'amount' => 100,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('cust_uid'),
        ]);

        $this->assertCount(3, $results);
        $custIds = array_map(fn ($d) => $d->getAttribute('cust_uid'), $results);
        $this->assertEquals(['gamma', 'beta', 'alpha'], $custIds);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testTwoTableJoinFromMainTable(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Main table: orders, referencing both customers and products
        $cCol = 'ttj_c';
        $pCol = 'ttj_p';
        $oCol = 'ttj_o';
        $cols = [$cCol, $pCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($pCol, new Attribute(key: 'title', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'prod_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Alice',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($cCol, new Document([
            '$id' => 'c2', 'name' => 'Bob',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1', 'title' => 'Widget',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($pCol, new Document([
            '$id' => 'p2', 'title' => 'Gadget',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $orders = [
            ['cust_uid' => 'c1', 'prod_uid' => 'p1', 'amount' => 100],
            ['cust_uid' => 'c1', 'prod_uid' => 'p1', 'amount' => 200],
            ['cust_uid' => 'c1', 'prod_uid' => 'p2', 'amount' => 300],
            ['cust_uid' => 'c2', 'prod_uid' => 'p1', 'amount' => 150],
            ['cust_uid' => 'c2', 'prod_uid' => 'p2', 'amount' => 250],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        // Join both customers and products from orders
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::join($pCol, 'prod_uid', '$id'),
            Query::count('*', 'order_cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('cust_uid')] = $doc;
        }
        $this->assertEquals(3, $mapped['c1']->getAttribute('order_cnt'));
        $this->assertEquals(600, (int) $mapped['c1']->getAttribute('total'));
        $this->assertEquals(2, $mapped['c2']->getAttribute('order_cnt'));
        $this->assertEquals(400, (int) $mapped['c2']->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinHavingNotBetween(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jhnb_o';
        $cCol = 'jhnb_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c2', 'amount' => 100],
            ['cust_uid' => 'c2', 'amount' => 200],
            ['cust_uid' => 'c3', 'amount' => 500],
            ['cust_uid' => 'c3', 'amount' => 600],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        // Sums: c1=10, c2=300, c3=1100
        // NOT BETWEEN 50 AND 500 → c1 (10) and c3 (1100)
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::notBetween('total', 50, 500)]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d->getAttribute('cust_uid'), $results);
        $this->assertContains('c1', $ids);
        $this->assertContains('c3', $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinWithFilterAndOrder(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jfo_o';
        $cCol = 'jfo_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 500],
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 100],
            ['cust_uid' => 'c2', 'status' => 'done', 'amount' => 900],
            ['cust_uid' => 'c3', 'status' => 'done', 'amount' => 200],
            ['cust_uid' => 'c3', 'status' => 'done', 'amount' => 300],
            ['cust_uid' => 'c3', 'status' => 'open', 'amount' => 10000],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        // Filter done only, group by customer, order by total ascending
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::equal('status', ['done']),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderAsc('total'),
        ]);

        $this->assertCount(3, $results);
        $totals = array_map(fn ($d) => (int) $d->getAttribute('total'), $results);
        $this->assertEquals([500, 600, 900], $totals);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinHavingNotEqual(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jhne_o';
        $cCol = 'jhne_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'amount' => 10],
            ['cust_uid' => 'c2', 'amount' => 20],
            ['cust_uid' => 'c2', 'amount' => 30],
            ['cust_uid' => 'c3', 'amount' => 40],
            ['cust_uid' => 'c3', 'amount' => 50],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        // Counts: c1=1, c2=2, c3=2. HAVING count != 2 → c1 only
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::notEqual('cnt', 2)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c1', $results[0]->getAttribute('cust_uid'));
        $this->assertEquals(1, $results[0]->getAttribute('cnt'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testLeftJoinAllUnmatched(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $pCol = 'ljau_p';
        $oCol = 'ljau_o';
        $cols = [$pCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($pCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'prod_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'qty', type: ColumnType::Integer, size: 0, required: true));

        foreach (['p1', 'p2'] as $pid) {
            $database->createDocument($pCol, new Document([
                '$id' => $pid, 'name' => 'Product ' . $pid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        // Orders reference non-existent products
        $database->createDocument($oCol, new Document([
            'prod_uid' => 'nonexistent', 'qty' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($pCol, [
            Query::leftJoin($oCol, '$id', 'prod_uid'),
            Query::count('*', 'cnt'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(2, $results);
        foreach ($results as $doc) {
            $this->assertEquals(1, $doc->getAttribute('cnt'));
        }

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinSameTableDifferentFilters(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jstdf_o';
        $cCol = 'jstdf_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'category', type: ColumnType::String, size: 50, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'category' => 'electronics', 'amount' => 500],
            ['cust_uid' => 'c1', 'category' => 'books', 'amount' => 20],
            ['cust_uid' => 'c1', 'category' => 'books', 'amount' => 30],
            ['cust_uid' => 'c2', 'category' => 'electronics', 'amount' => 1000],
            ['cust_uid' => 'c2', 'category' => 'electronics', 'amount' => 200],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        // Filter electronics only, group by customer
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::equal('category', ['electronics']),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('total'),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('c2', $results[0]->getAttribute('cust_uid'));
        $this->assertEquals(1200, (int) $results[0]->getAttribute('total'));
        $this->assertEquals('c1', $results[1]->getAttribute('cust_uid'));
        $this->assertEquals(500, (int) $results[1]->getAttribute('total'));

        // Now books only
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::equal('category', ['books']),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c1', $results[0]->getAttribute('cust_uid'));
        $this->assertEquals(50, (int) $results[0]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinGroupByMultipleColumnsWithHaving(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jgmh_o';
        $cCol = 'jgmh_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 100],
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 200],
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 300],
            ['cust_uid' => 'c1', 'status' => 'open', 'amount' => 50],
            ['cust_uid' => 'c2', 'status' => 'done', 'amount' => 400],
            ['cust_uid' => 'c2', 'status' => 'open', 'amount' => 25],
            ['cust_uid' => 'c2', 'status' => 'open', 'amount' => 75],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        // GROUP BY cust_uid, status with HAVING count >= 2
        // c1/done (3), c1/open (1), c2/done (1), c2/open (2)
        // Should return c1/done and c2/open
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid', 'status']),
            Query::having([Query::greaterThanEqual('cnt', 2)]),
        ]);

        $this->assertCount(2, $results);
        $keys = array_map(fn ($d) => $d->getAttribute('cust_uid') . '_' . $d->getAttribute('status'), $results);
        $this->assertContains('c1_done', $keys);
        $this->assertContains('c2_open', $keys);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinCountDistinctGrouped(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jcdg_o';
        $cCol = 'jcdg_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'product', type: ColumnType::String, size: 50, required: true));

        foreach (['c1', 'c2'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'product' => 'A'],
            ['cust_uid' => 'c1', 'product' => 'A'],
            ['cust_uid' => 'c1', 'product' => 'B'],
            ['cust_uid' => 'c1', 'product' => 'C'],
            ['cust_uid' => 'c2', 'product' => 'A'],
            ['cust_uid' => 'c2', 'product' => 'A'],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::countDistinct('product', 'unique_products'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('cust_uid')] = $doc;
        }
        $this->assertEquals(3, $mapped['c1']->getAttribute('unique_products'));
        $this->assertEquals(1, $mapped['c2']->getAttribute('unique_products'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinHavingOnSumWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jhsf_o';
        $cCol = 'jhsf_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $orders = [
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 100],
            ['cust_uid' => 'c1', 'status' => 'done', 'amount' => 200],
            ['cust_uid' => 'c1', 'status' => 'open', 'amount' => 9999],
            ['cust_uid' => 'c2', 'status' => 'done', 'amount' => 50],
            ['cust_uid' => 'c3', 'status' => 'done', 'amount' => 400],
            ['cust_uid' => 'c3', 'status' => 'done', 'amount' => 500],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        // Filter to 'done' only, then HAVING sum > 200
        // c1 done sum=300, c2 done sum=50, c3 done sum=900
        // → c1 and c3 match
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::equal('status', ['done']),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('total', 200)]),
            Query::orderAsc('total'),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('c1', $results[0]->getAttribute('cust_uid'));
        $this->assertEquals(300, (int) $results[0]->getAttribute('total'));
        $this->assertEquals('c3', $results[1]->getAttribute('cust_uid'));
        $this->assertEquals(900, (int) $results[1]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testLeftJoinGroupByWithOrderAndLimit(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $pCol = 'ljgl_p';
        $oCol = 'ljgl_o';
        $cols = [$pCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($pCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'prod_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'qty', type: ColumnType::Integer, size: 0, required: true));

        for ($i = 1; $i <= 5; $i++) {
            $pid = 'p' . $i;
            $database->createDocument($pCol, new Document([
                '$id' => $pid, 'name' => 'Product ' . $i,
                '$permissions' => [Permission::read(Role::any())],
            ]));
            for ($j = 0; $j < $i; $j++) {
                $database->createDocument($oCol, new Document([
                    'prod_uid' => $pid, 'qty' => 10,
                    '$permissions' => [Permission::read(Role::any())],
                ]));
            }
        }

        // Get top 3 products by order count, descending
        $results = $database->find($pCol, [
            Query::leftJoin($oCol, '$id', 'prod_uid'),
            Query::count('*', 'order_cnt'),
            Query::groupBy(['name']),
            Query::orderDesc('order_cnt'),
            Query::limit(3),
        ]);

        $this->assertCount(3, $results);
        $counts = array_map(fn ($d) => (int) $d->getAttribute('order_cnt'), $results);
        $this->assertEquals([5, 4, 3], $counts);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinWithEndsWith(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jew_o';
        $cCol = 'jew_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'tag', type: ColumnType::String, size: 50, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $orders = [
            ['cust_uid' => 'c1', 'tag' => 'order_express', 'amount' => 100],
            ['cust_uid' => 'c1', 'tag' => 'order_express', 'amount' => 200],
            ['cust_uid' => 'c1', 'tag' => 'order_standard', 'amount' => 50],
        ];
        foreach ($orders as $o) {
            $database->createDocument($oCol, new Document(array_merge($o, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::endsWith('tag', 'express'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(2, $results[0]->getAttribute('cnt'));
        $this->assertEquals(300, $results[0]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinHavingLessThanEqual(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $oCol = 'jhle_o';
        $cCol = 'jhle_c';
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection($cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($cCol, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));

        $database->createCollection($oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]);
        $database->createAttribute($oCol, new Attribute(key: 'cust_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($oCol, new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['c1', 'c2', 'c3'] as $cid) {
            $database->createDocument($cCol, new Document([
                '$id' => $cid, 'name' => 'Customer ' . $cid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        // c1: sum=100, c2: sum=200, c3: sum=300
        foreach (['c1' => [100], 'c2' => [100, 100], 'c3' => [100, 100, 100]] as $cid => $amounts) {
            foreach ($amounts as $amt) {
                $database->createDocument($oCol, new Document([
                    'cust_uid' => $cid, 'amount' => $amt,
                    '$permissions' => [Permission::read(Role::any())],
                ]));
            }
        }

        // HAVING sum <= 200 → c1 (100) and c2 (200)
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::lessThanEqual('total', 200)]),
            Query::orderAsc('total'),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('c1', $results[0]->getAttribute('cust_uid'));
        $this->assertEquals(100, (int) $results[0]->getAttribute('total'));
        $this->assertEquals('c2', $results[1]->getAttribute('cust_uid'));
        $this->assertEquals(200, (int) $results[1]->getAttribute('total'));

        $this->cleanupAggCollections($database, $cols);
    }
}
