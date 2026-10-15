<?php

namespace Tests\E2E\Adapter\Scopes;

use PHPUnit\Framework\Attributes\DataProvider;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Adapter\SQLite;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Order as OrderException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Filter;
use Utopia\Database\Index;
use Utopia\Database\Permission;
use Utopia\Database\Query;
use Utopia\Database\Role;
use Utopia\Database\Storage;
use Utopia\Query\Method;

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

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        foreach (['Alpha', 'Beta', 'Gamma'] as $name) {
            $database->createDocument($pCol, new Document([
                '$id' => strtolower($name),
                'name' => $name,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->aggregate($pCol, [
            Query::leftJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::count('*', 'cnt'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(3, $results);
        foreach ($results as $doc) {
            $this->assertEquals(1, $doc['cnt']);
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

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

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

        $results = $database->aggregate($pCol, [
            Query::leftJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::count('*', 'cnt'),
            Query::avg('score', 'avg_score'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(3, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $name = $doc['name'];
            $this->assertIsString($name);
            $mapped[$name] = $doc;
        }
        $this->assertEquals(3, $mapped['Product p1']['cnt']);
        $this->assertEqualsWithDelta(4.0, $this->numericAttribute($mapped['Product p1'], 'avg_score'), 0.1);
        $this->assertEquals(2, $mapped['Product p2']['cnt']);
        $this->assertEqualsWithDelta(3.0, $this->numericAttribute($mapped['Product p2'], 'avg_score'), 0.1);
        $this->assertEquals(1, $mapped['Product p3']['cnt']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'order_count'),
            Query::sum('amount', 'total_amount'),
            Query::avg('amount', 'avg_amount'),
            Query::min('amount', 'min_amount'),
            Query::max('amount', 'max_amount'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(5, $results[0]['order_count']);
        $this->assertEquals(1500, $results[0]['total_amount']);
        $this->assertEqualsWithDelta(300.0, $this->numericAttribute($results[0], 'avg_amount'), 0.1);
        $this->assertEquals(100, $results[0]['min_amount']);
        $this->assertEquals(500, $results[0]['max_amount']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'status', size: 20, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid', 'status']),
        ]);

        $this->assertCount(4, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $custUid = $doc['cust_uid'];
            $status = $doc['status'];
            $this->assertIsString($custUid);
            $this->assertIsString($status);
            $key = $custUid . '_' . $status;
            $mapped[$key] = $doc;
        }
        $this->assertEquals(2, $mapped['c1_done']['cnt']);
        $this->assertEquals(300, $mapped['c1_done']['total']);
        $this->assertEquals(1, $mapped['c1_pending']['cnt']);
        $this->assertEquals(50, $mapped['c1_pending']['total']);
        $this->assertEquals(1, $mapped['c2_done']['cnt']);
        $this->assertEquals(300, $mapped['c2_done']['total']);
        $this->assertEquals(2, $mapped['c2_pending']['cnt']);
        $this->assertEquals(100, $mapped['c2_pending']['total']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('cnt', 1)]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d['cust_uid'], $results);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::avg('amount', 'avg_amt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('avg_amt', 100)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c2', $results[0]['cust_uid']);
        $avgAmt = $results[0]['avg_amt'];
        $this->assertIsNumeric($avgAmt);
        $this->assertEqualsWithDelta(550.0, (float) $avgAmt, 0.1);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('total', 250)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c2', $results[0]['cust_uid']);
        $this->assertEquals(700, $results[0]['total']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::between('total', 100, 500)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c2', $results[0]['cust_uid']);
        $this->assertEquals(300, $results[0]['total']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'product', size: 50, required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::countDistinct('product', 'uniq_prod'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(3, $results[0]['uniq_prod']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::min('amount', 'min_amt'),
            Query::max('amount', 'max_amt'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $cust_uid = $doc['cust_uid'];
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
        }
        $this->assertEquals(10, $mapped['c1']['min_amt']);
        $this->assertEquals(50, $mapped['c1']['max_amt']);
        $this->assertEquals(100, $mapped['c2']['min_amt']);
        $this->assertEquals(200, $mapped['c2']['max_amt']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'status', size: 20, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::equal('status', ['done']),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $cust_uid = $doc['cust_uid'];
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
        }
        $this->assertEquals(1, $mapped['c1']['cnt']);
        $this->assertEquals(100, $mapped['c1']['total']);
        $this->assertEquals(2, $mapped['c2']['cnt']);
        $this->assertEquals(700, $mapped['c2']['total']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::between('amount', 100, 300),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(2, $results[0]['cnt']);
        $this->assertEquals(400, $results[0]['total']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::greaterThan('amount', 15),
            Query::lessThanEqual('amount', 40),
            Query::count('*', 'cnt'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(3, $results[0]['cnt']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $database->createDocument($oCol, new Document([
            'cust_uid' => 'nonexistent', 'amount' => 100,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(0, $results[0]['cnt']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'status', size: 20, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            'cust_uid' => 'c1', 'status' => 'done', 'amount' => 100,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::equal('status', ['ghost']),
            Query::count('*', 'cnt'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(0, $results[0]['cnt']);

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

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($pCol, [
            Query::leftJoin($oCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $name = $doc['name'];
            $this->assertIsString($name);
            $mapped[$name] = $doc;
        }
        $this->assertEquals(300, $mapped['WithOrders']['total']);
        $noOrderTotal = $mapped['NoOrders']['total'];
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'status', size: 20, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::equal('status', ['done']),
            Query::greaterThan('amount', 100),
            Query::sum('amount', 'total'),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('total', 500)]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d['cust_uid'], $results);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(10, $results);
        foreach ($results as $doc) {
            $this->assertEquals(10, $doc['cnt']);
            $this->assertEquals(550, $doc['total']);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'status', size: 20, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::notEqual('status', 'cancel'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(2, $results[0]['cnt']);
        $this->assertEquals(300, $results[0]['total']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'tag', size: 50, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::startsWith('tag', 'promo'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(2, $results[0]['cnt']);
        $this->assertEquals(300, $results[0]['total']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'status', size: 20, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::equal('status', ['done', 'open']),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $cust_uid = $doc['cust_uid'];
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
        }
        $this->assertEquals(2, $mapped['c1']['cnt']);
        $this->assertEquals(300, $mapped['c1']['total']);
        $this->assertEquals(1, $mapped['c2']['cnt']);
        $this->assertEquals(300, $mapped['c2']['total']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::lessThan('total', 100)]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d['cust_uid'], $results);
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

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($pCol, [
            Query::leftJoin($oCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::count('*', 'cnt'),
            Query::groupBy(['name']),
            Query::having([Query::greaterThan('cnt', 1)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('Product p1', $results[0]['name']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
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
            $cust_uid = $doc['cust_uid'];
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
        }

        $this->assertEquals(3, $mapped['c1']['cnt']);
        $this->assertEquals(600, $mapped['c1']['total']);
        $c1Avg = $mapped['c1']['avg_amt'];
        $this->assertIsNumeric($c1Avg);
        $this->assertEqualsWithDelta(200.0, (float) $c1Avg, 0.1);
        $this->assertEquals(100, $mapped['c1']['min_amt']);
        $this->assertEquals(300, $mapped['c1']['max_amt']);

        $this->assertEquals(2, $mapped['c2']['cnt']);
        $this->assertEquals(200, $mapped['c2']['total']);
        $c2Avg = $mapped['c2']['avg_amt'];
        $this->assertIsNumeric($c2Avg);
        $this->assertEqualsWithDelta(100.0, (float) $c2Avg, 0.1);
        $this->assertEquals(50, $mapped['c2']['min_amt']);
        $this->assertEquals(150, $mapped['c2']['max_amt']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(3, $results);
        foreach ($results as $doc) {
            $this->assertEquals(1, $doc['cnt']);
        }

        $mapped = [];
        foreach ($results as $doc) {
            $cust_uid = $doc['cust_uid'];
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
        }
        $this->assertEquals(100, $mapped['c1']['total']);
        $this->assertEquals(200, $mapped['c2']['total']);
        $this->assertEquals(300, $mapped['c3']['total']);

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

        $pCol = 'jtc_p_'.$joinMethod;
        $oCol = 'jtc_o_'.$joinMethod;
        $cols = [$pCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'qty', required: true));

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
            'join' => Query::join($oCol, 'j0', [Query::on('$id', 'prod_uid')]),
            'leftJoin' => Query::leftJoin($oCol, 'j0', [Query::on('$id', 'prod_uid')]),
            default => throw new \InvalidArgumentException('Unknown join method: '.$joinMethod),
        };

        $results = $database->aggregate($pCol, [
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

        $oCol = 'jat_o_'.$aggMethod;
        $cCol = 'jat_c_'.$aggMethod;
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
            default => throw new \InvalidArgumentException('Unknown aggregation method: '.$aggMethod),
        };

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            $aggQuery,
        ]);

        $this->assertCount(1, $results);
        if ($aggMethod === 'avg') {
            $result = $results[0]['result'];
            $this->assertIsNumeric($result);
            $this->assertEqualsWithDelta($expected, (float) $result, 0.1);
        } else {
            $this->assertEquals($expected, $results[0]['result']);
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

        $oCol = 'jho_o_'.$operator;
        $cCol = 'jho_c_'.$operator;
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
            default => throw new \InvalidArgumentException('Unknown operator: '.$operator),
        };

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('total'),
        ]);

        $this->assertCount(3, $results);
        $totals = array_map(fn (array $d) => $this->intAttribute($d, 'total'), $results);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('total'),
            Query::limit(2),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals(500, $this->intAttribute($results[0], 'total'));
        $this->assertEquals(400, $this->intAttribute($results[1], 'total'));

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('total'),
            Query::limit(2),
            Query::offset(1),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals(400, $this->intAttribute($results[0], 'total'));
        $this->assertEquals(300, $this->intAttribute($results[1], 'total'));

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([
                Query::greaterThanEqual('cnt', 2),
                Query::greaterThan('total', 200),
            ]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d['cust_uid'], $results);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::equal('cnt', [2])]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d['cust_uid'], $results);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1', 'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        // Main table (orders) is empty
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(0, $results[0]['cnt']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('cust_uid'),
        ]);

        $this->assertCount(3, $results);
        $custIds = array_map(fn ($d) => $d['cust_uid'], $results);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'title', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::join($pCol, 'j1', [Query::on('prod_uid', '$id')]),
            Query::count('*', 'order_cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $cust_uid = $doc['cust_uid'];
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
        }
        $this->assertEquals(3, $mapped['c1']['order_cnt']);
        $this->assertEquals(600, $this->intAttribute($mapped['c1'], 'total'));
        $this->assertEquals(2, $mapped['c2']['order_cnt']);
        $this->assertEquals(400, $this->intAttribute($mapped['c2'], 'total'));

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::notBetween('total', 50, 500)]),
        ]);

        $this->assertCount(2, $results);
        $ids = array_map(fn ($d) => $d['cust_uid'], $results);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'status', size: 20, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::equal('status', ['done']),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderAsc('total'),
        ]);

        $this->assertCount(3, $results);
        $totals = array_map(fn (array $d) => $this->intAttribute($d, 'total'), $results);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::notEqual('cnt', 2)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c1', $results[0]['cust_uid']);
        $this->assertEquals(1, $results[0]['cnt']);

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

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'qty', required: true));

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

        $results = $database->aggregate($pCol, [
            Query::leftJoin($oCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::count('*', 'cnt'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(2, $results);
        foreach ($results as $doc) {
            $this->assertEquals(1, $doc['cnt']);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'category', size: 50, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::equal('category', ['electronics']),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('total'),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('c2', $results[0]['cust_uid']);
        $this->assertEquals(1200, $this->intAttribute($results[0], 'total'));
        $this->assertEquals('c1', $results[1]['cust_uid']);
        $this->assertEquals(500, $this->intAttribute($results[1], 'total'));

        // Now books only
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::equal('category', ['books']),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c1', $results[0]['cust_uid']);
        $this->assertEquals(50, $this->intAttribute($results[0], 'total'));

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'status', size: 20, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid', 'status']),
            Query::having([Query::greaterThanEqual('cnt', 2)]),
        ]);

        $this->assertCount(2, $results);
        $keys = array_map(function (array $document): string {
            $custUid = $document['cust_uid'];
            $status = $document['status'];
            $this->assertIsString($custUid);
            $this->assertIsString($status);

            return $custUid . '_' . $status;
        }, $results);
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'product', size: 50, required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::countDistinct('product', 'unique_products'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $cust_uid = $doc['cust_uid'];
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
        }
        $this->assertEquals(3, $mapped['c1']['unique_products']);
        $this->assertEquals(1, $mapped['c2']['unique_products']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'status', size: 20, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::equal('status', ['done']),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('total', 200)]),
            Query::orderAsc('total'),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('c1', $results[0]['cust_uid']);
        $this->assertEquals(300, $this->intAttribute($results[0], 'total'));
        $this->assertEquals('c3', $results[1]['cust_uid']);
        $this->assertEquals(900, $this->intAttribute($results[1], 'total'));

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

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'qty', required: true));

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
        $results = $database->aggregate($pCol, [
            Query::leftJoin($oCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::count('*', 'order_cnt'),
            Query::groupBy(['name']),
            Query::orderDesc('order_cnt'),
            Query::limit(3),
        ]);

        $this->assertCount(3, $results);
        $counts = [];
        foreach ($results as $document) {
            $count = $document['order_cnt'];
            $this->assertIsNumeric($count);
            $counts[] = (int) $count;
        }
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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::string(key: 'tag', size: 50, required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::endsWith('tag', 'express'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(2, $results[0]['cnt']);
        $this->assertEquals(300, $results[0]['total']);

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

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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
        $results = $database->aggregate($oCol, [
            Query::join($cCol, 'j0', [Query::on('cust_uid', '$id')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::lessThanEqual('total', 200)]),
            Query::orderAsc('total'),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('c1', $results[0]['cust_uid']);
        $c1Total = $results[0]['total'];
        $this->assertIsNumeric($c1Total);
        $this->assertEquals(100, (int) $c1Total);
        $this->assertEquals('c2', $results[1]['cust_uid']);
        $c2Total = $results[1]['total'];
        $this->assertIsNumeric($c2Total);
        $this->assertEquals(200, (int) $c2Total);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testRightJoinIncludesUnmatchedRightRows(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 't8_rj_p';
        $rCol = 't8_rj_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'missing',
            'score' => 9,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->getAuthorization()->skip(fn () => $database->find($pCol, [
            Query::rightJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::select(['name']),
        ]));

        $this->assertCount(2, $results);
        $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
        \sort($ids);
        $this->assertSame(['', 'p1'], $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testCrossJoinCartesianProduct(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $aCol = 't8_xj_a';
        $bCol = 't8_xj_b';
        $cols = [$aCol, $bCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $aCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($aCol, Attribute::string(key: 'label', size: 100, required: true));

        $database->createCollection(Collection::create(id: $bCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($bCol, Attribute::string(key: 'tag', size: 100, required: true));

        foreach (['a1', 'a2', 'a3'] as $id) {
            $database->createDocument($aCol, new Document([
                '$id' => $id,
                'label' => $id,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }
        foreach (['b1', 'b2'] as $id) {
            $database->createDocument($bCol, new Document([
                '$id' => $id,
                'tag' => $id,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($aCol, [
            Query::crossJoin($bCol, 'j0'),
        ]);

        $this->assertCount(6, $results);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testFullOuterJoinIncludesBothUnmatched(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 't8_fo_p';
        $rCol = 't8_fo_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'missing',
            'score' => 9,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->getAuthorization()->skip(fn () => $database->find($pCol, [
            Query::fullOuterJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::select(['name']),
        ]));

        $this->assertSame(3, \count($results));
        $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
        \sort($ids);
        $this->assertSame(['', 'p1', 'p2'], $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testFullOuterJoinSelectDoesNotCollapseOneToMany(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 't8_fo_1n_p';
        $rCol = 't8_fo_1n_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 3,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'missing',
            'score' => 9,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->getAuthorization()->skip(fn () => $database->find($pCol, [
            Query::fullOuterJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::select(['name']),
        ]));

        $this->assertSame(4, \count($results));
        $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
        \sort($ids);
        $this->assertSame(['', 'p1', 'p1', 'p2'], $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testNaturalJoinThrowsQueryException(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 't8_nj_p';
        $rCol = 't8_nj_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'name', size: 100, required: true));

        try {
            $database->find($pCol, [
                Query::naturalJoin($rCol, 'j0'),
            ]);
            $this->fail('Expected QueryException for natural join');
        } catch (QueryException $exception) {
            $this->assertStringContainsString('Natural joins are not supported', $exception->getMessage());
        } finally {
            $this->cleanupAggCollections($database, $cols);
        }
    }

    public function testJoinOperatorGreaterThan(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $lCol = 't8_jgt_l';
        $rCol = 't8_jgt_r';
        $cols = [$lCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $lCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($lCol, Attribute::integer(key: 'value', required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::integer(key: 'threshold', required: true));

        foreach ([10, 20, 30] as $value) {
            $database->createDocument($lCol, new Document([
                'value' => $value,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }
        foreach ([15, 25] as $threshold) {
            $database->createDocument($rCol, new Document([
                'threshold' => $threshold,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $results = $database->find($lCol, [
            Query::join($rCol, 'j0', [Query::on('value', 'threshold', '>')]),
        ]);

        $this->assertCount(3, $results);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinPreservesUserAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $cCol = 't8_jua_c';
        $oCol = 't8_jua_o';
        $cols = [$cCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1',
            'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            'cust_uid' => 'c1',
            'amount' => 150,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($cCol, [
            Query::join($oCol, 'ord', [Query::on('$id', 'cust_uid')]),
            Query::select(['name', 'ord.amount']),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame('c1', $results[0]->getId());
        $this->assertSame('Customer 1', $results[0]->getAttribute('name'));
        $amount = $results[0]->getAttribute('ord.amount');
        $this->assertIsNumeric($amount);
        $this->assertSame(150, (int) $amount);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testChainedJoinsQualifySecondJoinLeft(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $cCol = 't8_jch_c';
        $oCol = 't8_jch_o';
        $iCol = 't8_jch_i';
        $cols = [$cCol, $oCol, $iCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

        $database->createCollection(Collection::create(id: $iCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($iCol, Attribute::string(key: 'order_uid', required: true));
        $database->createAttribute($iCol, Attribute::string(key: 'sku', size: 100, required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1',
            'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            '$id' => 'o1',
            'cust_uid' => 'c1',
            'amount' => 150,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($iCol, new Document([
            'order_uid' => 'o1',
            'sku' => 'widget',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($cCol, [
            Query::join($oCol, 'ord', [Query::on('$id', 'cust_uid')]),
            Query::join($iCol, 'itm', [Query::on('ord.$id', 'order_uid')]),
            Query::select(['name', 'ord.amount', 'itm.sku']),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame('c1', $results[0]->getId());
        $chainedAmount = $results[0]->getAttribute('ord.amount');
        $this->assertIsNumeric($chainedAmount);
        $this->assertSame(150, (int) $chainedAmount);
        $this->assertSame('widget', $results[0]->getAttribute('itm.sku'));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testSelectJoinedColumnByAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $cCol = 't8_jsa_c';
        $oCol = 't8_jsa_o';
        $cols = [$cCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'c1',
            'name' => 'Customer 1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            'cust_uid' => 'c1',
            'amount' => 275,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($cCol, [
            Query::select(['name', 'ord.amount']),
            Query::join($oCol, 'ord', [Query::on('$id', 'cust_uid')]),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame('Customer 1', $results[0]->getAttribute('name'));
        $selectedAmount = $results[0]->getAttribute('ord.amount');
        $this->assertIsNumeric($selectedAmount);
        $this->assertSame(275, (int) $selectedAmount);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testRightJoinUnmatchedRowSurvivesDocumentSecurity(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 't8_rjds_p';
        $rCol = 't8_rjds_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'missing',
            'score' => 9,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($pCol, [
            Query::rightJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::select(['name']),
        ]);

        $this->assertSame(2, \count($results));
        $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
        \sort($ids);
        $this->assertSame(['', 'p1'], $ids);

        $unmatched = null;
        foreach ($results as $document) {
            if ($document->getId() === '') {
                $unmatched = $document;
                break;
            }
        }
        $this->assertNotNull($unmatched);
        $this->assertTrue($unmatched->getAttribute('name') === null || $unmatched->getAttribute('name') === '');

        $this->cleanupAggCollections($database, $cols);
    }

    public function testFullOuterJoinUnmatchedRowsSurviveDocumentSecurity(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 't8_fods_p';
        $rCol = 't8_fods_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'missing',
            'score' => 9,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($pCol, [
            Query::fullOuterJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::select(['name']),
        ]);

        $this->assertSame(3, \count($results));
        $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
        \sort($ids);
        $this->assertSame(['', 'p1', 'p2'], $ids);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testSelfJoinAppliesPermissionToEachAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $col = 't8_sjacl';
        $cols = [$col];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $col, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($col, Attribute::string(key: 'payload', size: 100, required: true));
        $database->createAttribute($col, Attribute::string(key: 'code', size: 100, required: true));
        $database->createAttribute($col, Attribute::string(key: 'tag', size: 50, required: true));

        $database->createDocument($col, new Document([
            '$id' => 'open',
            'payload' => 'open-payload',
            'code' => 'open-code',
            'tag' => 'shared',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($col, new Document([
            '$id' => 'secret',
            'payload' => 'secret-payload',
            'code' => 'secret-code',
            'tag' => 'shared',
            '$permissions' => [Permission::read(Role::user('other'))],
        ]));

        $authorization = $database->getAuthorization();
        $previousRoles = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());

        try {
            $results = $database->find($col, [
                Query::join($col, 'visible', [Query::on('tag', 'tag')]),
                Query::join($col, 'hidden', [Query::on('tag', 'tag')]),
                Query::select(['visible.payload', 'hidden.code']),
            ]);

            $this->assertSame(1, \count($results));
            $this->assertSame('open-payload', $results[0]->getAttribute('visible.payload'));
            $this->assertSame('open-code', $results[0]->getAttribute('hidden.code'));

            foreach ($results as $document) {
                $this->assertNotSame('secret-payload', $document->getAttribute('visible.payload'));
                $this->assertNotSame('secret-code', $document->getAttribute('hidden.code'));
            }
        } finally {
            $authorization->cleanRoles();
            foreach ($previousRoles as $role) {
                $authorization->addRole($role);
            }
            $this->cleanupAggCollections($database, $cols);
        }
    }

    public function testRightJoinDoesNotLeakUnauthorizedJoinDocument(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $cCol = 't8_rjlk_c';
        $oCol = 't8_rjlk_o';
        $cols = [$cCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'customerId', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'cust1',
            'name' => 'Alice',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($cCol, new Document([
            '$id' => 'cust2',
            'name' => 'Bob',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            '$id' => 'ord-public',
            'customerId' => 'cust1',
            'amount' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            '$id' => 'ord-secret',
            'customerId' => 'cust1',
            'amount' => 999,
            '$permissions' => [Permission::read(Role::user('other'))],
        ]));

        $authorization = $database->getAuthorization();
        $previousRoles = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());

        try {
            $results = $database->find($cCol, [
                Query::rightJoin($oCol, 'j0', [Query::on('$id', 'customerId')]),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));

            $amounts = [];
            foreach ($results as $document) {
                $this->assertNotSame('ord-secret', $document->getId());
                $amount = $document->getAttribute('j0.amount');
                if (\is_numeric($amount)) {
                    $amount = (int) $amount;
                    $amounts[] = $amount;
                    $this->assertNotSame(999, $amount);
                }
            }
            $this->assertContains(10, $amounts);
        } finally {
            $authorization->cleanRoles();
            foreach ($previousRoles as $role) {
                $authorization->addRole($role);
            }
            $this->cleanupAggCollections($database, $cols);
        }
    }

    public function testFullOuterJoinDoesNotLeakUnauthorizedJoinDocument(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $cCol = 't8_folk_c';
        $oCol = 't8_folk_o';
        $cols = [$cCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $oCol, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'customerId', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

        $database->createDocument($cCol, new Document([
            '$id' => 'cust1',
            'name' => 'Alice',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($cCol, new Document([
            '$id' => 'cust2',
            'name' => 'Bob',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            '$id' => 'ord-public',
            'customerId' => 'cust1',
            'amount' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($oCol, new Document([
            '$id' => 'ord-secret',
            'customerId' => 'cust1',
            'amount' => 999,
            '$permissions' => [Permission::read(Role::user('other'))],
        ]));

        $authorization = $database->getAuthorization();
        $previousRoles = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());

        try {
            $results = $database->find($cCol, [
                Query::fullOuterJoin($oCol, 'j0', [Query::on('$id', 'customerId')]),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));

            $amounts = [];
            foreach ($results as $document) {
                $this->assertNotSame('ord-secret', $document->getId());
                $amount = $document->getAttribute('j0.amount');
                if (\is_numeric($amount)) {
                    $amount = (int) $amount;
                    $amounts[] = $amount;
                    $this->assertNotSame(999, $amount);
                }
            }
            $this->assertContains(10, $amounts);
        } finally {
            $authorization->cleanRoles();
            foreach ($previousRoles as $role) {
                $authorization->addRole($role);
            }
            $this->cleanupAggCollections($database, $cols);
        }
    }

    public function testRightJoinUnmatchedRowSurvivesSharedTables(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        if (! $database->getAdapter()->supports(Capability::Schemas)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $sharedTables = $database->hasSharedTables();
        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();
        $tenant = $database->getTenant();

        $sharedTablesDb = 'sharedTablesRj_'.static::getTestToken();
        $pCol = 't8_rjst_p';
        $rCol = 't8_rjst_r';

        try {
            if ($database->exists($sharedTablesDb)) {
                $database->setDatabase($sharedTablesDb)->delete();
            }

            $database
                ->setDatabase($sharedTablesDb)
                ->setNamespace('')
                ->setSharedTables(true)
                ->setTenant(null)
                ->create();

            $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
            $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

            $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
            $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
            $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

            $database->setTenant(1);
            $database->createDocument($pCol, new Document([
                '$id' => 'p1',
                'name' => 'Product p1',
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($rCol, new Document([
                '$id' => 'r-match',
                'prod_uid' => 'p1',
                'score' => 5,
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($rCol, new Document([
                '$id' => 'r-unmatched',
                'prod_uid' => 'missing',
                'score' => 9,
                '$permissions' => [Permission::read(Role::any())],
            ]));

            $database->setTenant(2);
            $database->createDocument($rCol, new Document([
                '$id' => 'r-other-tenant',
                'prod_uid' => 'missing',
                'score' => 77,
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($rCol, new Document([
                '$id' => 'r-other-match',
                'prod_uid' => 'p1',
                'score' => 88,
                '$permissions' => [Permission::read(Role::any())],
            ]));

            $database->setTenant(1);
            $results = $database->find($pCol, [
                Query::rightJoin($rCol, 'rev', [Query::on('$id', 'prod_uid')]),
                Query::select(['name', 'rev.score']),
            ]);

            $this->assertSame(2, \count($results));

            $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
            \sort($ids);
            $this->assertSame(['', 'p1'], $ids);

            $scores = [];
            $unmatched = null;
            foreach ($results as $document) {
                $this->assertNotSame('r-other-tenant', $document->getId());
                $this->assertNotSame('r-other-match', $document->getId());
                $score = $document->getAttribute('rev.score');
                if (\is_numeric($score)) {
                    $score = (int) $score;
                    $scores[] = $score;
                    $this->assertNotSame(77, $score);
                    $this->assertNotSame(88, $score);
                }
                if ($document->getId() === '') {
                    $unmatched = $document;
                }
            }

            $this->assertNotNull($unmatched);
            $this->assertTrue($unmatched->getAttribute('name') === null || $unmatched->getAttribute('name') === '');
            \sort($scores);
            $this->assertSame([5, 9], $scores);
        } finally {
            $database->setTenant(null)->setSharedTables(false);
            if ($database->exists($sharedTablesDb)) {
                $database->delete($sharedTablesDb);
            }
            $database
                ->setSharedTables($sharedTables)
                ->setTenant($tenant)
                ->setNamespace($namespace)
                ->setDatabase($schema);
        }
    }

    public function testGetDocumentInnerJoinMatched(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_ijm_p';
        $rCol = 'gd_ijm_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p1', [
            Query::join($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
        ]);

        $this->assertSame(false, $document->isEmpty());
        $score = $document->getAttribute('j0.score');
        $this->assertIsNumeric($score);
        $this->assertSame(5, (int) $score);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentLeftJoinUnmatchedNullish(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_ljun_p';
        $rCol = 'gd_ljun_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p2', [
            Query::leftJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
        ]);

        $this->assertSame(false, $document->isEmpty());
        $this->assertArrayHasKey('j0.score', $document->getArrayCopy());
        $score = $document->getAttribute('j0.score');
        $this->assertTrue($score === null || $score === '');

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentInnerJoinUnmatchedEmpty(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_ijue_p';
        $rCol = 'gd_ijue_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p2', [
            Query::join($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
        ]);

        $this->assertSame(true, $document->isEmpty());

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentRightJoinUnmatchedEmpty(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_rjue_p';
        $rCol = 'gd_rjue_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p2', [
            Query::rightJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
        ]);

        $this->assertSame(true, $document->isEmpty());

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentOneToManyReturnsFirstRow(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_otm_p';
        $rCol = 'gd_otm_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 3,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p1', [
            Query::join($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
        ]);

        $this->assertSame(false, $document->isEmpty());
        $score = $document->getAttribute('j0.score');
        $this->assertIsNumeric($score);
        $this->assertContains((int) $score, [5, 3]);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentSelectPlusJoin(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_spj_p';
        $rCol = 'gd_spj_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p1', [
            Query::join($rCol, 'rev', [Query::on('$id', 'prod_uid')]),
            Query::select(['name', 'rev.score']),
        ]);

        $this->assertSame(false, $document->isEmpty());
        $this->assertSame('Product p1', $document->getAttribute('name'));
        $score = $document->getAttribute('rev.score');
        $this->assertIsNumeric($score);
        $this->assertSame(5, (int) $score);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentRejectsCount(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_rc_p';
        $rCol = 'gd_rc_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $this->expectException(QueryException::class);
        try {
            $database->getDocument($pCol, 'p1', [
                Query::join($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
                Query::count('*', 'cnt'),
            ]);
        } finally {
            $this->cleanupAggCollections($database, $cols);
        }
    }

    public function testGetDocumentRejectsNaturalJoin(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_rnj_p';
        $rCol = 'gd_rnj_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'name', size: 100, required: true));

        try {
            $database->getDocument($pCol, 'p1', [
                Query::naturalJoin($rCol, 'j0'),
            ]);
            $this->fail('Expected QueryException for natural join');
        } catch (QueryException $exception) {
            $this->assertStringContainsString('Natural joins are not supported', $exception->getMessage());
        } finally {
            $this->cleanupAggCollections($database, $cols);
        }
    }

    public function testGetDocumentSkipsCacheWhenJoinsPresent(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_sc_p';
        $rCol = 'gd_sc_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $database->getDocument($pCol, 'p1');
        $document = $database->getDocument($pCol, 'p1', [
            Query::leftJoin($rCol, 'rev', [Query::on('$id', 'prod_uid')]),
            Query::select(['rev.score']),
        ]);

        $this->assertSame(false, $document->isEmpty());
        $this->assertSame('p1', $document->getId());
        $score = $document->getAttribute('rev.score');
        $this->assertIsNumeric($score);
        $this->assertSame(5, (int) $score);

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentJoinKeepsMainDocumentId(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_jmid_p';
        $rCol = 'gd_jmid_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            '$id' => 'r1',
            'prod_uid' => 'p1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $inner = $database->getDocument($pCol, 'p1', [
            Query::join($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
        ]);
        $this->assertSame(false, $inner->isEmpty());
        $this->assertSame('p1', $inner->getId());

        $left = $database->getDocument($pCol, 'p1', [
            Query::leftJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
        ]);
        $this->assertSame(false, $left->isEmpty());
        $this->assertSame('p1', $left->getId());

        $unmatched = $database->getDocument($pCol, 'p2', [
            Query::leftJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
        ]);
        $this->assertSame(false, $unmatched->isEmpty());
        $this->assertSame('p2', $unmatched->getId());

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentFullOuterJoinExistingIdBehavesLikeLeft(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_foj_p';
        $rCol = 'gd_foj_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p2', [
            Query::fullOuterJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
        ]);

        $this->assertSame(false, $document->isEmpty());
        $this->assertSame('p2', $document->getId());
        $this->assertArrayHasKey('j0.score', $document->getArrayCopy());
        $score = $document->getAttribute('j0.score');
        $this->assertTrue($score === null || $score === '');

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentFullOuterJoinUnauthorizedJoinStillReturnsMain(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 'gd_fojua_p';
        $rCol = 'gd_fojua_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            '$id' => 'r-secret',
            'prod_uid' => 'p1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('other'))],
        ]));

        $authorization = $database->getAuthorization();
        $previousRoles = $authorization->getRoles();
        $authorization->cleanRoles();
        $authorization->addRole(Role::any()->toString());

        try {
            $document = $database->getDocument($pCol, 'p1', [
                Query::fullOuterJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            ]);
            $this->assertSame(false, $document->isEmpty());
            $this->assertSame('p1', $document->getId());
            $this->assertNotSame('r-secret', $document->getId());
            $score = $document->getAttribute('j0.score');
            if (\is_numeric($score)) {
                $score = (int) $score;
                $this->assertNotSame(999, $score);
            }

            $left = $database->getDocument($pCol, 'p1', [
                Query::leftJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            ]);
            $this->assertSame(false, $left->isEmpty());
            $this->assertSame('p1', $left->getId());
        } finally {
            $authorization->cleanRoles();
            foreach ($previousRoles as $role) {
                $authorization->addRole($role);
            }
            $this->cleanupAggCollections($database, $cols);
        }
    }

    public function testFullOuterJoinLimitAppliesToOuterQuery(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 't8_folim_p';
        $rCol = 't8_folim_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p1',
            'name' => 'Product p1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'missing1',
            'score' => 8,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($rCol, new Document([
            'prod_uid' => 'missing2',
            'score' => 9,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $results = $database->find($pCol, [
            Query::fullOuterJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::limit(2),
        ]);

        $this->assertSame(2, \count($results));

        $this->cleanupAggCollections($database, $cols);
    }

    public function testFullOuterJoinOffsetAppliesToOuterQuery(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $pCol = 't8_fooff_p';
        $rCol = 't8_fooff_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        foreach (['p1', 'p2', 'p3', 'p4'] as $id) {
            $database->createDocument($pCol, new Document([
                '$id' => $id,
                'name' => 'Product '.$id,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $queries = [
            Query::fullOuterJoin($rCol, 'j0', [Query::on('$id', 'prod_uid')]),
            Query::orderAsc('name'),
        ];

        $full = $database->find($pCol, $queries);
        $sliced = $database->find($pCol, [
            ...$queries,
            Query::limit(2),
            Query::offset(1),
        ]);

        $identity = static function (Document $document): string {
            $name = $document->getAttribute('name');
            $score = $document->getAttribute('j0.score');

            return $document->getId().':'.(\is_scalar($name) ? (string) $name : '').':'.(\is_scalar($score) ? (string) $score : '');
        };

        $this->assertSame(2, \count($sliced));
        $this->assertSame(
            \array_slice(\array_map($identity, $full), 1, 2),
            \array_map($identity, $sliced)
        );

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinCollectionAclRejectsUnauthorizedJoinedCollection(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_nocr_m';
        $jCol = 'jp_nocr_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $jCol, permissions: [Permission::create(Role::any())], documentSecurity: false));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $joins = [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
                Query::leftJoin($jCol, 'j1', [Query::on('$id', 'mainId')]),
                Query::rightJoin($jCol, 'j2', [Query::on('$id', 'mainId')]),
                Query::fullOuterJoin($jCol, 'j3', [Query::on('$id', 'mainId')]),
                Query::crossJoin($jCol, 'j4'),
            ];

            foreach ($joins as $join) {
                try {
                    $results = $database->find($mCol, [$join]);
                    foreach ($results as $document) {
                        $this->assertJoinAttributesAbsent($document);
                        $this->assertSecretJoinHidden($document, 'j-secret', 999);
                    }
                } catch (AuthorizationException|QueryException $exception) {
                    $this->assertNotSame('', $exception->getMessage());
                }
            }

            try {
                $document = $database->getDocument($mCol, 'm1', [
                    Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
                ]);
                if (! $document->isEmpty()) {
                    $this->assertJoinAttributesAbsent($document);
                    $this->assertSecretJoinHidden($document, 'j-secret', 999);
                }
            } catch (AuthorizationException|QueryException $exception) {
                $this->assertNotSame('', $exception->getMessage());
            }
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinCollectionAclAllowsWhenDocumentSecurityOff(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_dsoff_m';
        $jCol = 'jp_dsoff_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $jCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j1',
            'mainId' => 'm1',
            'score' => 5,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));
            $this->assertContains(5, $this->numericScores($results));
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinCollectionAclAllowsWhenDocumentSecurityOffOnPhysicalIds(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'database_1_collection_1';
        $jCol = 'database_1_collection_2';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $jCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('other'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::leftJoin($jCol, 'rev', [Query::on('$id', 'mainId')]),
                Query::select(['name', 'rev.score']),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));
            $this->assertContains(999, $this->aliasedScores($results));

            $rewritten = Query::leftJoin('jp_dsoff_public', 'rev', [Query::on('$id', 'mainId')]);
            $rewritten->setAttribute($jCol);
            $rewrittenResults = $database->find($mCol, [
                $rewritten,
                Query::select(['name', 'rev.score']),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($rewrittenResults));
            $this->assertContains(999, $this->aliasedScores($rewrittenResults));

            $document = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($jCol, 'rev', [Query::on('$id', 'mainId')]),
                Query::select(['name', 'rev.score']),
            ]);
            $this->assertSame(false, $document->isEmpty());
            $this->assertContains(999, $this->aliasedScores([$document]));
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testInnerJoinDoesNotLeakUnauthorizedJoinDocument(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_ij_m';
        $jCol = 'jp_ij_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));
            foreach ($results as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
            }
            $this->assertContains(10, $this->numericScores($results));
            $this->assertSame(false, \in_array(999, $this->numericScores($results), true));
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testLeftJoinUnauthorizedJoinAttributesAreNullish(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_lj_m';
        $jCol = 'jp_lj_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Alice',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'm2',
            'name' => 'Bob',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::leftJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);

            $this->assertGreaterThanOrEqual(2, \count($results));
            $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
            $this->assertContains('m1', $ids);
            $this->assertContains('m2', $ids);

            foreach ($results as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
                $this->assertNullishScore($document);
            }
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testRightJoinDoesNotLeakUnauthorizedMainOrJoin(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_rj_m';
        $jCol = 'jp_rj_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol, mainGranted: false);

        $database->createDocument($mCol, new Document([
            '$id' => 'm-public',
            'name' => 'Public Main',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'm-secret',
            'name' => 'Secret Main',
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm-public',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm-secret',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-unmatched',
            'mainId' => 'missing',
            'score' => 7,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-unmatched-secret',
            'mainId' => 'missing-secret',
            'score' => 888,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::rightJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));
            $scores = $this->numericScores($results);
            $this->assertContains(10, $scores);
            $this->assertSame(false, \in_array(999, $scores, true));
            $this->assertSame(false, \in_array(888, $scores, true));

            foreach ($results as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
                $this->assertNotSame('m-secret', $document->getId());
                $this->assertNotSame('j-unmatched-secret', $document->getId());
                $name = $document->getAttribute('name');
                $this->assertNotSame('Secret Main', $name);
            }
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testFullOuterJoinFindPermissionMatrix(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_fo_m';
        $jCol = 'jp_fo_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol, mainGranted: false);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Matched',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'm2',
            'name' => 'Unmatched Left',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'm-secret',
            'name' => 'Secret Main',
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-unmatched',
            'mainId' => 'missing',
            'score' => 7,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-unmatched-secret',
            'mainId' => 'missing-secret',
            'score' => 888,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::fullOuterJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);

            $ids = \array_map(static fn (Document $document): string => $document->getId(), $results);
            $this->assertContains('m1', $ids);
            $this->assertContains('m2', $ids);
            $this->assertSame(false, \in_array('m-secret', $ids, true));
            $this->assertSame(false, \in_array('j-secret', $ids, true));
            $this->assertSame(false, \in_array('j-unmatched-secret', $ids, true));

            $scores = $this->numericScores($results);
            $this->assertContains(10, $scores);
            $this->assertContains(7, $scores);
            $this->assertSame(false, \in_array(999, $scores, true));
            $this->assertSame(false, \in_array(888, $scores, true));

            $unmatchedLeft = null;
            $unmatchedRight = null;
            foreach ($results as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
                $this->assertNotSame('Secret Main', $document->getAttribute('name'));
                if ($document->getId() === 'm2') {
                    $unmatchedLeft = $document;
                }
                if ($document->getId() === '') {
                    $score = $document->getAttribute('j0.score');
                    if (\is_numeric($score) && (int) $score === 7) {
                        $unmatchedRight = $document;
                    }
                }
            }

            $this->assertNotNull($unmatchedLeft);
            $this->assertNullishScore($unmatchedLeft);
            $this->assertNotNull($unmatchedRight);
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testCrossJoinDoesNotLeakUnauthorizedJoinDocuments(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_xj_m';
        $jCol = 'jp_xj_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'A',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'm2',
            'name' => 'B',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::crossJoin($jCol, 'j0'),
            ]);

            $this->assertSame(2, \count($results));
            foreach ($results as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
            }
            $this->assertSame([10, 10], $this->numericScores($results));
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentJoinPermissionMatrix(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_gd_m';
        $jCol = 'jp_gd_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Secret Match',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'm2',
            'name' => 'Public Match',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'm3',
            'name' => 'Unmatched',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm2',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $innerSecret = $database->getDocument($mCol, 'm1', [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(true, $innerSecret->isEmpty());

            $innerPublic = $database->getDocument($mCol, 'm2', [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(false, $innerPublic->isEmpty());
            $this->assertSame('m2', $innerPublic->getId());
            $this->assertSecretJoinHidden($innerPublic, 'j-secret', 999);
            $publicScore = $innerPublic->getAttribute('j0.score');
            $this->assertTrue(\is_numeric($publicScore));
            $this->assertSame(10, (int) $publicScore);

            $leftSecret = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(false, $leftSecret->isEmpty());
            $this->assertSame('m1', $leftSecret->getId());
            $this->assertNotSame('j-secret', $leftSecret->getId());
            $this->assertSecretJoinHidden($leftSecret, 'j-secret', 999);
            $this->assertNullishScore($leftSecret);

            $leftPublic = $database->getDocument($mCol, 'm2', [
                Query::leftJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(false, $leftPublic->isEmpty());
            $this->assertSame('m2', $leftPublic->getId());

            $rightSecret = $database->getDocument($mCol, 'm1', [
                Query::rightJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            if (! $rightSecret->isEmpty()) {
                $this->assertSame('m1', $rightSecret->getId());
                $this->assertSecretJoinHidden($rightSecret, 'j-secret', 999);
            }

            $rightPublic = $database->getDocument($mCol, 'm2', [
                Query::rightJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(false, $rightPublic->isEmpty());
            $this->assertSame('m2', $rightPublic->getId());
            $this->assertSecretJoinHidden($rightPublic, 'j-secret', 999);

            $fojSecret = $database->getDocument($mCol, 'm1', [
                Query::fullOuterJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(false, $fojSecret->isEmpty());
            $this->assertSame('m1', $fojSecret->getId());
            $this->assertNotSame('j-secret', $fojSecret->getId());
            $this->assertSecretJoinHidden($fojSecret, 'j-secret', 999);
            $this->assertNullishScore($fojSecret);

            $fojPublic = $database->getDocument($mCol, 'm2', [
                Query::fullOuterJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(false, $fojPublic->isEmpty());
            $this->assertSame('m2', $fojPublic->getId());
            $this->assertSecretJoinHidden($fojPublic, 'j-secret', 999);

            $fojUnmatched = $database->getDocument($mCol, 'm3', [
                Query::fullOuterJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(false, $fojUnmatched->isEmpty());
            $this->assertSame('m3', $fojUnmatched->getId());
            $this->assertSecretJoinHidden($fojUnmatched, 'j-secret', 999);
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinSelectDoesNotReturnSecretScore(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_sel_m';
        $jCol = 'jp_sel_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $finds = $database->find($mCol, [
                Query::leftJoin($jCol, 'rev', [Query::on('$id', 'mainId')]),
                Query::select(['name', 'rev.score']),
            ]);
            foreach ($finds as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
            }
            $this->assertContains(10, $this->numericScores($finds));
            $this->assertSame(false, \in_array(999, $this->numericScores($finds), true));

            $document = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($jCol, 'rev', [Query::on('$id', 'mainId')]),
                Query::select(['rev.score']),
            ]);
            $this->assertSame(false, $document->isEmpty());
            $this->assertSame('m1', $document->getId());
            $this->assertSecretJoinHidden($document, 'j-secret', 999);
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinDoesNotLeakOtherTenantRows(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $sharedTables = $database->hasSharedTables();
        $supportsSchemas = $database->getAdapter()->supports(Capability::Schemas);
        if (! $sharedTables && ! $supportsSchemas) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $namespace = $database->getNamespace();
        $schema = $database->getDatabase();
        $tenant = $database->getTenant();
        $createdDatabase = false;
        $sharedTablesDb = 'sharedTablesJp_'.static::getTestToken();
        $mCol = 'jp_tn_m';
        $jCol = 'jp_tn_j';
        $cols = [$mCol, $jCol];

        try {
            if ($supportsSchemas) {
                if ($database->exists($sharedTablesDb)) {
                    $database->setDatabase($sharedTablesDb)->delete();
                }

                $database
                    ->setDatabase($sharedTablesDb)
                    ->setNamespace('')
                    ->setSharedTables(true)
                    ->setTenant(null)
                    ->create();
                $createdDatabase = true;
            } else {
                $database->setTenant(null);
            }

            $this->cleanupAggCollections($database, $cols);
            $this->createJoinPermissionCollections($database, $mCol, $jCol);

            $database->setTenant(1);
            $database->createDocument($mCol, new Document([
                '$id' => 'm1',
                'name' => 'Tenant One',
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($mCol, new Document([
                '$id' => 'm2',
                'name' => 'Unmatched Left',
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($jCol, new Document([
                '$id' => 'j-match',
                'mainId' => 'm1',
                'score' => 5,
                '$permissions' => [Permission::read(Role::any())],
            ]));

            $database->setTenant(2);
            $database->createDocument($jCol, new Document([
                '$id' => 'j-other-match',
                'mainId' => 'm1',
                'score' => 88,
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($jCol, new Document([
                '$id' => 'j-other-unmatched',
                'mainId' => 'missing',
                'score' => 77,
                '$permissions' => [Permission::read(Role::any())],
            ]));

            $database->setTenant(1);
            $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
                foreach ([
                    [Query::join($jCol, 'j0', [Query::on('$id', 'mainId')])],
                    [Query::leftJoin($jCol, 'j0', [Query::on('$id', 'mainId')])],
                    [Query::fullOuterJoin($jCol, 'j0', [Query::on('$id', 'mainId')])],
                ] as $queries) {
                    $results = $database->find($mCol, $queries);
                    $this->assertGreaterThanOrEqual(1, \count($results));
                    $scores = $this->numericScores($results);
                    $this->assertSame(false, \in_array(88, $scores, true));
                    $this->assertSame(false, \in_array(77, $scores, true));
                    foreach ($results as $document) {
                        $this->assertNotSame('j-other-match', $document->getId());
                        $this->assertNotSame('j-other-unmatched', $document->getId());
                        $this->assertSecretJoinHidden($document, 'j-other-match', 88);
                    }
                }
            });
        } finally {
            if ($createdDatabase) {
                $database->setTenant(null)->setSharedTables(false);
                if ($database->exists($sharedTablesDb)) {
                    $database->delete($sharedTablesDb);
                }
                $database
                    ->setSharedTables($sharedTables)
                    ->setTenant($tenant)
                    ->setNamespace($namespace)
                    ->setDatabase($schema);
            } else {
                $database->setTenant(null);
                $this->cleanupAggCollections($database, $cols);
                $database->setTenant($tenant);
            }
        }
    }

    public function testJoinSecretRowOnlyVisibleToMatchingRole(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_rl_m';
        $jCol = 'jp_rl_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $mCol, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $jCol, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::read(Role::user('jp-acl')),
                Permission::read(Role::guests()),
            ],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-any',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-guest',
            'mainId' => 'm1',
            'score' => 20,
            '$permissions' => [Permission::read(Role::guests())],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $scores = $this->numericScores($results);
            $this->assertContains(10, $scores);
            $this->assertSame(false, \in_array(999, $scores, true));
            $this->assertSame(false, \in_array(20, $scores, true));
            foreach ($results as $document) {
                $this->assertNotSame('j-secret', $document->getId());
            }
        });

        $this->withAuthorizationRoles($database, [Role::user('jp-acl')->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $scores = $this->numericScores($results);
            $this->assertContains(999, $scores);
            $this->assertSame(false, \in_array(10, $scores, true));
            $this->assertSame(false, \in_array(20, $scores, true));
        });

        $this->withAuthorizationRoles($database, [Role::guests()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $scores = $this->numericScores($results);
            $this->assertContains(20, $scores);
            $this->assertSame(false, \in_array(999, $scores, true));
            $this->assertSame(false, \in_array(10, $scores, true));
            foreach ($results as $document) {
                $this->assertNotSame('j-secret', $document->getId());
            }
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinSkipAuthDoesNotSkipJoinSideAcl(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_sa_m';
        $jCol = 'jp_sa_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $withoutJoin = $database->find($mCol);
            $this->assertSame(1, \count($withoutJoin));
            $this->assertSame('m1', $withoutJoin[0]->getId());

            $inner = $database->find($mCol, [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertContains(10, $this->numericScores($inner));
            $this->assertSame(false, \in_array(999, $this->numericScores($inner), true));
            foreach ($inner as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
            }

            $left = $database->find($mCol, [
                Query::leftJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($left));
            foreach ($left as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
            }

            $document = $database->getDocument($mCol, 'm1', [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            if (! $document->isEmpty()) {
                $this->assertSame('m1', $document->getId());
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
            }
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinFilterOrderHavingOracleDoesNotRevealSecret(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_foh_m';
        $jCol = 'jp_foh_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $join = Query::join($jCol, 'rev', [Query::on('$id', 'mainId')]);
            $baseline = $database->find($mCol, [$join]);
            $this->assertSame(1, \count($baseline));
            $this->assertContains(10, $this->numericScores($baseline));
            $this->assertSecretJoinPayloadHidden($baseline, 'j-secret', 999);

            $filtered = $database->find($mCol, [
                $join,
                Query::equal('rev.score', [999]),
            ]);
            $this->assertLessThanOrEqual(\count($baseline), \count($filtered));
            $this->assertSecretJoinPayloadHidden($filtered, 'j-secret', 999);

            $ordered = $database->find($mCol, [
                $join,
                Query::orderDesc('rev.score'),
            ]);
            $this->assertSame(\count($baseline), \count($ordered));
            $this->assertSecretJoinPayloadHidden($ordered, 'j-secret', 999);

            if (! $database->getAdapter()->supports(Capability::Aggregations)) {
                return;
            }

            $aggregated = $database->aggregate($mCol, [
                $join,
                Query::max('rev.score', 'max_score'),
                Query::groupBy(['name']),
                Query::having([Query::greaterThanEqual('max_score', 999)]),
            ]);
            $this->assertLessThanOrEqual(\count($baseline), \count($aggregated));
            $this->assertSecretJoinPayloadHidden(self::rowDocuments($aggregated), 'j-secret', 999);

            $maxOnly = $database->aggregate($mCol, [
                $join,
                Query::max('rev.score', 'max_score'),
                Query::groupBy(['name']),
            ]);
            $this->assertSecretJoinPayloadHidden(self::rowDocuments($maxOnly), 'j-secret', 999);
            foreach ($maxOnly as $document) {
                $maxScore = $document['max_score'];
                if (\is_numeric($maxScore)) {
                    $this->assertNotSame(999, (int) $maxScore);
                }
            }
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinExactCountHidesSecretSiblingOnSameDocument(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_exc_m';
        $jCol = 'jp_exc_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Matched',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($mCol, new Document([
            '$id' => 'm2',
            'name' => 'Unmatched',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('jp-acl'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $inner = $database->find($mCol, [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(1, \count($inner));
            $this->assertSame('m1', $inner[0]->getId());
            $this->assertContains(10, $this->numericScores($inner));
            $this->assertSecretJoinPayloadHidden($inner, 'j-secret', 999);

            $publicMains = $database->find($mCol);
            $this->assertSame(2, \count($publicMains));

            $left = $database->find($mCol, [
                Query::leftJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(\count($publicMains), \count($left));
            $this->assertSecretJoinPayloadHidden($left, 'j-secret', 999);
            foreach ($left as $document) {
                if ($document->getId() !== 'm1') {
                    $this->assertNullishScore($document);
                }
            }
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinMixedDocumentSecurityHidesSecretOnFindAndGetDocument(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_mds_m';
        $jCol = 'jp_mds_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createMixedJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-public',
            'mainId' => 'm1',
            'score' => 10,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('other'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $inner = $database->find($mCol, [
                Query::join($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(1, \count($inner));
            $this->assertSame('m1', $inner[0]->getId());
            $this->assertContains(10, $this->numericScores($inner));
            $this->assertSecretJoinPayloadHidden($inner, 'j-secret', 999, 'user:other');

            $left = $database->find($mCol, [
                Query::leftJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(1, \count($left));
            $this->assertSame('m1', $left[0]->getId());
            $this->assertSecretJoinPayloadHidden($left, 'j-secret', 999, 'user:other');

            $document = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(false, $document->isEmpty());
            $this->assertSame('m1', $document->getId());
            $this->assertSecretJoinHidden($document, 'j-secret', 999, 'user:other');
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinThreeTableDeniesUnauthorizedCollection(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $aCol = 'jp_3d_a';
        $bCol = 'jp_3d_b';
        $cCol = 'jp_3d_c';
        $cols = [$aCol, $bCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $aCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($aCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $bCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($bCol, Attribute::string(key: 'aId', required: true));

        $database->createCollection(Collection::create(id: $cCol, permissions: [Permission::create(Role::any())], documentSecurity: false));
        $database->createAttribute($cCol, Attribute::string(key: 'bId', required: true));
        $database->createAttribute($cCol, Attribute::string(key: 'secret', size: 100, required: true));

        $database->createDocument($aCol, new Document([
            '$id' => 'a1',
            'name' => 'Alpha',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($bCol, new Document([
            '$id' => 'b1',
            'aId' => 'a1',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($cCol, new Document([
            '$id' => 'c-secret',
            'bId' => 'b1',
            'secret' => 'c-secret-token',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $aCol, $bCol, $cCol): void {
            try {
                $results = $database->find($aCol, [
                    Query::join($bCol, 'b', [Query::on('$id', 'aId')]),
                    Query::join($cCol, 'c', [Query::on('b.$id', 'bId')]),
                ]);
                foreach ($results as $document) {
                    $encoded = \json_encode($document);
                    $this->assertNotFalse($encoded);
                    $this->assertSame(false, \str_contains($encoded, 'c-secret-token'));
                    $this->assertSame(false, \str_contains($encoded, 'c-secret'));
                    $this->assertNotSame('c-secret-token', $document->getAttribute('secret'));
                }
                $this->fail('Join A→B→C must reject unauthorized collection C');
            } catch (AuthorizationException $exception) {
                $this->assertSame(true, \str_contains($exception->getMessage(), 'Unauthorized access to joined collection'));
                $this->assertSame(true, \str_contains($exception->getMessage(), $cCol));
            }
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testGetDocumentJoinSkipAuthDoesNotRevealSecret(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jp_gds_m';
        $jCol = 'jp_gds_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $this->createMixedJoinPermissionCollections($database, $mCol, $jCol);

        $database->createDocument($mCol, new Document([
            '$id' => 'm1',
            'name' => 'Main',
            '$permissions' => [Permission::read(Role::any())],
        ]));
        $database->createDocument($jCol, new Document([
            '$id' => 'j-secret',
            'mainId' => 'm1',
            'score' => 999,
            '$permissions' => [Permission::read(Role::user('other'))],
        ]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $document = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($jCol, 'j0', [Query::on('$id', 'mainId')]),
            ]);
            $this->assertSame(false, $document->isEmpty());
            $this->assertSame('m1', $document->getId());
            $this->assertSecretJoinHidden($document, 'j-secret', 999, 'user:other');
            $this->assertNullishScore($document);
        });

        $this->cleanupAggCollections($database, $cols);
    }

    /**
     * @param list<string> $roles
     * @param callable(): void $callback
     */
    private function withAuthorizationRoles(Database $database, array $roles, callable $callback): void
    {
        $authorization = $database->getAuthorization();
        $previousRoles = $authorization->getRoles();
        $authorization->cleanRoles();
        foreach ($roles as $role) {
            $authorization->addRole($role);
        }

        try {
            $callback();
        } finally {
            $authorization->cleanRoles();
            foreach ($previousRoles as $role) {
                $authorization->addRole($role);
            }
        }
    }

    /**
     * The joined collection grants no collection-level read, so its rows are
     * filtered per document exactly as a direct list would filter them.
     */
    private function createJoinPermissionCollections(Database $database, string $main, string $joined, bool $mainGranted = true): void
    {
        $granted = [Permission::create(Role::any()), Permission::read(Role::any())];
        $documentLevel = [Permission::create(Role::any())];

        $database->createCollection(Collection::create(id: $main, permissions: $mainGranted ? $granted : $documentLevel));
        $database->createAttribute($main, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $joined, permissions: $documentLevel));
        $database->createAttribute($joined, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($joined, Attribute::integer(key: 'score', required: true));
    }

    private function createMixedJoinPermissionCollections(Database $database, string $main, string $joined): void
    {
        $database->createCollection(Collection::create(id: $main, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($main, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $joined, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($joined, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($joined, Attribute::integer(key: 'score', required: true));
    }

    /**
     * @param array<Document> $documents
     */
    private function assertSecretJoinPayloadHidden(array $documents, string $secretId, int $secretScore, string $forbiddenRole = 'user:jp-acl'): void
    {
        $payload = [];
        foreach ($documents as $document) {
            $this->assertSecretJoinHidden($document, $secretId, $secretScore, $forbiddenRole);
            $payload[] = $document->getArrayCopy();
        }

        $this->assertEncodedJoinSecretHidden(\json_encode($payload), $secretId, $secretScore, $forbiddenRole);
    }

    private function assertSecretJoinHidden(Document $document, string $secretId, int $secretScore, string $forbiddenRole = 'user:jp-acl'): void
    {
        $this->assertNotSame($secretId, $document->getId());

        foreach ([...$this->joinedValues($document, 'score'), ...$this->joinedValues($document, 'amount')] as $value) {
            if (\is_numeric($value)) {
                $this->assertNotSame($secretScore, (int) $value);
            }
        }

        foreach ($document->getPermissions() as $permission) {
            $this->assertSame(false, \str_contains($permission, $secretId));
            $this->assertSame(false, \str_contains($permission, $forbiddenRole));
        }

        $this->assertEncodedJoinSecretHidden(\json_encode($document), $secretId, $secretScore, $forbiddenRole);
    }

    private function assertEncodedJoinSecretHidden(string|false $encoded, string $secretId, int $secretScore, string $forbiddenRole): void
    {
        $this->assertNotFalse($encoded);
        $this->assertSame(false, \str_contains($encoded, $secretId));
        $this->assertSame(false, \str_contains($encoded, $forbiddenRole));
        $this->assertSame(false, $this->encodedJsonContainsScalar($encoded, $secretScore));
    }

    private function encodedJsonContainsScalar(string $encoded, int $needle): bool
    {
        $decoded = \json_decode($encoded, true);
        if (! \is_array($decoded)) {
            return false;
        }

        return $this->jsonContainsScalar($decoded, $needle);
    }

    private function jsonContainsScalar(mixed $value, int $needle, string|int|null $key = null): bool
    {
        if (\is_int($value) || \is_float($value) || (\is_string($value) && \is_numeric($value))) {
            if ($this->isIgnoredJoinSecretKey($key)) {
                return false;
            }

            return (int) $value === $needle;
        }

        if (! \is_array($value)) {
            return false;
        }

        foreach ($value as $childKey => $child) {
            if ($this->jsonContainsScalar($child, $needle, $childKey)) {
                return true;
            }
        }

        return false;
    }

    private function isIgnoredJoinSecretKey(string|int|null $key): bool
    {
        return \in_array($key, [
            Document::SEQUENCE,
            Document::CREATED_AT,
            Document::UPDATED_AT,
            Document::TENANT,
            Document::COLLECTION,
            Document::DISTANCE,
            Document::DELETED_AT,
        ], true);
    }

    private function assertJoinAttributesAbsent(Document $document): void
    {
        $score = $document->getAttribute('score');
        $this->assertTrue($score === null || $score === '');
        $amount = $document->getAttribute('amount');
        $this->assertTrue($amount === null || $amount === '');
    }

    private function assertNullishScore(Document $document): void
    {
        $scores = $this->joinedValues($document, 'score');
        $this->assertNotSame([], $scores);
        foreach ($scores as $score) {
            $this->assertTrue($score === null || $score === '');
        }
    }

    /**
     * @param array<Document> $documents
     * @return list<int>
     */
    private function numericScores(array $documents): array
    {
        $scores = [];
        foreach ($documents as $document) {
            foreach ($this->joinedValues($document, 'score') as $score) {
                if (\is_numeric($score)) {
                    $scores[] = (int) $score;
                }
            }
        }

        return $scores;
    }

    /**
     * An attribute's values under its bare name and under every join alias.
     *
     * @return list<mixed>
     */
    private function joinedValues(Document $document, string $attribute): array
    {
        $values = [];
        foreach ($document->getArrayCopy() as $key => $value) {
            if ($key === $attribute || \str_ends_with((string) $key, '.'.$attribute)) {
                $values[] = $value;
            }
        }

        return $values;
    }

    /**
     * @param array<Document> $documents
     * @return list<int>
     */
    private function aliasedScores(array $documents): array
    {
        $scores = [];
        foreach ($documents as $document) {
            $score = $document->getAttribute('rev.score') ?? $document->getAttribute('score');
            if (\is_numeric($score)) {
                $scores[] = (int) $score;
            }
        }

        return $scores;
    }

    public function testLeftJoinOnFilterKeepsUnmatchedMainRows(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $pCol = 'ljon_p';
        $rCol = 'ljon_r';
        $cols = [$pCol, $rCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        foreach (['p1' => 'Alpha', 'p2' => 'Beta', 'p3' => 'Gamma'] as $id => $name) {
            $database->createDocument($pCol, new Document([
                '$id' => $id,
                'name' => $name,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        foreach ([
            ['prod_uid' => 'p1', 'score' => 5],
            ['prod_uid' => 'p2', 'score' => 2],
        ] as $review) {
            $database->createDocument($rCol, new Document(array_merge($review, [
                '$permissions' => [Permission::read(Role::any())],
            ])));
        }

        $results = $database->find($pCol, [
            Query::leftJoin($rCol, 'rev', [
                Query::on('$id', 'prod_uid'),
                Query::greaterThanEqual('rev.score', 4),
            ]),
            Query::select(['name', 'rev.score']),
        ]);

        $this->assertCount(3, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $name = $doc->getAttribute('name');
            $this->assertIsString($name);
            $mapped[$name] = $doc->getAttribute('rev.score');
        }
        $this->assertEquals(5, $mapped['Alpha']);
        $this->assertTrue($mapped['Beta'] === null || $mapped['Beta'] === '');
        $this->assertTrue($mapped['Gamma'] === null || $mapped['Gamma'] === '');

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinAliasesNeverCollide(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $first, $second] = $this->createAliasCollections($database);

        $declaredFirst = $database->find($main, [
            Query::join($first, 'j1', [Query::on('$id', 'mainId')]),
            Query::join($second, 'j2', [Query::on('$id', 'mainId')]),
            Query::select(['name', 'j1.score']),
        ]);
        $this->assertCount(1, $declaredFirst);
        $this->assertSame(1, $this->scoreOf($declaredFirst[0], 'j1.score'));

        $declaredLater = $database->find($main, [
            Query::join($first, 'j1', [Query::on('$id', 'mainId')]),
            Query::join($second, 'j0', [Query::on('$id', 'mainId')]),
            Query::select(['name', 'j0.score']),
        ]);
        $this->assertCount(1, $declaredLater);
        $this->assertSame(10, $this->scoreOf($declaredLater[0], 'j0.score'));

        foreach ([
            'the same alias twice' => [
                Query::join($first, 'x', [Query::on('$id', 'mainId')]),
                Query::join($second, 'x', [Query::on('$id', 'mainId')]),
            ],
            'the main collection alias' => [
                Query::join($first, Query::DEFAULT_ALIAS, [Query::on('$id', 'mainId')]),
            ],
        ] as $label => $joins) {
            $this->assertJoinQueryRejected(fn () => $database->find($main, $joins), "find with {$label}");
            $this->assertJoinQueryRejected(fn () => $database->count($main, $joins), "count with {$label}");
        }

        $this->cleanupAggCollections($database, [$main, $first, $second]);
    }

    public function testJoinWithoutSelectReturnsJoinedAttributesUnderTheAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $first] = $this->createAliasCollections($database);
        $plain = \array_keys($database->getDocument($main, 'm1')->getArrayCopy());
        $expected = [...$plain, 'ord.$id', 'ord.mainId', 'ord.score', 'ord.secret'];
        \sort($expected);

        foreach ([
            'find' => $database->find($main, [Query::join($first, 'ord', [Query::on('$id', 'mainId')])]),
            'getDocument' => [$database->getDocument($main, 'm1', [Query::leftJoin($first, 'ord', [Query::on('$id', 'mainId')])])],
        ] as $label => $rows) {
            $this->assertCount(1, $rows, $label);
            $keys = \array_keys($rows[0]->getArrayCopy());
            \sort($keys);
            $this->assertSame($expected, $keys, $label);
            $this->assertSame('m1', $rows[0]->getId(), $label);
            $this->assertSame('b1', $rows[0]->getAttribute('ord.$id'), $label);
            $this->assertSame(1, $this->scoreOf($rows[0], 'ord.score'), $label);
            $this->assertSame('first-secret', $rows[0]->getAttribute('ord.secret'), $label);
        }

        $this->cleanupAggCollections($database, $this->aliasCollections());
    }

    public function testJoinedValuesDecodeLikeADirectRead(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $database->addFilter(
            'joinedSeal',
            static fn (mixed $value): mixed => \is_string($value) ? \json_encode(['data' => \base64_encode($value), 'method' => 'base64']) : $value,
            static function (mixed $value): mixed {
                $payload = \is_string($value) ? \json_decode($value, true) : null;
                if (! \is_array($payload) || ! \is_string($payload['data'] ?? null)) {
                    return $value;
                }

                return \base64_decode($payload['data'], true);
            },
        );

        $main = 'jdec_main';
        $joined = 'jdec_joined';
        $this->cleanupAggCollections($database, [$main, $joined]);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $main, permissions: $permissions, documentSecurity: false));
        $database->createAttribute($main, Attribute::string(key: 'name', size: 64, required: true));
        $database->createCollection(Collection::create(id: $joined, permissions: $permissions, documentSecurity: false));
        $database->createAttributes($joined, [
            Attribute::string(key: 'mainId', size: 64, required: true),
            Attribute::integer(key: 'total', required: true),
            Attribute::float(key: 'price', required: true),
            Attribute::boolean(key: 'paid', required: true),
            Attribute::datetime(key: 'placedAt', required: true),
            Attribute::string(key: 'tags', size: 32, array: true),
            Attribute::string(key: 'meta', size: 1024, filters: [Filter::Json]),
            Attribute::string(key: 'secret', size: 1024, filters: ['joinedSeal']),
        ]);

        $placedAt = ['m1' => '2024-05-06T07:08:09.123+00:00', 'm2' => '2024-05-06T08:00:00.000+00:00', 'm3' => '2024-05-06T09:00:00.000+00:00'];
        foreach ($placedAt as $id => $at) {
            $database->createDocument($main, new Document(['$id' => $id, 'name' => $id, '$permissions' => [Permission::read(Role::any())]]));
            $database->createDocument($joined, new Document([
                '$id' => 'j'.$id,
                'mainId' => $id,
                'total' => 10,
                'price' => 2.5,
                'paid' => true,
                'placedAt' => $at,
                'tags' => ['a', 'b'],
                'meta' => ['color' => 'red'],
                'secret' => 'plain-secret',
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $attributes = ['mainId', 'total', 'price', 'paid', 'placedAt', 'tags', 'meta', 'secret'];
        $direct = $database->getDocument($joined, 'jm1');
        $join = Query::join($joined, 'dec', [Query::on('$id', 'mainId')]);
        $selected = Query::select(['name', ...\array_map(static fn (string $attribute): string => 'dec.'.$attribute, $attributes)]);

        foreach ([
            'find' => $database->find($main, [$join, Query::equal('$id', ['m1'])]),
            'find with a select' => $database->find($main, [$join, $selected, Query::equal('$id', ['m1'])]),
            'getDocument' => [$database->getDocument($main, 'm1', [$join])],
            'getDocument with a select' => [$database->getDocument($main, 'm1', [$join, $selected])],
        ] as $label => $rows) {
            $this->assertCount(1, $rows, $label);
            $row = $rows[0];
            foreach ($attributes as $attribute) {
                $this->assertSame($direct->getAttribute($attribute), $row->getAttribute('dec.'.$attribute), "{$label}: dec.{$attribute}");
            }
            $this->assertSame(10, $row->getAttribute('dec.total'), $label);
            $this->assertSame(2.5, $row->getAttribute('dec.price'), $label);
            $this->assertTrue($row->getAttribute('dec.paid'), $label);
            $this->assertSame(['a', 'b'], $row->getAttribute('dec.tags'), $label);
            $this->assertSame(['color' => 'red'], $row->getAttribute('dec.meta'), $label);
            $this->assertSame('plain-secret', $row->getAttribute('dec.secret'), $label);
        }

        $page = [$join, Query::orderAsc('dec.placedAt'), Query::limit(1)];
        $ids = [];
        $cursor = null;
        for ($attempt = 0; $attempt < 4; $attempt++) {
            $rows = $database->find($main, $cursor === null ? $page : [...$page, Query::cursorAfter($cursor)]);
            if ($rows === []) {
                break;
            }
            $ids[] = $rows[0]->getId();
            $cursor = $rows[0];
        }
        $this->assertSame(['m1', 'm2', 'm3'], $ids, 'A cursor carrying decoded joined values pages by them');

        $this->cleanupAggCollections($database, [$main, $joined]);
    }

    public function testFullOuterJoinThenRightJoinReturnsUnmatchedRowsOnce(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $first, $second] = $this->createAliasCollections($database);
        $database->createDocument($second, new Document([
            '$id' => 'c3',
            'mainId' => 'zz',
            'score' => 30,
            '$permissions' => [Permission::read(Role::any())],
        ]));

        foreach ([
            'on the main collection' => [
                Query::fullOuterJoin($first, 'b', [Query::on('$id', 'mainId')]),
                Query::rightJoin($second, 'c', [Query::on('$id', 'mainId')]),
            ],
            'on the full outer joined collection' => [
                Query::fullOuterJoin($first, 'b', [Query::on('$id', 'mainId')]),
                Query::rightJoin($second, 'c', [Query::on('b.mainId', 'mainId')]),
            ],
        ] as $label => $joins) {
            $rows = $database->find($main, [...$joins, Query::select(['name', 'b.score', 'c.score'])]);
            $values = \array_map(
                fn (Document $row): string => (string) \json_encode([
                    $row->getAttribute('name'),
                    $this->scoreOf($row, 'b.score'),
                    $this->scoreOf($row, 'c.score'),
                ]),
                $rows,
            );
            \sort($values);

            $this->assertSame(['["m1",1,10]', '[null,null,30]'], $values, $label);
            $this->assertSame(2, $database->count($main, $joins), $label);
            $this->assertEquals(40, $database->sum($main, 'c.score', $joins), $label);
        }

        $this->cleanupAggCollections($database, $this->aliasCollections());
    }

    public function testRightJoinOnAnUnmatchedFullOuterJoinRowIsPairedNotDuplicated(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $first, $second] = $this->createAliasCollections($database);
        $database->createDocument($first, new Document([
            '$id' => 'b2',
            'mainId' => 'zz',
            'score' => 2,
            '$permissions' => [Permission::read(Role::any())],
        ]));
        foreach (['c2' => 'zz', 'c3' => 'nobody'] as $id => $mainId) {
            $database->createDocument($second, new Document([
                '$id' => $id,
                'mainId' => $mainId,
                'score' => 20,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $joins = [
            Query::fullOuterJoin($first, 'b', [Query::on('$id', 'mainId')]),
            Query::rightJoin($second, 'c', [Query::on('b.mainId', 'mainId')]),
        ];
        $rows = $database->find($main, [...$joins, Query::select(['$id', 'b.$id', 'c.$id'])]);
        $values = \array_map(
            static fn (Document $row): string => (string) \json_encode([
                $row->getId() !== '' ? $row->getId() : null,
                $row->getAttribute('b.$id'),
                $row->getAttribute('c.$id'),
            ]),
            $rows,
        );
        \sort($values);

        $this->assertSame(['["m1","b1","c1"]', '[null,"b2","c2"]', '[null,null,"c3"]'], $values);
        $this->assertSame(3, $database->count($main, $joins));

        $this->cleanupAggCollections($database, $this->aliasCollections());
    }

    public function testTwoFullOuterJoinsRunOnlyWhereTheEngineHasThem(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $first, $second] = $this->createAliasCollections($database);
        $joins = [
            Query::fullOuterJoin($first, 'b', [Query::on('$id', 'mainId')]),
            Query::fullOuterJoin($second, 'c', [Query::on('b.mainId', 'mainId')]),
        ];

        if ($database->getAdapter() instanceof Postgres) {
            $rows = $database->find($main, [...$joins, Query::select(['$id', 'b.$id', 'c.$id'])]);
            $this->assertCount(1, $rows);
            $this->assertSame('m1', $rows[0]->getId());
            $this->assertSame('b1', $rows[0]->getAttribute('b.$id'));
            $this->assertSame('c1', $rows[0]->getAttribute('c.$id'));
            $this->assertSame(1, $database->count($main, $joins));
        } else {
            $message = $this->assertJoinQueryRejected(fn () => $database->find($main, $joins), 'find');
            $this->assertSame('A query can hold only one full outer join on this database', $message);
            $this->assertJoinQueryRejected(fn () => $database->count($main, $joins), 'count');
        }

        $this->cleanupAggCollections($database, $this->aliasCollections());
    }

    public function testVectorSearchPagesByAJoinedAttributeWithACursor(): void
    {
        $database = static::getDatabase();
        $adapter = $database->getAdapter();
        if (! $adapter->supports(Capability::Joins) || ! $adapter->supports(Capability::Vectors)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $main = 'vjc_main';
        $meta = 'vjc_meta';
        $this->cleanupAggCollections($database, [$main, $meta]);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $main, permissions: $permissions));
        $database->createAttribute($main, Attribute::vector(key: 'embedding', dimensions: 3, required: true));
        $database->createCollection(Collection::create(id: $meta, permissions: $permissions));
        $database->createAttribute($meta, Attribute::string(key: 'mainId', size: 64, required: true));
        $database->createAttribute($meta, Attribute::integer(key: 'score', required: true));

        foreach ([
            'near-high' => [[1.0, 0.0, 0.0], 20],
            'near-low' => [[1.0, 0.0, 0.0], 10],
            'side-low' => [[0.0, 1.0, 0.0], 5],
            'side-high' => [[0.0, 1.0, 0.0], 50],
            'far' => [[-1.0, 0.0, 0.0], 1],
        ] as $id => [$embedding, $score]) {
            $database->createDocument($main, new Document([
                '$id' => $id,
                'embedding' => $embedding,
                '$permissions' => [Permission::read(Role::any())],
            ]));
            $database->createDocument($meta, new Document([
                'mainId' => $id,
                'score' => $score,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $queries = [
            Query::vectorCosine('embedding', [1.0, 0.0, 0.0]),
            Query::join($meta, 'meta', [Query::on('$id', 'mainId')]),
            Query::orderAsc('meta.score'),
            Query::limit(2),
        ];

        $ids = [];
        $cursor = null;
        for ($page = 0; $page < 4; $page++) {
            $rows = $database->find($main, $cursor === null ? $queries : [...$queries, Query::cursorAfter($cursor)]);
            if ($rows === []) {
                break;
            }
            foreach ($rows as $row) {
                $ids[] = $row->getId();
            }
            $cursor = $rows[\count($rows) - 1];
        }

        $this->assertSame(['near-low', 'near-high', 'side-low', 'side-high', 'far'], $ids);

        $this->cleanupAggCollections($database, [$main, $meta]);
    }

    public function testFullOuterJoinAggregatesCountEveryRowOnce(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->createFullOuterJoinAggregateCollections($database);
        $join = Query::fullOuterJoin($joined, 'b', [Query::on('link', 'link')]);

        $rows = $database->aggregate($main, [
            $join,
            Query::count('*', 'rows'),
            Query::count('b.$id', 'joined'),
            Query::countDistinct('b.category', 'categories'),
            Query::sum('b.score', 'total'),
            Query::avg('b.score', 'mean'),
            Query::min('b.score', 'low'),
            Query::max('b.score', 'high'),
            Query::sum('score', 'mainTotal'),
        ]);

        $this->assertCount(1, $rows);
        $expected = ['rows' => 6, 'joined' => 4, 'categories' => 2, 'total' => 22, 'low' => 4, 'high' => 7, 'mainTotal' => 70];
        foreach ($expected as $key => $value) {
            $this->assertSame($value, $this->intAttribute($rows[0], $key), $key);
        }
        $this->assertEqualsWithDelta(5.5, $this->numericAttribute($rows[0], 'mean'), 0.001);

        $empty = $database->aggregate($main, [
            $join,
            Query::equal('category', ['none']),
            Query::count('*', 'rows'),
            Query::sum('b.score', 'total'),
            Query::max('b.score', 'high'),
        ]);

        $this->assertCount(1, $empty);
        $this->assertSame(0, $this->intAttribute($empty[0], 'rows'));
        $this->assertNull($empty[0]['total']);
        $this->assertNull($empty[0]['high']);

        $this->cleanupAggCollections($database, $this->fullOuterJoinAggregateCollections());
    }

    public function testFullOuterJoinGroupsHavingAndPagesCountEveryRowOnce(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->createFullOuterJoinAggregateCollections($database);
        $grouped = [
            Query::fullOuterJoin($joined, 'b', [Query::on('link', 'link')]),
            Query::groupBy(['b.category']),
            Query::count('*', 'rows'),
            Query::sum('b.score', 'total'),
            Query::sum('score', 'mainTotal'),
            Query::orderDesc('rows'),
        ];
        $this->assertSame(
            [[null, 3, 6, 50], ['p', 2, 9, 10], ['q', 1, 7, 10]],
            $this->fullOuterJoinGroups($database->aggregate($main, $grouped)),
        );
        $this->assertSame(
            [[null, 3, 6, 50], ['p', 2, 9, 10]],
            $this->fullOuterJoinGroups($database->aggregate($main, [...$grouped, Query::having([Query::greaterThan('rows', 1)])])),
        );
        $this->assertSame(
            [['p', 2, 9, 10], ['q', 1, 7, 10]],
            $this->fullOuterJoinGroups($database->aggregate($main, [...$grouped, Query::limit(2), Query::offset(1)])),
        );

        $this->cleanupAggCollections($database, $this->fullOuterJoinAggregateCollections());
    }

    public function testFullOuterJoinDistinctReturnsAValueBothSidesHoldOnce(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->createFullOuterJoinAggregateCollections($database);
        $join = Query::fullOuterJoin($joined, 'b', [Query::on('link', 'link')]);

        $all = $this->joinedCategories($database->find($main, [$join, Query::distinct(), Query::select(['b.category'])]));
        \sort($all);
        $this->assertSame([null, 'p', 'q'], $all);

        $this->assertSame(['q'], $this->joinedCategories($database->find($main, [
            $join,
            Query::isNotNull('b.category'),
            Query::distinct(),
            Query::select(['b.category']),
            Query::orderAsc('b.category'),
            Query::limit(1),
            Query::offset(1),
        ])));

        $this->cleanupAggCollections($database, $this->fullOuterJoinAggregateCollections());
    }

    public function testFullOuterJoinDistinctOrderedByAnUnselectedAttributeIsRejectedWhereEmulated(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || $database->getAdapter() instanceof Postgres) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->createFullOuterJoinAggregateCollections($database);

        $message = $this->assertJoinQueryRejected(fn () => $database->find($main, [
            Query::fullOuterJoin($joined, 'b', [Query::on('link', 'link')]),
            Query::distinct(),
            Query::select(['b.category']),
            Query::orderAsc('score'),
        ]), 'find');
        $this->assertSame('A distinct() query over a full outer join can only be ordered by a selected attribute on this database, and score is not selected', $message);

        $this->cleanupAggCollections($database, $this->fullOuterJoinAggregateCollections());
    }

    public function testFullOuterJoinUnaliasedAggregatesComeBackUnderTheirDefaultNames(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->createFullOuterJoinAggregateCollections($database);
        $aggregates = [Query::count(), Query::sum('b.score'), Query::max('score')];

        $left = $database->aggregate($main, [Query::leftJoin($joined, 'b', [Query::on('link', 'link')]), ...$aggregates]);
        $full = $database->aggregate($main, [Query::fullOuterJoin($joined, 'b', [Query::on('link', 'link')]), ...$aggregates]);

        $this->assertCount(1, $left);
        $this->assertCount(1, $full);
        $this->assertSame(['count', 'sum_b_score', 'max_score'], \array_keys($left[0]));
        $this->assertSame(\array_keys($left[0]), \array_keys($full[0]));
        $this->assertSame(
            [6, 22, 30],
            \array_map(static fn (mixed $value): int => \is_numeric($value) ? (int) $value : -1, \array_values($full[0])),
        );

        $this->cleanupAggCollections($database, $this->fullOuterJoinAggregateCollections());
    }

    /**
     * @param  list<array<string, mixed>>  $rows
     * @return list<array{mixed, int, int, int}>
     */
    private function fullOuterJoinGroups(array $rows): array
    {
        return \array_map(
            fn (array $row): array => [
                $row['category'] ?? null,
                $this->intAttribute($row, 'rows'),
                $this->intAttribute($row, 'total'),
                $this->intAttribute($row, 'mainTotal'),
            ],
            $rows,
        );
    }

    /**
     * @param  array<Document>  $rows
     * @return list<mixed>
     */
    private function joinedCategories(array $rows): array
    {
        return \array_values(\array_map(static fn (Document $row): mixed => $row->getAttribute('b.category'), $rows));
    }

    /**
     * @return list<string>
     */
    private function fullOuterJoinAggregateCollections(): array
    {
        return ['foja_main', 'foja_joined'];
    }

    /**
     * m1 matches b1 and b4, m2 and m3 match nothing and nothing matches b2 and b3, so an emulated full
     * outer join returns rows from both halves, and equal categories (null among them) from both.
     *
     * @return list<string>
     */
    private function createFullOuterJoinAggregateCollections(Database $database): array
    {
        $collections = $this->fullOuterJoinAggregateCollections();
        [$main, $joined] = $collections;
        $this->cleanupAggCollections($database, $collections);

        $rows = [
            $main => ['m1' => ['1', 'p', 10], 'm2' => ['2', 'q', 20], 'm3' => ['5', 'p', 30]],
            $joined => ['b1' => ['1', 'p', 4], 'b2' => ['3', 'p', 5], 'b3' => ['6', null, 6], 'b4' => ['1', 'q', 7]],
        ];
        foreach ($rows as $collection => $documents) {
            $database->createCollection(Collection::create(
                id: $collection,
                attributes: [
                    Attribute::string(key: 'link', size: 16, required: true),
                    Attribute::string(key: 'category', size: 16, required: false),
                    Attribute::integer(key: 'score', required: true),
                ],
                permissions: $collection === $main
                    ? [Permission::create(Role::any())]
                    : [Permission::create(Role::any()), Permission::read(Role::any())],
                documentSecurity: true,
            ));

            foreach ($documents as $id => [$link, $category, $score]) {
                $database->createDocument($collection, new Document([
                    '$id' => $id,
                    'link' => $link,
                    'category' => $category,
                    'score' => $score,
                    '$permissions' => [Permission::read(Role::any())],
                ]));
            }
        }

        return $collections;
    }

    /**
     * @return list<string>
     */
    private function aliasCollections(): array
    {
        return ['jal_main', 'jal_first', 'jal_second'];
    }

    /**
     * @return list<string>
     */
    private function createAliasCollections(Database $database): array
    {
        $collections = $this->aliasCollections();
        [$main, $first, $second] = $collections;
        $this->cleanupAggCollections($database, $collections);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $main, permissions: $permissions, documentSecurity: false));
        $database->createAttribute($main, Attribute::string(key: 'name', size: 64, required: true));
        foreach ([$first, $second] as $joined) {
            $database->createCollection(Collection::create(id: $joined, permissions: $permissions, documentSecurity: false));
            $database->createAttribute($joined, Attribute::string(key: 'mainId', size: 64, required: true));
            $database->createAttribute($joined, Attribute::integer(key: 'score', required: true));
            $database->createAttribute($joined, Attribute::string(key: 'secret', size: 64, required: false));
        }

        $database->createDocument($main, new Document(['$id' => 'm1', 'name' => 'm1', '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($first, new Document(['$id' => 'b1', 'mainId' => 'm1', 'score' => 1, 'secret' => 'first-secret', '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($second, new Document(['$id' => 'c1', 'mainId' => 'm1', 'score' => 10, 'secret' => 'second-secret', '$permissions' => [Permission::read(Role::any())]]));

        return $collections;
    }

    private function scoreOf(Document $document, string $key): ?int
    {
        $score = $document->getAttribute($key);

        return \is_numeric($score) ? (int) $score : null;
    }

    /**
     * @param  callable(): mixed  $query
     */
    private function assertJoinQueryRejected(callable $query, string $label): string
    {
        try {
            $query();
        } catch (QueryException $exception) {
            return $exception->getMessage();
        }

        $this->fail("Accepted {$label}");
    }

    public function testJoinBareAggregateAttributeAmbiguousAcrossJoinsIsRejected(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, $orders, $refunds] = $collections = $this->seedJoinedAttributeCollections($database, 'jbaa');
        $joins = [
            Query::join($orders, 'alpha', [Query::on('$id', 'customerId')]),
            Query::join($refunds, 'beta', [Query::on('$id', 'customerId')]),
        ];

        foreach ([
            [Query::sum('amount', 'total')],
            [Query::count('*', 'rows'), Query::groupBy(['amount'])],
        ] as $aggregation) {
            try {
                $database->aggregate($customers, [...$joins, ...$aggregation]);
                $this->fail('A bare attribute two joins declare was bound to one of them');
            } catch (QueryException $error) {
                $this->assertSame('Invalid query: Attribute "amount" is ambiguous across joins; qualify it with a join alias', $error->getMessage());
            }
        }

        $results = $database->aggregate($customers, [
            ...$joins,
            Query::sum('alpha.amount', 'ordered'),
            Query::sum('beta.amount', 'refunded'),
        ]);

        $this->assertCount(1, $results);
        $this->assertSame(150, $this->intAttribute($results[0], 'ordered'));
        $this->assertSame(10, $this->intAttribute($results[0], 'refunded'));

        $this->cleanupAggCollections($database, $collections);
    }

    public function testJoinBareAggregateAttributeResolvesToTheDeclaringJoin(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, $orders, , $notes] = $collections = $this->seedJoinedAttributeCollections($database, 'jbar');

        $results = $database->aggregate($customers, [
            Query::join($notes, 'note', [Query::on('$id', 'customerId')]),
            Query::join($orders, 'purchase', [Query::on('$id', 'customerId')]),
            Query::sum('amount', 'total'),
        ]);
        $this->assertCount(1, $results);
        $this->assertSame(150, $this->intAttribute($results[0], 'total'));

        $results = $database->aggregate($customers, [
            Query::join($notes, 'j0', [Query::on('$id', 'customerId')]),
            Query::join($orders, 'j1', [Query::on('$id', 'customerId')]),
            Query::sum('amount', 'total'),
            Query::groupBy(['status']),
        ]);
        $totals = [];
        foreach ($results as $result) {
            $status = $result['status'];
            $this->assertIsString($status);
            $totals[$status] = $this->intAttribute($result, 'total');
        }
        \ksort($totals);
        $this->assertSame(['open' => 50, 'paid' => 100], $totals);

        $results = $database->aggregate($customers, [
            Query::leftJoin($notes, 'note', [Query::on('$id', 'customerId')]),
            Query::count('$id', 'customers'),
        ]);
        $this->assertCount(1, $results);
        $this->assertSame(2, $this->intAttribute($results[0], 'customers'));

        $this->cleanupAggCollections($database, $collections);
    }

    public function testJoinSearchOnJoinedAttributeRequiresFulltextIndex(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::IndexFulltext)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, $orders, , $notes] = $collections = $this->seedJoinedAttributeCollections($database, 'jsfi');
        $unindexed = [
            Query::join($orders, 'purchase', [Query::on('$id', 'customerId')]),
            Query::search('purchase.memo', 'gift'),
        ];

        foreach ([
            'find' => fn () => $database->find($customers, $unindexed),
            'count' => fn () => $database->count($customers, $unindexed),
        ] as $method => $read) {
            try {
                $read();
                $this->fail($method.'() searched a joined attribute without a fulltext index');
            } catch (QueryException $error) {
                $this->assertSame('Searching by attribute "purchase.memo" requires a fulltext index.', $error->getMessage(), $method);
            }
        }

        $results = $database->find($customers, [
            Query::join($notes, 'note', [Query::on('$id', 'customerId')]),
            Query::search('note.body', 'needle'),
            Query::select(['name']),
        ]);
        $this->assertSame(['first'], \array_map(static fn (Document $document): string => $document->getId(), $results));

        $this->cleanupAggCollections($database, $collections);
    }

    /**
     * @return array{0: string, 1: string, 2: string, 3: string}
     */
    private function seedJoinedAttributeCollections(Database $database, string $prefix): array
    {
        $collections = [$prefix.'_c', $prefix.'_o', $prefix.'_r', $prefix.'_n'];
        [$customers, $orders, $refunds, $notes] = $collections;
        $this->cleanupAggCollections($database, $collections);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $customers, permissions: $permissions));
        $database->createAttribute($customers, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(Collection::create(id: $orders, permissions: $permissions));
        $database->createAttribute($orders, Attribute::string(key: 'customerId', size: 64, required: true));
        $database->createAttribute($orders, Attribute::integer(key: 'amount', required: true));
        $database->createAttribute($orders, Attribute::string(key: 'status', size: 32, required: true));
        $database->createAttribute($orders, Attribute::string(key: 'memo', size: 256, required: true));

        $database->createCollection(Collection::create(id: $refunds, permissions: $permissions));
        $database->createAttribute($refunds, Attribute::string(key: 'customerId', size: 64, required: true));
        $database->createAttribute($refunds, Attribute::integer(key: 'amount', required: true));

        $database->createCollection(Collection::create(id: $notes, permissions: $permissions));
        $database->createAttribute($notes, Attribute::string(key: 'customerId', size: 64, required: true));
        $database->createAttribute($notes, Attribute::string(key: 'body', size: 256, required: true));
        if ($database->getAdapter()->supports(Capability::IndexFulltext)) {
            $database->createIndex($notes, Index::fulltext(key: 'body_fulltext', attributes: ['body']));
        }

        $rows = [
            [$customers, 'first', ['name' => 'First']],
            [$customers, 'second', ['name' => 'Second']],
            [$orders, 'paid', ['customerId' => 'first', 'amount' => 100, 'status' => 'paid', 'memo' => 'gift wrapped']],
            [$orders, 'open', ['customerId' => 'first', 'amount' => 50, 'status' => 'open', 'memo' => 'pending']],
            [$orders, 'other', ['customerId' => 'second', 'amount' => 7, 'status' => 'paid', 'memo' => 'plain']],
            [$refunds, 'refund', ['customerId' => 'first', 'amount' => 5]],
            [$notes, 'note', ['customerId' => 'first', 'body' => 'a needle in a haystack']],
        ];
        foreach ($rows as [$collection, $id, $attributes]) {
            $database->createDocument($collection, new Document([
                '$id' => $id,
                '$permissions' => [Permission::read(Role::any())],
                ...$attributes,
            ]));
        }

        return [$customers, $orders, $refunds, $notes];
    }

    public function testJoinParityKeepsMainRowsReadableThroughCollectionGrant(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jpar_grant_m';
        $jCol = 'jpar_grant_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $granted = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $mCol, permissions: $granted));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createAttribute($mCol, Attribute::integer(key: 'visits', required: true));
        $database->createCollection(Collection::create(id: $jCol, permissions: $granted));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::string(key: 'bio', size: 100, required: true));

        $database->createDocument($mCol, new Document(['$id' => 'open', 'name' => 'Open', 'visits' => 1, '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($mCol, new Document(['$id' => 'bare', 'name' => 'Bare', 'visits' => 10, '$permissions' => []]));
        $database->createDocument($jCol, new Document(['$id' => 'open-profile', 'mainId' => 'open', 'bio' => 'Hello', '$permissions' => [Permission::read(Role::any())]]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $join = Query::leftJoin($jCol, 'profile', [Query::on('$id', 'mainId')]);

            $this->assertSame(['bare', 'open'], $this->joinParityIds($database->find($mCol)));
            $this->assertSame(
                ['bare', 'open'],
                $this->joinParityIds($database->find($mCol, [$join, Query::select(['name', 'profile.bio'])])),
                'A left join is additive: it must not hide a row the collection grant makes readable',
            );
            $this->assertSame(2, $database->count($mCol));
            $this->assertSame(2, $database->count($mCol, [$join]));
            $this->assertSame(11, (int) $database->sum($mCol, 'visits'));
            $this->assertSame(11, (int) $database->sum($mCol, 'visits', [$join]));
            $this->assertSame('bare', $database->getDocument($mCol, 'bare')->getId());
            $this->assertSame('bare', $database->getDocument($mCol, 'bare', [$join])->getId());
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinParityShowsEveryRowOfAGrantedJoinedCollection(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jpar_all_m';
        $jCol = 'jpar_all_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $granted = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $mCol, permissions: $granted));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createCollection(Collection::create(id: $jCol, permissions: $granted));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::integer(key: 'amount', required: true));

        $database->createDocument($mCol, new Document(['$id' => 'customer', 'name' => 'Customer', '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($jCol, new Document(['$id' => 'public-order', 'mainId' => 'customer', 'amount' => 100, '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($jCol, new Document(['$id' => 'secret-order', 'mainId' => 'customer', 'amount' => 9999, '$permissions' => [Permission::read(Role::user('other'))]]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $join = Query::join($jCol, 'ord', [Query::on('$id', 'mainId')]);

            $this->assertSame([100, 9999], $this->joinParityIntegers($database->find($jCol), 'amount'));
            $this->assertSame(
                [100, 9999],
                $this->joinParityIntegers($database->find($mCol, [$join, Query::select(['name', 'ord.amount'])]), 'ord.amount'),
                'The collection grant makes every order readable directly, so the join must show every order',
            );
            $this->assertSame($database->count($jCol), $database->count($mCol, [$join]));
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinParityFiltersAJoinedCollectionPerDocumentWithoutGrant(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jpar_doc_m';
        $jCol = 'jpar_doc_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createCollection(Collection::create(id: $jCol, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::string(key: 'text', size: 100, required: true));

        $database->createDocument($mCol, new Document(['$id' => 'customer', 'name' => 'Customer', '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($jCol, new Document(['$id' => 'alice-note', 'mainId' => 'customer', 'text' => 'mine', '$permissions' => [Permission::read(Role::user('alice'))]]));
        $database->createDocument($jCol, new Document(['$id' => 'bob-note', 'mainId' => 'customer', 'text' => 'theirs', '$permissions' => [Permission::read(Role::user('bob'))]]));

        $this->withAuthorizationRoles($database, [Role::any()->toString(), Role::user('alice')->toString()], function () use ($database, $mCol, $jCol): void {
            $join = Query::join($jCol, 'note', [Query::on('$id', 'mainId')]);

            $this->assertSame(['mine'], $this->joinParityStrings($database->find($jCol), 'text'));
            $this->assertSame(
                ['mine'],
                $this->joinParityStrings($database->find($mCol, [$join, Query::select(['name', 'note.text'])]), 'note.text'),
                'Without a collection grant the joined rows are filtered per document, exactly like a direct list',
            );
            $this->assertSame($database->count($jCol), $database->count($mCol, [$join]));
            $this->assertSame(
                'mine',
                $database->getDocument($mCol, 'customer', [$join, Query::select(['name', 'note.text'])])->getAttribute('note.text'),
            );
        });

        $this->cleanupAggCollections($database, $cols);
    }

    public function testJoinParityRejectsAJoinedCollectionWithoutGrantOrDocumentSecurity(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $mCol = 'jpar_deny_m';
        $jCol = 'jpar_deny_j';
        $cols = [$mCol, $jCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(Collection::create(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createAttribute($mCol, Attribute::integer(key: 'visits', required: true));
        $database->createCollection(Collection::create(id: $jCol, permissions: [Permission::create(Role::any())], documentSecurity: false));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));

        $database->createDocument($mCol, new Document(['$id' => 'customer', 'name' => 'Customer', 'visits' => 1, '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($jCol, new Document(['$id' => 'entry', 'mainId' => 'customer', '$permissions' => [Permission::read(Role::any())]]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $join = Query::join($jCol, 'ledger', [Query::on('$id', 'mainId')]);
            $reads = [
                'direct find' => fn () => $database->find($jCol),
                'find' => fn () => $database->find($mCol, [$join]),
                'count' => fn () => $database->count($mCol, [$join]),
                'sum' => fn () => $database->sum($mCol, 'visits', [$join]),
                'getDocument' => fn () => $database->getDocument($mCol, 'customer', [$join]),
            ];

            foreach ($reads as $read => $callback) {
                try {
                    $callback();
                    $this->fail("{$read} must reject a collection readable neither at collection nor at document level");
                } catch (AuthorizationException $exception) {
                    $this->assertNotSame('', $exception->getMessage());
                }
            }
        });

        $this->cleanupAggCollections($database, $cols);
    }

    /**
     * @param array<Document> $documents
     * @return list<string>
     */
    private function joinParityIds(array $documents): array
    {
        $ids = \array_values(\array_unique(\array_map(static fn (Document $document): string => $document->getId(), $documents)));
        \sort($ids);

        return $ids;
    }

    /**
     * @param array<Document> $documents
     * @return list<int>
     */
    private function joinParityIntegers(array $documents, string $attribute): array
    {
        $values = [];
        foreach ($documents as $document) {
            $value = $document->getAttribute($attribute);
            if (\is_numeric($value)) {
                $values[] = (int) $value;
            }
        }
        \sort($values);

        return $values;
    }

    /**
     * @param array<Document> $documents
     * @return list<string>
     */
    private function joinParityStrings(array $documents, string $attribute): array
    {
        $values = [];
        foreach ($documents as $document) {
            $value = $document->getAttribute($attribute);
            if (\is_string($value)) {
                $values[] = $value;
            }
        }
        \sort($values);

        return $values;
    }

    public function testSharedTablesJoinsReadOnlyTheSelectedTenantsRows(): void
    {
        $database = static::getDatabase();
        if (! $database->hasSharedTables() || ! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collections = ['jtn_authors', 'jtn_books', 'jtn_reviews'];
        [$authors, $books] = $collections;
        $tenant = $database->getTenant();

        $rowsByJoin = [
            [Method::Join, [
                1 => [['one-a1', 11]],
                2 => [['two-a1', 21], ['two-a2', 22]],
            ]],
            [Method::LeftJoin, [
                1 => [['one-a1', 11], ['one-a2', null]],
                2 => [['two-a1', 21], ['two-a2', 22], ['two-shared', null]],
            ]],
            [Method::RightJoin, [
                1 => [['one-a1', 11], [null, 12], [null, 13]],
                2 => [['two-a1', 21], ['two-a2', 22]],
            ]],
            [Method::FullOuterJoin, [
                1 => [['one-a1', 11], ['one-a2', null], [null, 12], [null, 13]],
                2 => [['two-a1', 21], ['two-a2', 22], ['two-shared', null]],
            ]],
            [Method::CrossJoin, [
                1 => [['one-a1', 11], ['one-a1', 12], ['one-a1', 13], ['one-a2', 11], ['one-a2', 12], ['one-a2', 13]],
                2 => [['two-a1', 21], ['two-a1', 22], ['two-a2', 21], ['two-a2', 22], ['two-shared', 21], ['two-shared', 22]],
            ]],
        ];

        try {
            $this->seedJoinTenancyFixture($database, ...$collections);

            foreach ($rowsByJoin as [$method, $rowsByTenant]) {
                foreach ($rowsByTenant as $selected => $rows) {
                    $database->setTenant($selected);
                    $join = fn (): Query => $this->joinTenancyJoin($method, $books, 'book');

                    $this->assertSame(
                        $this->joinTenancySorted($rows),
                        $this->joinTenancyRows($database->find($authors, [$join(), Query::select(['name', 'book.pages'])]), ['book.pages']),
                        "Tenant {$selected} must read exactly its own rows through a {$method->value}",
                    );
                    $this->assertSame(
                        \count($rows),
                        $database->count($authors, [$join()]),
                        "Tenant {$selected} must count exactly its own rows through a {$method->value}",
                    );
                    $this->assertSame(
                        \array_sum(\array_map(static fn (array $row): int => $row[1] ?? 0, $rows)),
                        $database->sum($authors, 'book.pages', [$join()]),
                        "Tenant {$selected} must sum exactly its own rows through a {$method->value}",
                    );
                }

                $database->setTenant(1);
                $queries = fn (): array => [$this->joinTenancyJoin($method, $books, 'book'), Query::select(['name', 'book.pages'])];
                $this->assertSame('one-a1', $database->getDocument($authors, 'a1', $queries())->getAttribute('name'));
                $this->assertTrue(
                    $database->getDocument($authors, 'shared', $queries())->isEmpty(),
                    "Tenant 1 must not read tenant 2's document through a {$method->value}",
                );
                $this->assertTrue(
                    $database->getDocument($authors, 'legacy', $queries())->isEmpty(),
                    "Tenant 1 must not read a tenantless document through a {$method->value}",
                );
            }
        } finally {
            $database->setTenant(null);
            $this->cleanupAggCollections($database, $collections);
            $database->setTenant($tenant);
        }
    }

    /**
     * A join no index serves and a join an index serves, inner and left, read only the selected tenant's rows:
     * reviews are indexed by author, books are not.
     */
    public function testSharedTablesJoinsWithAndWithoutAnIndexReadOnlyTheSelectedTenantsRows(): void
    {
        $database = static::getDatabase();
        if (! $database->hasSharedTables() || ! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collections = ['jti_authors', 'jti_books', 'jti_reviews'];
        [$authors, $books, $reviews] = $collections;
        $tenant = $database->getTenant();

        $rowsByChain = [
            [Method::Join, Method::Join, [
                1 => [['one-a1', 11, 5]],
                2 => [],
            ]],
            [Method::LeftJoin, Method::LeftJoin, [
                1 => [['one-a1', 11, 5], ['one-a2', null, 4]],
                2 => [['two-a1', 21, null], ['two-a2', 22, null], ['two-shared', null, 1]],
            ]],
            [Method::Join, Method::LeftJoin, [
                1 => [['one-a1', 11, 5]],
                2 => [['two-a1', 21, null], ['two-a2', 22, null]],
            ]],
            [Method::LeftJoin, Method::Join, [
                1 => [['one-a1', 11, 5], ['one-a2', null, 4]],
                2 => [['two-shared', null, 1]],
            ]],
        ];

        try {
            $this->seedJoinTenancyFixture($database, ...$collections);
            $database->createIndex($reviews, Index::key('author_key', ['authorId']));

            foreach ($rowsByChain as [$first, $second, $rowsByTenant]) {
                $joins = fn (): array => [
                    $this->joinTenancyJoin($first, $books, 'book'),
                    $this->joinTenancyJoin($second, $reviews, 'review'),
                ];
                $label = "{$first->value} unindexed books then {$second->value} indexed reviews";

                foreach ($rowsByTenant as $selected => $rows) {
                    $database->setTenant($selected);

                    $this->assertSame(
                        $this->joinTenancySorted($rows),
                        $this->joinTenancyRows(
                            $database->find($authors, [...$joins(), Query::select(['name', 'book.pages', 'review.stars'])]),
                            ['book.pages', 'review.stars'],
                        ),
                        "Tenant {$selected} must read exactly its own rows through {$label}",
                    );
                    $this->assertSame(\count($rows), $database->count($authors, $joins()), "Tenant {$selected} must count exactly its own rows through {$label}");
                    $this->assertSame(
                        \array_sum(\array_map(static fn (array $row): int => $row[1] ?? 0, $rows)),
                        $database->sum($authors, 'book.pages', $joins()),
                        "Tenant {$selected} must sum exactly its own rows through {$label}",
                    );
                }

                $database->setTenant(1);
                $queries = fn (): array => [...$joins(), Query::select(['name', 'book.pages', 'review.stars'])];
                $this->assertSame('one-a1', $database->getDocument($authors, 'a1', $queries())->getAttribute('name'));
                $this->assertTrue($database->getDocument($authors, 'shared', $queries())->isEmpty(), "Tenant 1 must not read tenant 2's document through {$label}");
                $this->assertTrue($database->getDocument($authors, 'legacy', $queries())->isEmpty(), "Tenant 1 must not read a tenantless document through {$label}");
            }
        } finally {
            $database->setTenant(null);
            $this->cleanupAggCollections($database, $collections);
            $database->setTenant($tenant);
        }
    }

    public function testSharedTablesChainedJoinsKeepTheSelectedTenantsUnmatchedRows(): void
    {
        $database = static::getDatabase();
        if (! $database->hasSharedTables() || ! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collections = ['jtc_authors', 'jtc_books', 'jtc_reviews'];
        [$authors, $books, $reviews] = $collections;
        $tenant = $database->getTenant();

        $rowsByChain = [
            [Method::Join, Method::RightJoin, [
                1 => [['one-a1', 11, 5], [null, null, 4], [null, null, 3], [null, null, 2]],
                2 => [[null, null, 1]],
            ]],
            [Method::RightJoin, Method::RightJoin, [
                1 => [['one-a1', 11, 5], [null, null, 4], [null, null, 3], [null, null, 2]],
                2 => [[null, null, 1]],
            ]],
            [Method::LeftJoin, Method::FullOuterJoin, [
                1 => [['one-a1', 11, 5], ['one-a2', null, 4], [null, null, 3], [null, null, 2]],
                2 => [['two-a1', 21, null], ['two-a2', 22, null], ['two-shared', null, 1]],
            ]],
            [Method::CrossJoin, Method::RightJoin, [
                1 => [
                    ['one-a1', 11, 5], ['one-a1', 12, 5], ['one-a1', 13, 5],
                    ['one-a2', 11, 4], ['one-a2', 12, 4], ['one-a2', 13, 4],
                    [null, null, 3], [null, null, 2],
                ],
                2 => [['two-shared', 21, 1], ['two-shared', 22, 1]],
            ]],
        ];

        try {
            $this->seedJoinTenancyFixture($database, ...$collections);

            foreach ($rowsByChain as [$first, $second, $rowsByTenant]) {
                foreach ($rowsByTenant as $selected => $rows) {
                    $database->setTenant($selected);
                    $joins = fn (): array => [
                        $this->joinTenancyJoin($first, $books, 'book'),
                        $this->joinTenancyJoin($second, $reviews, 'review'),
                    ];
                    $label = "{$first->value} books then {$second->value} reviews";

                    $this->assertSame(
                        $this->joinTenancySorted($rows),
                        $this->joinTenancyRows(
                            $database->find($authors, [...$joins(), Query::select(['name', 'book.pages', 'review.stars'])]),
                            ['book.pages', 'review.stars'],
                        ),
                        "Tenant {$selected} must read exactly its own rows through {$label}",
                    );
                    $this->assertSame(
                        \count($rows),
                        $database->count($authors, $joins()),
                        "Tenant {$selected} must count exactly its own rows through {$label}",
                    );
                }
            }
        } finally {
            $database->setTenant(null);
            $this->cleanupAggCollections($database, $collections);
            $database->setTenant($tenant);
        }
    }

    /**
     * Two tenants reusing the same document ids, plus one legacy row per collection that has no
     * tenant at all. Tenant 1's book b2 names an author only tenant 2 has, b3 the tenantless author,
     * and review r4 an author only tenant 2 has; its author a2 has books only in tenant 2.
     */
    private function seedJoinTenancyFixture(Database $database, string $authors, string $books, string $reviews): void
    {
        $database->setTenant(null);
        $this->cleanupAggCollections($database, [$authors, $books, $reviews]);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $authors, permissions: $permissions, documentSecurity: false));
        $database->createAttribute($authors, Attribute::string(key: 'name', size: 64, required: true));
        foreach ([$books => 'pages', $reviews => 'stars'] as $collection => $number) {
            $database->createCollection(Collection::create(id: $collection, permissions: $permissions, documentSecurity: false));
            $database->createAttribute($collection, Attribute::string(key: 'authorId', size: 64, required: true));
            $database->createAttribute($collection, Attribute::integer(key: $number, required: true));
        }

        $tenantless = 3;
        $rows = [
            1 => [
                $authors => ['a1' => ['name' => 'one-a1'], 'a2' => ['name' => 'one-a2']],
                $books => [
                    'b1' => ['authorId' => 'a1', 'pages' => 11],
                    'b2' => ['authorId' => 'shared', 'pages' => 12],
                    'b3' => ['authorId' => 'legacy', 'pages' => 13],
                ],
                $reviews => [
                    'r1' => ['authorId' => 'a1', 'stars' => 5],
                    'r2' => ['authorId' => 'a2', 'stars' => 4],
                    'r3' => ['authorId' => 'ghost', 'stars' => 3],
                    'r4' => ['authorId' => 'shared', 'stars' => 2],
                ],
            ],
            2 => [
                $authors => ['a1' => ['name' => 'two-a1'], 'a2' => ['name' => 'two-a2'], 'shared' => ['name' => 'two-shared']],
                $books => [
                    'b1' => ['authorId' => 'a1', 'pages' => 21],
                    'b2' => ['authorId' => 'a2', 'pages' => 22],
                ],
                $reviews => ['r1' => ['authorId' => 'shared', 'stars' => 1]],
            ],
            $tenantless => [
                $authors => ['legacy' => ['name' => 'no-tenant']],
                $books => ['orphan' => ['authorId' => 'a1', 'pages' => 99]],
                $reviews => ['stale' => ['authorId' => 'a2', 'stars' => 9]],
            ],
        ];

        foreach ($rows as $owner => $documentsByCollection) {
            $database->setTenant($owner);
            foreach ($documentsByCollection as $collection => $documents) {
                foreach ($documents as $id => $attributes) {
                    $database->createDocument($collection, new Document([
                        '$id' => $id,
                        '$permissions' => [Permission::read(Role::any())],
                        ...$attributes,
                    ]));
                }
            }
        }

        $database->setTenant($tenantless);
        $database->getAuthorization()->skip(function () use ($database, $rows, $tenantless): void {
            foreach ($rows[$tenantless] as $collection => $documents) {
                $database->from($collection)
                    ->set([Document::TENANT => null])
                    ->filter([Query::equal(Document::ID, \array_keys($documents)), Query::equal(Document::TENANT, [$tenantless])])
                    ->update()
                    ->execute();
            }
        });
    }

    private function joinTenancyJoin(Method $method, string $collection, string $alias): Query
    {
        return match ($method) {
            Method::Join => Query::join($collection, $alias, [Query::on('$id', 'authorId')]),
            Method::LeftJoin => Query::leftJoin($collection, $alias, [Query::on('$id', 'authorId')]),
            Method::RightJoin => Query::rightJoin($collection, $alias, [Query::on('$id', 'authorId')]),
            Method::FullOuterJoin => Query::fullOuterJoin($collection, $alias, [Query::on('$id', 'authorId')]),
            Method::CrossJoin => Query::crossJoin($collection, $alias),
            default => throw new \InvalidArgumentException("{$method->value} is not a join"),
        };
    }

    /**
     * @param array<Document> $documents
     * @param list<string> $numbers
     * @return list<list<string|int|null>>
     */
    private function joinTenancyRows(array $documents, array $numbers): array
    {
        return $this->joinTenancySorted(\array_map(static function (Document $document) use ($numbers): array {
            $name = $document->getAttribute('name');
            $row = [\is_string($name) && $name !== '' ? $name : null];
            foreach ($numbers as $number) {
                $value = $document->getAttribute($number);
                $row[] = \is_numeric($value) ? (int) $value : null;
            }

            return $row;
        }, $documents));
    }

    /**
     * @param array<list<string|int|null>> $rows
     * @return list<list<string|int|null>>
     */
    private function joinTenancySorted(array $rows): array
    {
        \usort($rows, static fn (array $left, array $right): int => \json_encode($left) <=> \json_encode($right));

        return $rows;
    }

    /**
     * Chains of joins over collections read per document return what the same joins return over
     * the documents direct reads return: an unreadable document neither hides a row an outer join
     * keeps nor pairs with it, so a review of an unreadable author comes back like a review of an
     * author that does not exist.
     */
    public function testJoinChainsReadWhatDirectReadsAllow(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $secured = ['jcv_authors', 'jcv_books', 'jcv_reviews', 'jcv_extras'];
        $direct = ['jcvd_authors', 'jcvd_books', 'jcvd_reviews', 'jcvd_extras'];

        $chains = [
            'a right join' => [[Method::RightJoin, 1, '$id']],
            'a full outer join' => [[Method::FullOuterJoin, 1, '$id']],
            'an inner join, then a right join' => [[Method::Join, 1, '$id'], [Method::RightJoin, 2, '$id']],
            'a left join, then a right join' => [[Method::LeftJoin, 1, '$id'], [Method::RightJoin, 2, '$id']],
            'a cross join, then a right join' => [[Method::CrossJoin, 3, ''], [Method::RightJoin, 2, '$id']],
            'a right join, then a right join on it' => [[Method::RightJoin, 1, '$id'], [Method::RightJoin, 2, 'book.authorId']],
            'a full outer join, then a right join' => [[Method::FullOuterJoin, 1, '$id'], [Method::RightJoin, 2, '$id']],
            'a right join, then a full outer join on it' => [[Method::RightJoin, 1, '$id'], [Method::FullOuterJoin, 2, 'book.authorId']],
        ];

        try {
            $this->seedJoinChainVisibility($database, $secured, documentSecurity: true);
            $this->seedJoinChainVisibility($database, $direct, documentSecurity: false);

            $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $chains, $secured, $direct): void {
                foreach ($chains as $label => $chain) {
                    $this->assertSame(
                        $this->joinChainVisibilityRead($database, $direct, $chain),
                        $this->joinChainVisibilityRead($database, $secured, $chain),
                        "{$label} must return what the same joins return over what direct reads return",
                    );
                }
            });
        } finally {
            $this->cleanupAggCollections($database, [...$secured, ...$direct]);
        }
    }

    /**
     * A right join that follows a cross join, or a right join its ON references, must not pair its
     * rows with another tenant's rows of the earlier table: they would vanish instead of coming back
     * unmatched, and what a tenant reads would depend on another tenant's keys. A full outer join
     * combined with a right join reads what the tenant's own database reads too.
     */
    public function testSharedTablesJoinChainsKeepRowsOnlyAnotherTenantMatches(): void
    {
        $database = static::getDatabase();
        if (! $database->hasSharedTables() || ! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collections = ['jtx_authors', 'jtx_books', 'jtx_reviews'];
        [$authors, $books, $reviews] = $collections;
        $extras = 'jtx_extras';
        $tenant = $database->getTenant();

        $rowsByChain = [
            'a cross join, then a right join' => [
                'joins' => [Query::crossJoin($extras, 'extra'), Query::rightJoin($reviews, 'review', [Query::on('$id', 'authorId')])],
                'numbers' => ['extra.weight', 'review.stars'],
                'rows' => [
                    1 => [[null, null, 5], [null, null, 4], [null, null, 3], [null, null, 2]],
                    2 => [['two-shared', 7, 1]],
                ],
            ],
            'a cross join, then a right join on it' => [
                'joins' => [Query::crossJoin($extras, 'extra'), Query::rightJoin($reviews, 'review', [Query::on('extra.authorId', 'authorId')])],
                'numbers' => ['extra.weight', 'review.stars'],
                'rows' => [
                    1 => [[null, null, 5], [null, null, 4], [null, null, 3], [null, null, 2]],
                    2 => [['two-a1', 7, 1], ['two-a2', 7, 1], ['two-shared', 7, 1]],
                ],
            ],
            'a right join, then a right join on it' => [
                'joins' => [Query::rightJoin($books, 'book', [Query::on('$id', 'authorId')]), Query::rightJoin($reviews, 'review', [Query::on('book.authorId', 'authorId')])],
                'numbers' => ['book.pages', 'review.stars'],
                'rows' => [
                    1 => [['one-a1', 11, 5], [null, null, 4], [null, null, 3], [null, 12, 2]],
                    2 => [[null, null, 1]],
                ],
            ],
            'a full outer join, then a right join' => [
                'joins' => [Query::fullOuterJoin($books, 'book', [Query::on('$id', 'authorId')]), Query::rightJoin($reviews, 'review', [Query::on('$id', 'authorId')])],
                'numbers' => ['book.pages', 'review.stars'],
                'rows' => [
                    1 => [['one-a1', 11, 5], ['one-a2', null, 4], [null, null, 2], [null, null, 3]],
                    2 => [['two-shared', null, 1]],
                ],
            ],
            'a right join, then a full outer join on it' => [
                'joins' => [Query::rightJoin($books, 'book', [Query::on('$id', 'authorId')]), Query::fullOuterJoin($reviews, 'review', [Query::on('book.authorId', 'authorId')])],
                'numbers' => ['book.pages', 'review.stars'],
                'rows' => [
                    1 => [['one-a1', 11, 5], [null, 12, 2], [null, 13, null], [null, null, 3], [null, null, 4]],
                    2 => [['two-a1', 21, null], ['two-a2', 22, null], [null, null, 1]],
                ],
            ],
        ];

        try {
            $this->seedJoinTenancyFixture($database, ...$collections);
            $this->seedJoinTenancyExtras($database, $extras);

            foreach ($rowsByChain as $label => ['joins' => $joins, 'numbers' => $numbers, 'rows' => $rowsByTenant]) {
                foreach ($rowsByTenant as $selected => $rows) {
                    $database->setTenant($selected);

                    $this->assertSame(
                        $this->joinTenancySorted($rows),
                        $this->joinTenancyRows(
                            $database->find($authors, [...$joins, Query::select(['name', ...$numbers])]),
                            $numbers,
                        ),
                        "Tenant {$selected} must read through {$label} what its own database would return",
                    );
                    $this->assertSame(\count($rows), $database->count($authors, $joins), "Tenant {$selected} must count through {$label} what its own database would count");
                    $this->assertSame(
                        \array_sum(\array_column($rows, 2)),
                        (int) $database->sum($authors, $numbers[1], $joins),
                        "Tenant {$selected} must sum through {$label} what its own database would sum",
                    );
                }
            }
        } finally {
            $database->setTenant(null);
            $this->cleanupAggCollections($database, [...$collections, $extras]);
            $database->setTenant($tenant);
        }
    }

    /**
     * The documents of testJoinChainsReadWhatDirectReadsAllow: with document security every
     * collection shows only the documents the caller holds read on, and the unreadable ones share
     * keys with readable ones. Without it, the collections hold exactly the documents a direct read
     * of the others returns.
     *
     * @param array{string, string, string, string} $collections authors, books, reviews, extras
     */
    private function seedJoinChainVisibility(Database $database, array $collections, bool $documentSecurity): void
    {
        [$authors, $books, $reviews, $extras] = $collections;
        $this->cleanupAggCollections($database, $collections);

        $permissions = $documentSecurity
            ? [Permission::create(Role::any())]
            : [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $authors, permissions: $permissions, documentSecurity: $documentSecurity));
        $database->createAttribute($authors, Attribute::string(key: 'name', size: 64, required: true));
        foreach ([$books => 'pages', $reviews => 'stars', $extras => 'weight'] as $collection => $number) {
            $database->createCollection(Collection::create(id: $collection, permissions: $permissions, documentSecurity: $documentSecurity));
            $database->createAttribute($collection, Attribute::string(key: 'authorId', size: 64, required: true));
            $database->createAttribute($collection, Attribute::integer(key: $number, required: true));
        }

        $documents = [
            $authors => [
                'a1' => [['name' => 'a1'], true],
                'a2' => [['name' => 'a2'], true],
                'hidden' => [['name' => 'hidden'], false],
            ],
            $books => [
                'b1' => [['authorId' => 'a1', 'pages' => 1], true],
                'b2' => [['authorId' => 'a2', 'pages' => 2], false],
                'b3' => [['authorId' => 'hidden', 'pages' => 3], true],
                'b4' => [['authorId' => 'ghost', 'pages' => 4], true],
                'b5' => [['authorId' => 'a1', 'pages' => 5], false],
            ],
            $reviews => [
                'r1' => [['authorId' => 'a1', 'stars' => 10], true],
                'r2' => [['authorId' => 'a2', 'stars' => 20], true],
                'r3' => [['authorId' => 'hidden', 'stars' => 30], true],
                'r4' => [['authorId' => 'ghost', 'stars' => 40], true],
                'r5' => [['authorId' => 'a2', 'stars' => 50], false],
            ],
            $extras => [
                'x1' => [['authorId' => 'a1', 'weight' => 100], false],
            ],
        ];

        foreach ($documents as $collection => $rows) {
            foreach ($rows as $id => [$attributes, $readable]) {
                if (! $readable && ! $documentSecurity) {
                    continue;
                }

                $database->createDocument($collection, new Document([
                    '$id' => $id,
                    '$permissions' => [$readable ? Permission::read(Role::any()) : Permission::read(Role::user('someone-else'))],
                    ...$attributes,
                ]));
            }
        }
    }

    /**
     * @param array{string, string, string, string} $collections authors, books, reviews, extras
     * @param list<array{Method, int, string}> $chain Each join's method, collection index and ON column
     * @return array{rows: list<list<string|int|null>>, count: int, sum: int|float}|string
     */
    private function joinChainVisibilityRead(Database $database, array $collections, array $chain): array|string
    {
        $numbers = [];
        $joins = [];
        foreach ($chain as [$method, $collection, $on]) {
            [$alias, $number] = [1 => ['book', 'pages'], 2 => ['review', 'stars'], 3 => ['extra', 'weight']][$collection];
            $numbers[] = $alias.'.'.$number;
            $joins[] = match ($method) {
                Method::Join => Query::join($collections[$collection], $alias, [Query::on($on, 'authorId')]),
                Method::LeftJoin => Query::leftJoin($collections[$collection], $alias, [Query::on($on, 'authorId')]),
                Method::RightJoin => Query::rightJoin($collections[$collection], $alias, [Query::on($on, 'authorId')]),
                Method::FullOuterJoin => Query::fullOuterJoin($collections[$collection], $alias, [Query::on($on, 'authorId')]),
                Method::CrossJoin => Query::crossJoin($collections[$collection], $alias),
                default => throw new \InvalidArgumentException("{$method->value} is not a join"),
            };
        }

        try {
            return [
                'rows' => $this->joinTenancyRows(
                    $database->find($collections[0], [...$joins, Query::select(['name', ...$numbers]), Query::limit(100)]),
                    $numbers,
                ),
                'count' => $database->count($collections[0], $joins),
                'sum' => $database->sum($collections[0], $numbers[\count($numbers) - 1], $joins),
            ];
        } catch (QueryException) {
            return 'rejected';
        }
    }

    /**
     * Extras only tenant 2 and a tenantless row have, so tenant 1's cross join with them is empty.
     */
    private function seedJoinTenancyExtras(Database $database, string $extras): void
    {
        $database->setTenant(null);
        $database->createCollection(Collection::create(
            id: $extras,
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
            documentSecurity: false,
        ));
        $database->createAttribute($extras, Attribute::string(key: 'authorId', size: 64, required: true));
        $database->createAttribute($extras, Attribute::integer(key: 'weight', required: true));

        $tenantless = 3;
        foreach ([2 => ['x1', 'shared', 7], $tenantless => ['x9', 'a2', 9]] as $owner => [$id, $authorId, $weight]) {
            $database->setTenant($owner);
            $database->createDocument($extras, new Document([
                '$id' => $id,
                '$permissions' => [Permission::read(Role::any())],
                'authorId' => $authorId,
                'weight' => $weight,
            ]));
        }

        $database->getAuthorization()->skip(function () use ($database, $extras, $tenantless): void {
            $database->from($extras)
                ->set([Document::TENANT => null])
                ->filter([Query::equal(Document::ID, ['x9']), Query::equal(Document::TENANT, [$tenantless])])
                ->update()
                ->execute();
        });
    }

    /**
     * The builder declares a join alias quoted, so the tenant and permission conditions added for
     * it must name it quoted too: PostgreSQL folds an unquoted mixed-case alias to lower case and
     * then finds no table by that name.
     */
    public function testMixedCaseJoinAliasesReadWhatLowerCaseAliasesRead(): void
    {
        $this->assertJoinAliasesReadWhatLowerCaseAliasesRead(['jam_authors', 'jam_books', 'jam_reviews', 'jam_extras'], [1 => 'Book', 2 => 'Review', 3 => 'Extra']);
    }

    /**
     * A reserved word is a valid join alias once quoted, as the builder declares it, but not where
     * a tenant or permission condition names it unquoted.
     */
    public function testReservedWordJoinAliasesReadWhatLowerCaseAliasesRead(): void
    {
        $this->assertJoinAliasesReadWhatLowerCaseAliasesRead(['jar_authors', 'jar_books', 'jar_reviews', 'jar_extras'], [1 => 'order', 2 => 'group', 3 => 'select']);
    }

    /**
     * Every join type and the chains whose later right join repeats earlier tables' conditions,
     * over collections read per document, under the adapter's tenancy.
     *
     * @param array{string, string, string, string} $collections authors, books, reviews, extras
     * @param array{1: string, 2: string, 3: string} $aliases The alias of books, reviews and extras
     */
    private function assertJoinAliasesReadWhatLowerCaseAliasesRead(array $collections, array $aliases): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $chains = [
            'an inner join' => [[Method::Join, 1, null]],
            'a left join' => [[Method::LeftJoin, 1, null]],
            'a right join' => [[Method::RightJoin, 1, null]],
            'a full outer join' => [[Method::FullOuterJoin, 1, null]],
            'a cross join' => [[Method::CrossJoin, 1, null]],
            'an inner join, then a right join' => [[Method::Join, 1, null], [Method::RightJoin, 2, null]],
            'a cross join, then a right join' => [[Method::CrossJoin, 3, null], [Method::RightJoin, 2, null]],
            'a right join, then a right join on it' => [[Method::RightJoin, 1, null], [Method::RightJoin, 2, 1]],
        ];

        try {
            $this->seedJoinChainVisibility($database, $collections, documentSecurity: true);

            $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $collections, $aliases, $chains): void {
                foreach ($chains as $label => $chain) {
                    $expected = $this->joinAliasRead($database, $collections, $chain, [1 => 'book', 2 => 'review', 3 => 'extra']);

                    $this->assertNotSame([], $expected['rows'], "{$label} must return rows for the comparison to mean anything");
                    $this->assertSame(
                        $expected,
                        $this->joinAliasRead($database, $collections, $chain, $aliases),
                        "{$label} aliased ".\implode(', ', $aliases).' must read what it reads with lower-case aliases',
                    );
                }
            });
        } finally {
            $this->cleanupAggCollections($database, $collections);
        }
    }

    /**
     * @param array{string, string, string, string} $collections authors, books, reviews, extras
     * @param list<array{Method, int, ?int}> $chain Each join's method, the index of the collection it joins, and the
     *                                             index of the joined collection its ON names, or null for the main one
     * @param array{1: string, 2: string, 3: string} $aliases The alias of books, reviews and extras
     * @return array{rows: list<list<string|int|null>>, count: int, sum: int|float, document: list<list<string|int|null>>}
     */
    private function joinAliasRead(Database $database, array $collections, array $chain, array $aliases): array
    {
        $numbers = [];
        $joins = [];
        foreach ($chain as [$method, $collection, $on]) {
            $alias = $aliases[$collection];
            $numbers[] = $alias.'.'.[1 => 'pages', 2 => 'stars', 3 => 'weight'][$collection];
            $left = $on === null ? '$id' : $aliases[$on].'.authorId';
            $joins[] = match ($method) {
                Method::Join => Query::join($collections[$collection], $alias, [Query::on($left, 'authorId')]),
                Method::LeftJoin => Query::leftJoin($collections[$collection], $alias, [Query::on($left, 'authorId')]),
                Method::RightJoin => Query::rightJoin($collections[$collection], $alias, [Query::on($left, 'authorId')]),
                Method::FullOuterJoin => Query::fullOuterJoin($collections[$collection], $alias, [Query::on($left, 'authorId')]),
                Method::CrossJoin => Query::crossJoin($collections[$collection], $alias),
                default => throw new \InvalidArgumentException("{$method->value} is not a join"),
            };
        }
        $selection = Query::select(['name', ...$numbers]);

        return [
            'rows' => $this->joinTenancyRows($database->find($collections[0], [...$joins, $selection, Query::limit(100)]), $numbers),
            'count' => $database->count($collections[0], $joins),
            'sum' => $database->sum($collections[0], $numbers[\count($numbers) - 1], $joins),
            'document' => $this->joinTenancyRows([$database->getDocument($collections[0], 'a1', [...$joins, $selection])], $numbers),
        ];
    }

    public function testAttributeNamedLikeAFullOuterJoinOrderColumnIsRead(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->createOrderColumnCollections($database);
        $note = 'foj_ord_note';

        $this->assertSame('first', $database->getDocument($main, 'm1')->getAttribute($note), 'getDocument');
        $this->assertSame('first', $database->getDocument($main, 'm1', [Query::select([$note])])->getAttribute($note), 'getDocument with a select');
        $this->assertSame(['first', 'second'], $this->orderColumnValues($database->find($main), $note), 'find');
        $this->assertSame(['first', 'second'], $this->orderColumnValues($database->getAuthorization()->skip(fn (): array => $database->find($main)), $note), 'find without authorization');
        $this->assertSame(['second'], $this->orderColumnValues($database->find($main, [Query::equal($note, ['second'])]), $note), 'find filtered by the attribute');
        $this->assertSame(['second', 'first'], $this->orderColumnValues($database->find($main, [Query::orderDesc($note)]), $note), 'find ordered by the attribute');

        $this->assertOrderColumnFullOuterJoin($database, $main, $joined, 'j', [
            'ascending' => [Query::orderAsc($note), $note, ['first', 'second']],
            'descending' => [Query::orderDesc($note), $note, ['second', 'first']],
        ]);

        $this->cleanupAggCollections($database, $this->orderColumnCollections());
    }

    public function testJoinAliasNamedLikeAFullOuterJoinOrderColumnReturnsItsColumns(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->createOrderColumnCollections($database);
        $alias = 'foj_ord_x';

        $this->assertSame(
            [['m1', 'first', 'j1', 1]],
            $this->orderColumnSummaries($database->find($main, [Query::join($joined, $alias, [Query::on('link', 'link')])]), $alias),
            'find',
        );
        $this->assertSame(
            [['m1', 'first', 'j1', 1]],
            $this->orderColumnSummaries([$database->getDocument($main, 'm1', [Query::leftJoin($joined, $alias, [Query::on('link', 'link')])])], $alias),
            'getDocument',
        );

        $this->assertOrderColumnFullOuterJoin($database, $main, $joined, $alias, [
            'ascending' => [Query::orderAsc("{$alias}.score"), "{$alias}.score", [1, 3]],
            'descending' => [Query::orderDesc("{$alias}.score"), "{$alias}.score", [3, 1]],
        ]);

        $this->cleanupAggCollections($database, $this->orderColumnCollections());
    }

    /**
     * Every order returns each row of the full outer join once with its values, orders the rows that
     * hold a value (engines place nulls apart), and gives each row the columns a left join gives it.
     *
     * @param  array<string, array{Query, string, list<string|int>}>  $orders
     */
    private function assertOrderColumnFullOuterJoin(Database $database, string $main, string $joined, string $alias, array $orders): void
    {
        $leftJoined = $database->find($main, [Query::leftJoin($joined, $alias, [Query::on('link', 'link')])]);
        $this->assertCount(2, $leftJoined);
        $columns = $this->orderColumnKeys($leftJoined[0]);

        foreach ($orders as $label => [$order, $key, $ordered]) {
            $rows = $database->find($main, [Query::fullOuterJoin($joined, $alias, [Query::on('link', 'link')]), $order]);

            $summaries = $this->orderColumnSummaries($rows, $alias);
            \usort($summaries, static fn (array $left, array $right): int => \strcmp((string) \json_encode($left), (string) \json_encode($right)));
            $this->assertSame([['', null, 'j2', 3], ['m1', 'first', 'j1', 1], ['m2', 'second', null, null]], $summaries, $label);

            $values = \array_map(static fn (mixed $value): mixed => \is_numeric($value) ? (int) $value : $value, $this->orderColumnValues($rows, $key));
            $this->assertSame($ordered, \array_values(\array_filter($values, static fn (mixed $value): bool => $value !== null)), $label);

            foreach ($rows as $row) {
                $this->assertSame($columns, $this->orderColumnKeys($row), $label);
            }
        }
    }

    /**
     * @param  array<Document>  $rows
     * @return list<mixed>
     */
    private function orderColumnValues(array $rows, string $key): array
    {
        return \array_values(\array_map(static fn (Document $row): mixed => $row->getAttribute($key), $rows));
    }

    /**
     * @param  array<Document>  $rows
     * @return list<array{string, mixed, mixed, ?int}>
     */
    private function orderColumnSummaries(array $rows, string $alias): array
    {
        return \array_values(\array_map(
            fn (Document $row): array => [
                $row->getId(),
                $row->getAttribute('foj_ord_note'),
                $row->getAttribute("{$alias}.\$id"),
                $this->scoreOf($row, "{$alias}.score"),
            ],
            $rows,
        ));
    }

    /**
     * @return list<string>
     */
    private function orderColumnKeys(Document $row): array
    {
        $keys = \array_map(\strval(...), \array_keys($row->getArrayCopy()));
        \sort($keys);

        return $keys;
    }

    /**
     * @return list<string>
     */
    private function orderColumnCollections(): array
    {
        return ['fojo_main', 'fojo_joined'];
    }

    /**
     * m1 matches j1 and nothing matches m2 or j2, so a full outer join returns a row of each kind, and
     * an emulated one returns rows from both of its halves.
     *
     * @return list<string>
     */
    private function createOrderColumnCollections(Database $database): array
    {
        $collections = $this->orderColumnCollections();
        [$main, $joined] = $collections;
        $this->cleanupAggCollections($database, $collections);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(
            id: $main,
            attributes: [
                Attribute::string(key: 'link', size: 16, required: true),
                Attribute::string(key: 'foj_ord_note', size: 64, required: false),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(Collection::create(
            id: $joined,
            attributes: [
                Attribute::string(key: 'link', size: 16, required: true),
                Attribute::integer(key: 'score', required: true),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));

        foreach (['m1' => ['1', 'first'], 'm2' => ['2', 'second']] as $id => [$link, $note]) {
            $database->createDocument($main, new Document(['$id' => $id, 'link' => $link, 'foj_ord_note' => $note]));
        }
        foreach (['j1' => ['1', 1], 'j2' => ['3', 3]] as $id => [$link, $score]) {
            $database->createDocument($joined, new Document(['$id' => $id, 'link' => $link, 'score' => $score]));
        }

        return $collections;
    }

    public function testJoinColumnTheJoinedCollectionDoesNotDeclareIsAnInvalidQuery(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, $orders] = $collections = $this->seedJoinedAttributeCollections($database, 'jcnd');
        $join = Query::join($orders, 'purchase', [Query::on('$id', 'customerId')]);

        $reads = [
            'filter' => fn () => $database->find($customers, [$join, Query::equal('purchase.nothing', ['x'])]),
            'select' => fn () => $database->find($customers, [$join, Query::select(['name', 'purchase.nothing'])]),
            'order' => fn () => $database->find($customers, [$join, Query::orderAsc('purchase.nothing')]),
            'count()' => fn () => $database->count($customers, [$join, Query::equal('purchase.nothing', ['x'])]),
            'sum()' => fn () => $database->sum($customers, 'purchase.amount', [$join, Query::equal('purchase.nothing', ['x'])]),
        ];
        if ($database->getAdapter()->supports(Capability::Aggregations)) {
            $reads['aggregate'] = fn () => $database->aggregate($customers, [$join, Query::countDistinct('purchase.nothing', 'total')]);
            $reads['groupBy'] = fn () => $database->aggregate($customers, [$join, Query::count('*', 'rows'), Query::groupBy(['purchase.nothing'])]);
        }

        foreach ($reads as $type => $read) {
            try {
                $read();
                $this->fail('A '.$type.' on a column the joined collection does not declare reached the engine');
            } catch (QueryException $error) {
                $this->assertSame('Invalid query: Attribute not found in schema: purchase.nothing', $error->getMessage(), $type);
            }
        }

        try {
            $database->find($customers, [$join, Query::equal('purchase.$permissions', ['read("any")'])]);
            $this->fail('A filter on joined permissions was accepted although a filter on the main permissions is not');
        } catch (QueryException $error) {
            $this->assertSame('Invalid query: Attribute not found in schema: purchase.$permissions', $error->getMessage());
        }

        $results = $database->find($customers, [
            $join,
            Query::equal('purchase.$id', ['paid', 'open']),
            Query::between('purchase.$createdAt', '1970-01-01', '2099-12-31'),
            Query::between('purchase.amount', 10, 500),
            Query::select(['name', 'purchase.$id', 'purchase.$permissions', 'purchase.$createdAt', 'purchase.$sequence']),
            Query::orderAsc('purchase.amount'),
        ]);
        $this->assertSame(['open', 'paid'], \array_map(static fn (Document $document): mixed => $document->getAttribute('purchase.$id'), $results));

        $this->cleanupAggCollections($database, $collections);
    }

    public function testGetDocumentJoinConditionIsValidatedAsAListingValidatesIt(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, , , $notes] = $collections = $this->seedJoinedAttributeCollections($database, 'jgdc');

        foreach ([
            'join condition' => [Query::leftJoin($notes, 'note', [Query::on('$id', 'customerId'), Query::equal('note.nothing', ['x'])])],
            'select' => [Query::leftJoin($notes, 'note', [Query::on('$id', 'customerId')]), Query::select(['name', 'note.nothing'])],
        ] as $type => $queries) {
            try {
                $database->getDocument($customers, 'first', $queries);
                $this->fail('getDocument() sent a '.$type.' on a column the joined collection does not declare to the engine');
            } catch (QueryException $error) {
                $this->assertSame('Invalid query: Attribute not found in schema: note.nothing', $error->getMessage(), $type);
            }
        }

        try {
            $database->getDocument($customers, 'first', [
                Query::leftJoin($notes, 'note', [Query::on('$id', 'customerId')]),
                Query::equal('name', ['First']),
            ]);
            $this->fail('getDocument() accepted a filter outside a join condition');
        } catch (QueryException $error) {
            $this->assertSame('Invalid query method: equal', $error->getMessage());
        }

        $document = $database->getDocument($customers, 'first', [
            Query::leftJoin($notes, 'note', [Query::on('$id', 'customerId'), Query::equal('note.body', ['a needle in a haystack'])]),
        ]);
        $this->assertSame('first', $document->getId());

        $this->cleanupAggCollections($database, $collections);
    }

    public function testJoinArithmeticAggregateOfAJoinedStringIsAnInvalidQuery(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, $orders, , $notes] = $collections = $this->seedJoinedAttributeCollections($database, 'jnsa');
        $joins = [
            Query::join($orders, 'purchase', [Query::on('$id', 'customerId')]),
            Query::join($notes, 'note', [Query::on('$id', 'customerId')]),
        ];

        foreach ([
            'Aggregate sum requires a numeric attribute that is not an array: purchase.status' => Query::sum('purchase.status', 'total'),
            'Aggregate sum requires a numeric attribute that is not an array: status' => Query::sum('status', 'total'),
            'Aggregate avg requires a numeric attribute that is not an array: note.body' => Query::avg('note.body', 'average'),
            'Aggregate stddev requires a numeric attribute that is not an array: purchase.memo' => Query::stddev('purchase.memo', 'spread'),
            'Aggregate bitAnd requires a numeric attribute that is not an array: purchase.$createdAt' => Query::bitAnd('purchase.$createdAt', 'bits'),
        ] as $message => $aggregate) {
            try {
                $database->aggregate($customers, [...$joins, $aggregate]);
                $this->fail('An aggregate over a joined attribute that holds no number reached the engine: '.$message);
            } catch (QueryException $error) {
                $this->assertSame('Invalid query: '.$message, $error->getMessage());
            }
        }

        $results = $database->aggregate($customers, [...$joins, Query::sum('purchase.amount', 'total')]);
        $this->assertCount(1, $results);
        $this->assertSame(150, $this->intAttribute($results[0], 'total'));

        $this->cleanupAggCollections($database, $collections);
    }

    public function testJoinedInternalAttributesGroupTheJoinedRows(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, $orders] = $collections = $this->seedJoinedAttributeCollections($database, 'jiag');

        foreach ([
            'inner join' => Query::join($orders, 'purchase', [Query::on('$id', 'customerId')]),
            'left join' => Query::leftJoin($orders, 'purchase', [Query::on('$id', 'customerId')]),
            'full outer join' => Query::fullOuterJoin($orders, 'purchase', [Query::on('$id', 'customerId')]),
        ] as $type => $join) {
            foreach ([Document::ID, Document::SEQUENCE, Document::CREATED_AT, Document::UPDATED_AT, Document::PERMISSIONS] as $attribute) {
                $total = 0;
                foreach ($database->aggregate($customers, [$join, Query::count('*', 'rows'), Query::groupBy(['purchase.'.$attribute])]) as $group) {
                    $this->assertArrayHasKey(Storage::column($attribute), $group, $type.' grouped by purchase.'.$attribute);
                    $total += $this->intAttribute($group, 'rows');
                }
                $this->assertSame(3, $total, $type.' grouped by purchase.'.$attribute);
            }
        }

        $groups = $database->aggregate($customers, [
            Query::join($orders, 'purchase', [Query::on('$id', 'customerId')]),
            Query::sum('purchase.amount', 'total'),
            Query::groupBy(['purchase.$id']),
            Query::orderAsc('purchase.$id'),
        ]);
        $this->assertSame(['open', 'other', 'paid'], \array_map(static fn (array $group): mixed => $group[Storage::UID], $groups));
        $this->assertSame([50, 7, 100], \array_map(fn (array $group): int => $this->intAttribute($group, 'total'), $groups));

        $this->cleanupAggCollections($database, $collections);
    }

    public function testJoinConditionNamingNoColumnIsAnInvalidQuery(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, $orders, , $notes] = $collections = $this->seedJoinedAttributeCollections($database, 'jcnc');
        $purchase = Query::join($orders, 'purchase', [Query::on('$id', 'customerId')]);
        $notFound = 'Invalid query: Attribute not found in schema: ';

        foreach ([
            'an unknown right column' => [[Query::join($orders, 'purchase', [Query::on('$id', 'nothing')])], $notFound.'nothing'],
            'an unknown left column' => [[Query::join($orders, 'purchase', [Query::on('nothing', 'customerId')])], $notFound.'nothing'],
            'an unknown right column of an on condition' => [[Query::leftJoin($notes, 'note', [Query::on('$id', 'nothing')])], $notFound.'nothing'],
            'an unknown left column of an on condition' => [[Query::leftJoin($notes, 'note', [Query::on('nothing', 'customerId')])], $notFound.'nothing'],
            'an unknown column of an earlier join' => [[$purchase, Query::join($notes, 'note', [Query::on('purchase.nothing', 'customerId')])], $notFound.'purchase.nothing'],
            'a join declared after it' => [
                [Query::join($notes, 'note', [Query::on('purchase.customerId', 'customerId')]), $purchase],
                'Invalid query: The left column of a join condition must belong to the main collection or to a join declared before it: purchase.customerId',
            ],
        ] as $shape => [$joins, $message]) {
            foreach ([
                'find()' => fn () => $database->find($customers, $joins),
                'count()' => fn () => $database->count($customers, $joins),
                'sum()' => fn () => $database->sum($customers, '$sequence', $joins),
                'getDocument()' => fn () => $database->getDocument($customers, 'first', $joins),
            ] as $read => $call) {
                try {
                    $call();
                    $this->fail($read.' sent a join condition that names no column to the engine: '.$shape);
                } catch (QueryException $error) {
                    $this->assertSame($message, $error->getMessage(), $read.': '.$shape);
                }
            }
        }

        $this->assertSame(3, $database->count($customers, [Query::leftJoin($orders, 'purchase', [Query::on('$id', 'purchase.customerId')])]));
        $this->assertSame(2, $database->count($customers, [$purchase, Query::join($notes, 'note', [Query::on('purchase.customerId', 'customerId')])]), 'a join names the columns of the join before it');

        $this->cleanupAggCollections($database, $collections);
    }

    public function testSumRejectsAnAttributeASumAggregateRejects(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::DefinedAttributes)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $joins = $database->getAdapter()->supports(Capability::Joins);
        [$customers, $orders] = $collections = $this->seedJoinedAttributeCollections($database, 'jsum');
        $purchase = Query::join($orders, 'purchase', [Query::on('$id', 'customerId')]);
        $notFound = 'Invalid query: Attribute not found in schema: ';
        $numeric = 'Invalid query: Aggregate sum requires a numeric attribute that is not an array: ';

        $rejected = [
            'an unknown attribute' => [$customers, 'nothing', [], $notFound.'nothing'],
            'a string' => [$orders, 'status', [], $numeric.'status'],
            'an internal attribute' => [$orders, '$sequence', [], $numeric.'$sequence'],
        ];
        if ($joins) {
            $rejected += [
                'a joined string' => [$customers, 'purchase.status', [$purchase], $numeric.'purchase.status'],
                'an unknown joined attribute' => [$customers, 'purchase.nothing', [$purchase], $notFound.'purchase.nothing'],
                'a string only a join declares, unqualified' => [$customers, 'status', [$purchase], $numeric.'status'],
            ];
        }

        foreach ($rejected as $shape => [$collection, $attribute, $queries, $message]) {
            try {
                $database->sum($collection, $attribute, $queries);
                $this->fail('sum() added up '.$shape);
            } catch (QueryException $error) {
                $this->assertSame($message, $error->getMessage(), $shape);
            }
        }

        $this->assertSame(157, $database->sum($orders, 'amount'));
        if ($joins) {
            $this->assertSame(157, $database->sum($customers, 'purchase.amount', [$purchase]));
            $this->assertSame(157, $database->sum($customers, 'amount', [$purchase]));
        }

        $this->cleanupAggCollections($database, $collections);
    }

    public function testTenantIsReadOnlyWhereTheTablesHoldIt(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, $orders] = $collections = $this->seedJoinedAttributeCollections($database, 'jten');
        $purchase = Query::join($orders, 'purchase', [Query::on('$id', 'customerId')]);

        foreach ([
            'count' => [Query::count('$collection', 'total')],
            'groupBy' => [Query::count('*', 'rows'), Query::groupBy(['$collection'])],
        ] as $shape => $queries) {
            try {
                $database->aggregate($customers, $queries);
                $this->fail('a '.$shape.' of $collection, which no table holds, reached the engine');
            } catch (QueryException $error) {
                $this->assertSame('Invalid query: Attribute not found in schema: $collection', $error->getMessage(), $shape);
            }
        }

        $reads = [
            '$tenant' => [
                'count' => fn (): array => $database->aggregate($customers, [Query::count('$tenant', 'total')]),
                'groupBy' => fn (): array => $database->aggregate($customers, [Query::count('*', 'total'), Query::groupBy(['$tenant'])]),
            ],
            'purchase.$tenant' => [
                'count' => fn (): array => $database->aggregate($customers, [$purchase, Query::count('purchase.$tenant', 'total')]),
                'groupBy' => fn (): array => $database->aggregate($customers, [$purchase, Query::count('*', 'total'), Query::groupBy(['purchase.$tenant'])]),
            ],
        ];

        if (! $database->hasSharedTables()) {
            foreach ($reads as $attribute => $shapes) {
                foreach ($shapes as $shape => $read) {
                    try {
                        $read();
                        $this->fail('a '.$shape.' of '.$attribute.' reached a table that does not hold it');
                    } catch (QueryException $error) {
                        $this->assertSame('Invalid query: Attribute not found in schema: '.$attribute, $error->getMessage(), $shape);
                    }
                }
            }

            try {
                $database->find($customers, [$purchase, Query::select(['name', 'purchase.$tenant'])]);
                $this->fail('a select of purchase.$tenant reached a table that does not hold it');
            } catch (QueryException $error) {
                $this->assertSame('Invalid query: Attribute not found in schema: purchase.$tenant', $error->getMessage());
            }

            $this->cleanupAggCollections($database, $collections);

            return;
        }

        $tenant = (string) $database->getTenant();
        foreach (['$tenant' => 2, 'purchase.$tenant' => 3] as $attribute => $rows) {
            $counted = $reads[$attribute]['count']();
            $this->assertCount(1, $counted, $attribute);
            $this->assertSame($rows, $this->intAttribute($counted[0], 'total'), $attribute);

            $grouped = $reads[$attribute]['groupBy']();
            $this->assertCount(1, $grouped, $attribute);
            $this->assertSame($rows, $this->intAttribute($grouped[0], 'total'), $attribute);
            $value = $grouped[0][Storage::TENANT];
            $this->assertIsScalar($value, $attribute);
            $this->assertSame($tenant, (string) $value, $attribute);
        }

        foreach ($database->find($customers, [$purchase, Query::select(['name', 'purchase.$tenant'])]) as $customer) {
            $value = $customer->getAttribute('purchase.$tenant');
            $this->assertIsScalar($value);
            $this->assertSame($tenant, (string) $value);
        }

        $this->cleanupAggCollections($database, $collections);
    }

    public function testSqliteJoinPlansSearchAnIndexPerAlias(): void
    {
        $database = static::getDatabase();
        $adapter = $database->getAdapter();
        if (! $adapter instanceof SQLite) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collection = 'sqlite_join_plans';
        $this->cleanupAggCollections($database, [$collection]);
        $database->createCollection(Collection::create(id: $collection, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($collection, Attribute::string(key: 'name', size: 64, required: true));

        $documents = [];
        for ($position = 0; $position < 50; $position++) {
            $documents[] = new Document(['$id' => 'doc'.$position, 'name' => 'name'.$position]);
        }
        $database->createDocuments($collection, $documents);

        $table = '`'.$database->getNamespace().'_'.$collection.'`';
        $profiler = $database->setProfiling(true)->getProfiler();
        $this->assertNotNull($profiler);

        try {
            for ($joins = 1; $joins <= 4; $joins++) {
                $queries = [];
                for ($join = 1; $join <= $joins; $join++) {
                    $queries[] = Query::join($collection, 'p'.$join, [Query::on('$id', '$id')]);
                }

                $profiler->reset();
                $this->assertCount(25, $database->find($collection, $queries), $joins.' self-joins');

                $plans = 0;
                foreach ($profiler->getLogs() as $log) {
                    if (! \str_contains($log->query, 'SELECT') || ! \str_contains($log->query, $table.' AS `p1`')) {
                        continue;
                    }
                    $plans++;

                    $details = \array_map(
                        static function (Document $row): string {
                            $detail = $row->getAttribute('detail');
                            self::assertIsString($detail);

                            return $detail;
                        },
                        $adapter->rawQuery('EXPLAIN QUERY PLAN '.$log->query),
                    );
                    $report = $log->query."\n  ".\implode("\n  ", $details);

                    for ($join = 1; $join <= $joins; $join++) {
                        $lookups = \array_filter($details, static fn (string $detail): bool => \str_starts_with($detail, 'SEARCH p'.$join.' '));
                        $this->assertCount(1, $lookups, 'Alias p'.$join.' must be searched through an index: '.$report);
                        $this->assertStringContainsString('_uid=?', (string) \current($lookups), 'Alias p'.$join.' must be looked up by id: '.$report);
                    }
                    foreach ($details as $detail) {
                        $this->assertStringNotContainsString('AUTOMATIC', $detail, $report);
                        $this->assertDoesNotMatchRegularExpression('/^SCAN p\d+\b/', $detail, $report);
                    }
                }
                $this->assertSame(1, $plans, $joins.' self-joins must read the collection in one statement');
            }
        } finally {
            $database->setProfiling(false);
            $this->cleanupAggCollections($database, [$collection]);
        }
    }

    public function testSqliteJoinedSearchUsesTheJoinedFulltextIndex(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter() instanceof SQLite) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $authors = 'sqlite_search_authors';
        $posts = 'sqlite_search_posts';
        $collections = [$authors, $posts];
        $this->cleanupAggCollections($database, $collections);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $authors, permissions: $permissions));
        $database->createAttribute($authors, Attribute::string(key: 'name', size: 64, required: true));
        $database->createCollection(Collection::create(id: $posts, permissions: $permissions));
        $database->createAttribute($posts, Attribute::string(key: 'authorId', size: 64, required: true));
        $database->createAttribute($posts, Attribute::string(key: 'body', size: 256, required: true));
        $database->createIndex($posts, Index::fulltext(key: 'body_fulltext', attributes: ['body']));

        $bodies = [
            'brown' => 'the quick brown fox',
            'lazy' => 'a lazy dog sleeps',
            'foxes' => 'foxes run at night',
            'phrase' => 'quick fox',
        ];
        foreach ($bodies as $author => $body) {
            $database->createDocument($authors, new Document(['$id' => $author, 'name' => $author]));
            $database->createDocument($posts, new Document(['$id' => 'post_'.$author, 'authorId' => $author, 'body' => $body]));
        }

        $join = Query::join($posts, 'post', [Query::on('$id', 'authorId')]);
        $sorted = static function (array $ids): array {
            /** @var array<string> $ids */
            \sort($ids);

            return $ids;
        };

        foreach (['quick fox', '"quick fox"', 'lazy'] as $term) {
            $matching = $sorted(\array_map(
                static function (Document $post): string {
                    $author = $post->getAttribute('authorId');
                    self::assertIsString($author);

                    return $author;
                },
                $database->find($posts, [Query::search('body', $term)]),
            ));
            $this->assertNotSame([], $matching, $term);

            $found = $sorted(\array_map(
                static fn (Document $author): string => $author->getId(),
                $database->find($authors, [$join, Query::search('post.body', $term)]),
            ));
            $this->assertSame($matching, $found, $term);
            $this->assertSame(\count($matching), $database->count($authors, [$join, Query::search('post.body', $term)]), $term);

            $complement = $sorted(\array_values(\array_diff(\array_keys($bodies), $matching)));
            $found = $sorted(\array_map(
                static fn (Document $author): string => $author->getId(),
                $database->find($authors, [$join, Query::notSearch('post.body', $term)]),
            ));
            $this->assertSame($complement, $found, $term);
        }

        $this->assertSame(['brown', 'foxes', 'phrase'], $sorted(\array_map(
            static fn (Document $author): string => $author->getId(),
            $database->find($authors, [$join, Query::search('post.body', 'quick fox')]),
        )));

        $this->cleanupAggCollections($database, $collections);
    }

    public function testJoinedFiltersMatchWhatTheJoinedCollectionMatches(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $themes = 'jconv_themes';
        $tickets = 'jconv_tickets';
        $collections = [$themes, $tickets];
        $this->cleanupAggCollections($database, $collections);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(id: $themes, permissions: $permissions, documentSecurity: false));
        $database->createAttribute($themes, Attribute::string(key: 'tags', size: 32, array: true));
        $database->createAttribute($themes, Attribute::datetime(key: 'when'));
        $database->createCollection(Collection::create(id: $tickets, permissions: $permissions, documentSecurity: false));
        $database->createAttribute($tickets, Attribute::string(key: 'theme', size: 64));
        $database->createAttribute($tickets, Attribute::integer(key: 'amount'));

        foreach ([
            't1' => [['banana'], '2024-01-01T09:00:00.000+00:00'],
            't2' => [['a', 'b'], '2024-01-01T07:00:00.000+00:00'],
            't3' => [['b', 'c'], '2024-01-01T11:00:00.000+00:00'],
        ] as $id => [$tags, $when]) {
            $database->createDocument($themes, new Document(['$id' => $id, 'tags' => $tags, 'when' => $when]));
        }
        foreach (['k1' => ['t1', 1], 'k2' => ['t2', 10], 'k3' => ['t3', 100], 'k4' => ['missing', 1000]] as $id => [$theme, $amount]) {
            $database->createDocument($tickets, new Document(['$id' => $id, 'theme' => $theme, 'amount' => $amount]));
        }

        $later = '2024-01-01T10:00:00.000+02:00';
        $filters = [
            'containsAny' => [Query::containsAny('th.tags', ['a']), Query::containsAny('tags', ['a']), ['k2']],
            'containsAll' => [Query::containsAll('th.tags', ['a', 'b']), Query::containsAll('tags', ['a', 'b']), ['k2']],
            'notContains' => [Query::notContains('th.tags', ['a']), Query::notContains('tags', ['a']), ['k1', 'k3']],
            'greaterThan with an offset' => [Query::greaterThan('th.when', $later), Query::greaterThan('when', $later), ['k1', 'k3']],
            'equal in UTC' => [Query::equal('th.when', ['2024-01-01T09:00:00.000+00:00']), Query::equal('when', ['2024-01-01T09:00:00.000+00:00']), ['k1']],
            'equal with an offset' => [Query::equal('th.when', ['2024-01-01T11:00:00.000+02:00']), Query::equal('when', ['2024-01-01T11:00:00.000+02:00']), ['k1']],
        ];
        $amounts = ['k1' => 1, 'k2' => 10, 'k3' => 100, 'k4' => 1000];
        $ids = static function (array $documents): array {
            /** @var array<Document> $documents */
            $ids = \array_map(static fn (Document $document): string => $document->getId(), $documents);
            \sort($ids);

            return $ids;
        };
        $themeOf = ['t1' => 'k1', 't2' => 'k2', 't3' => 'k3'];

        foreach ($filters as $name => [$joined, $direct, $expected]) {
            $this->assertSame($expected, \array_map(
                static fn (string $theme): string => $themeOf[$theme],
                $ids($database->find($themes, [$direct])),
            ), $name.': the same filter on the joined collection');

            $join = Query::join($themes, 'th', [Query::on('theme', '$id')]);
            $this->assertSame($expected, $ids($database->find($tickets, [$join, $joined])), $name.': find()');
            $this->assertSame(\count($expected), $database->count($tickets, [$join, $joined]), $name.': count()');
            $this->assertEquals(
                \array_sum(\array_map(static fn (string $ticket): int => $amounts[$ticket], $expected)),
                $database->sum($tickets, 'amount', [$join, $joined]),
                $name.': sum()',
            );

            if ($joined->getMethod() !== Method::ContainsAll) {
                $onList = Query::join($themes, 'th', [Query::on('theme', '$id'), $joined]);
                $this->assertSame($expected, $ids($database->find($tickets, [$onList])), $name.': find() with the filter in the ON list');
                $this->assertSame(\count($expected), $database->count($tickets, [$onList]), $name.': count() with the filter in the ON list');
            }
        }

        $grouped = $database->aggregate($tickets, [
            Query::join($themes, 'th', [Query::on('theme', '$id')]),
            Query::count('*', 'total'),
            Query::groupBy(['th.when']),
            Query::having([Query::greaterThan('th.when', $later)]),
        ]);
        $this->assertCount(2, $grouped, 'having on a joined grouped datetime');

        $this->cleanupAggCollections($database, $collections);
    }

    /**
     * @return iterable<string, array{Method, list<string>}>
     */
    public static function joinCursorShapes(): iterable
    {
        $inner = ['a1/n1', 'a1/n2', 'a1/n3', 'a2/n4', 'a2/n6'];

        yield 'inner join' => [Method::Join, $inner];
        yield 'left join' => [Method::LeftJoin, [...$inner, 'a3/-']];
        yield 'right join' => [Method::RightJoin, [...$inner, '-/n5']];
        yield 'full outer join' => [Method::FullOuterJoin, [...$inner, 'a3/-', '-/n5']];
    }

    /**
     * @param  list<string>  $rows
     */
    #[DataProvider('joinCursorShapes')]
    public function testJoinCursorPagesEveryJoinedRowOnce(Method $join, array $rows): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$authors, $notes] = $this->seedJoinCursorFixture($database);
        $joinQuery = new Query($join, $notes, [Query::on('$id', 'author')], 'n');

        foreach ([
            'joined ascending' => [Query::orderAsc('n.score')],
            'joined descending' => [Query::orderDesc('n.score')],
            'main attribute' => [Query::orderAsc('score')],
            'default order' => [],
        ] as $label => $order) {
            $queries = [$joinQuery, ...$order];
            $all = \array_values($database->find($authors, [...$queries, Query::limit(100)]));
            $keys = \array_map($this->joinCursorKey(...), $all);
            $sorted = $keys;
            \sort($sorted);
            $expected = $rows;
            \sort($expected);
            $this->assertSame($expected, $sorted, "{$label}: the unpaged read returns each joined row once");

            foreach ($all as $index => $row) {
                $this->assertSame(\array_slice($keys, $index + 1), $this->joinCursorKeys($database, $authors, [...$queries, Query::cursorAfter($row)]), "{$label}: after {$keys[$index]}");
                $this->assertSame(\array_slice($keys, 0, $index), $this->joinCursorKeys($database, $authors, [...$queries, Query::cursorBefore($row)]), "{$label}: before {$keys[$index]}");
            }

            $forward = [];
            $cursor = null;
            for ($page = 0; $page <= \count($all); $page++) {
                $batch = $database->find($authors, [...$queries, Query::limit(2), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
                \array_push($forward, ...\array_map($this->joinCursorKey(...), $batch));
                if (\count($batch) < 2) {
                    break;
                }
                $cursor = $batch[1];
            }
            $this->assertSame($keys, $forward, "{$label}: paging forward in pages of two");

            $backward = [];
            $cursor = $all[\count($all) - 1];
            for ($page = 0; $page <= \count($all); $page++) {
                $batch = $database->find($authors, [...$queries, Query::limit(2), Query::cursorBefore($cursor)]);
                $backward = [...\array_map($this->joinCursorKey(...), $batch), ...$backward];
                if (\count($batch) < 2) {
                    break;
                }
                $cursor = $batch[0];
            }
            $this->assertSame(\array_slice($keys, 0, -1), $backward, "{$label}: paging backward in pages of two from the last row");

            $this->assertSame([], $database->find($authors, [...$queries, Query::cursorAfter($all[\count($all) - 1])]), "{$label}: after the last row");
        }

        $this->cleanupAggCollections($database, [$authors, $notes]);
    }

    public function testJoinCursorRefusesACursorThatDoesNotNameTheRow(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$authors, $notes] = $this->seedJoinCursorFixture($database);
        $join = Query::join($notes, 'n', [Query::on('$id', 'author')]);

        $withoutValue = $database->find($authors, [$join, Query::orderAsc('n.score'), Query::limit(1)])[0];
        $withoutValue->removeAttribute('n.score');
        $otherShape = $database->find($authors, [Query::join($notes, 'other', [Query::on('$id', 'author')]), Query::orderAsc('score'), Query::limit(1)])[0];

        foreach ([
            'a cursor without its joined order value' => [$withoutValue, [$join, Query::orderAsc('n.score')], 'n.score'],
            'a cursor from another join shape' => [$otherShape, [$join, Query::orderAsc('score')], 'n.$id'],
            'a document read without the join' => [$database->getDocument($authors, 'a1'), [$join, Query::orderAsc('score')], 'n.$id'],
        ] as $label => [$cursor, $queries, $missing]) {
            try {
                $database->find($authors, [...$queries, Query::cursorAfter($cursor)]);
                $this->fail("{$label} is refused");
            } catch (OrderException $exception) {
                $this->assertSame($missing, $exception->getAttribute(), $label);
                $this->assertStringContainsString("Cursor has no value for order attribute '{$missing}'", $exception->getMessage(), $label);
            }
        }

        $this->cleanupAggCollections($database, [$authors, $notes]);
    }

    public function testJoinCursorPagesADistinctReadByItsOrderValues(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$authors, $notes] = $this->seedJoinCursorFixture($database);

        foreach ([
            'distinct read' => [$notes, [Query::distinct(), Query::select(['label']), Query::orderAsc('label')], 'label', ['x', 'y', 'z']],
            'distinct read over a join' => [$authors, [Query::join($notes, 'n', [Query::on('$id', 'author')]), Query::distinct(), Query::select(['n.label']), Query::orderDesc('n.label')], 'n.label', ['y', 'x']],
        ] as $label => [$collection, $queries, $attribute, $values]) {
            $paged = [];
            $cursor = null;
            for ($page = 0; $page <= \count($values); $page++) {
                $rows = $database->find($collection, [...$queries, Query::limit(1), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
                if ($rows === []) {
                    break;
                }
                $paged[] = $rows[0]->getAttribute($attribute);
                $cursor = $rows[0];
            }
            $this->assertSame($values, $paged, $label);
        }

        $iterated = [];
        foreach ($database->cursor($notes, [Query::distinct(), Query::select(['label']), Query::orderAsc('label')], batchSize: 1) as $row) {
            $iterated[] = $row->getAttribute('label');
            if (\count($iterated) > 3) {
                break;
            }
        }
        $this->assertSame(['x', 'y', 'z'], $iterated);

        $queries = [Query::distinct(), Query::select(['label', 'score']), Query::orderAsc('label')];
        try {
            $database->find($notes, [...$queries, Query::cursorAfter($database->find($notes, [...$queries, Query::limit(1)])[0])]);
            $this->fail('A distinct read whose order leaves a selected attribute out cannot be paged');
        } catch (QueryException $exception) {
            $this->assertStringContainsString("'score' is not ordered", $exception->getMessage());
        }

        $this->cleanupAggCollections($database, [$authors, $notes]);
    }

    public function testJoinedGetDocumentPairsTheLowestSequenceJoinedRow(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$authors, $notes] = $this->seedJoinCursorFixture($database);
        $drafts = 'jcur_drafts';
        $this->cleanupAggCollections($database, [$drafts]);
        $database->createCollection(Collection::create(
            id: $drafts,
            attributes: [Attribute::string(key: 'author', size: 16), Attribute::string(key: 'label', size: 16)],
            indexes: [Index::key('author_label', ['author', 'label'])],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        foreach (['d-first' => 'z', 'd-second' => 'm', 'd-third' => 'a'] as $id => $label) {
            $database->createDocument($drafts, new Document(['$id' => $id, 'author' => 'a1', 'label' => $label, '$permissions' => [Permission::read(Role::any())]]));
        }

        foreach ([Method::Join, Method::LeftJoin, Method::RightJoin, Method::FullOuterJoin] as $join) {
            $document = $database->getDocument($authors, 'a1', [new Query($join, $drafts, [Query::on('$id', 'author')], 'd')]);
            $this->assertSame('d-first', $document->getAttribute('d.$id'), $join->value);
        }

        $this->cleanupAggCollections($database, [$authors, $notes, $drafts]);
    }

    public function testCursorIterationBuildsEachBatchFromTheCallerQueries(): void
    {
        $database = static::getDatabase();
        $items = 'jcur_items';
        $this->cleanupAggCollections($database, [$items]);
        $database->createCollection(Collection::create(
            id: $items,
            attributes: [Attribute::string(key: 'name', size: 16)],
            permissions: [Permission::create(Role::any()), Permission::read(Role::any())],
        ));
        for ($number = 1; $number <= 10; $number++) {
            $id = \sprintf('i%02d', $number);
            $database->createDocument($items, new Document(['$id' => $id, 'name' => $id, '$permissions' => [Permission::read(Role::any())]]));
        }

        $all = ['i01', 'i02', 'i03', 'i04', 'i05', 'i06', 'i07', 'i08', 'i09', 'i10'];
        foreach ([
            'an offset applies once' => [[Query::offset(2)], \array_slice($all, 2)],
            'a cursor starts the iteration, which then ends' => [[Query::cursorAfter($database->getDocument($items, 'i04'))], \array_slice($all, 4)],
            'a limit caps the iteration' => [[Query::limit(4)], \array_slice($all, 0, 4)],
            'a limit and an offset' => [[Query::offset(5), Query::limit(4)], \array_slice($all, 5, 4)],
        ] as $label => [$queries, $expected]) {
            foreach ([1, 3, 100] as $batchSize) {
                $ids = [];
                foreach ($database->cursor($items, $queries, $batchSize) as $item) {
                    $ids[] = $item->getId();
                    if (\count($ids) > 20) {
                        break;
                    }
                }
                $this->assertSame($expected, $ids, "{$label}, batches of {$batchSize}");
            }
        }

        try {
            $database->find($items, [Query::cursorAfter(new Document(['$collection' => $items, 'name' => 'i01']))]);
            $this->fail('A read without joins still refuses a cursor document without an id');
        } catch (QueryException $exception) {
            $this->assertStringContainsString('Invalid cursor', $exception->getMessage());
        }

        $this->cleanupAggCollections($database, [$items]);
    }

    /**
     * @return array{string, string}
     */
    private function seedJoinCursorFixture(Database $database): array
    {
        $authors = 'jcur_authors';
        $notes = 'jcur_notes';
        $this->cleanupAggCollections($database, [$authors, $notes]);

        $permissions = [Permission::create(Role::any()), Permission::read(Role::any())];
        $database->createCollection(Collection::create(
            id: $authors,
            attributes: [Attribute::string(key: 'name', size: 16), Attribute::integer(key: 'score')],
            permissions: $permissions,
        ));
        $database->createCollection(Collection::create(
            id: $notes,
            attributes: [Attribute::string(key: 'author', size: 16), Attribute::integer(key: 'score'), Attribute::string(key: 'label', size: 16)],
            permissions: $permissions,
        ));

        foreach (['a1' => 1, 'a2' => 2, 'a3' => 3] as $id => $score) {
            $database->createDocument($authors, new Document(['$id' => $id, 'name' => $id, 'score' => $score, '$permissions' => [Permission::read(Role::any())]]));
        }
        foreach ([
            'n1' => ['a1', 1, 'x'],
            'n2' => ['a1', 1, 'x'],
            'n3' => ['a1', 2, 'y'],
            'n4' => ['a2', 1, 'y'],
            'n5' => ['zz', 9, 'z'],
            'n6' => ['a2', null, 'x'],
        ] as $id => [$author, $score, $label]) {
            $database->createDocument($notes, new Document(['$id' => $id, 'author' => $author, 'score' => $score, 'label' => $label, '$permissions' => [Permission::read(Role::any())]]));
        }

        return [$authors, $notes];
    }

    /**
     * @param  list<Query>  $queries
     * @return list<string>
     */
    private function joinCursorKeys(Database $database, string $collection, array $queries): array
    {
        return \array_values(\array_map($this->joinCursorKey(...), $database->find($collection, [...$queries, Query::limit(100)])));
    }

    private function joinCursorKey(Document $row): string
    {
        $joined = $row->getAttribute('n.$id');

        return ($row->getId() === '' ? '-' : $row->getId()).'/'.(\is_string($joined) ? $joined : '-');
    }

    /**
     * An order on a bare name only the join declares (`label`) pages like the qualified `n.label`:
     * after and before every row, in pages of two both ways, through tied labels and the rows an
     * outer join left without a note. A name two joins declare is refused.
     *
     * @param  list<string>  $rows
     */
    #[DataProvider('joinCursorShapes')]
    public function testJoinCursorPagesAlongABareJoinedOrder(Method $join, array $rows): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$authors, $notes] = $this->seedJoinCursorFixture($database);
        $joinQuery = new Query($join, $notes, [Query::on('$id', 'author')], 'n');

        foreach (['ascending' => true, 'descending' => false] as $label => $ascending) {
            $queries = [$joinQuery, $ascending ? Query::orderAsc('label') : Query::orderDesc('label')];
            $qualified = [$joinQuery, $ascending ? Query::orderAsc('n.label') : Query::orderDesc('n.label')];

            $keys = $this->joinCursorKeys($database, $authors, $qualified);
            $sorted = $keys;
            \sort($sorted);
            $expected = $rows;
            \sort($expected);
            $this->assertSame($expected, $sorted, "{$label}: the qualified read returns each joined row once");
            $this->assertSame($keys, $this->joinCursorKeys($database, $authors, $queries), "{$label}: the bare name orders by the joined attribute");

            $all = \array_values($database->find($authors, [...$queries, Query::limit(100)]));
            foreach ($all as $index => $row) {
                $this->assertSame(\array_slice($keys, $index + 1), $this->joinCursorKeys($database, $authors, [...$queries, Query::cursorAfter($row)]), "{$label}: after {$keys[$index]}");
                $this->assertSame(\array_slice($keys, 0, $index), $this->joinCursorKeys($database, $authors, [...$queries, Query::cursorBefore($row)]), "{$label}: before {$keys[$index]}");
            }

            $forward = [];
            $cursor = null;
            for ($page = 0; $page <= \count($all); $page++) {
                $batch = $database->find($authors, [...$queries, Query::limit(2), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
                \array_push($forward, ...\array_map($this->joinCursorKey(...), $batch));
                if (\count($batch) < 2) {
                    break;
                }
                $cursor = $batch[1];
            }
            $this->assertSame($keys, $forward, "{$label}: paging forward in pages of two");

            $backward = [];
            $cursor = $all[\count($all) - 1];
            for ($page = 0; $page <= \count($all); $page++) {
                $batch = $database->find($authors, [...$queries, Query::limit(2), Query::cursorBefore($cursor)]);
                $backward = [...\array_map($this->joinCursorKey(...), $batch), ...$backward];
                if (\count($batch) < 2) {
                    break;
                }
                $cursor = $batch[0];
            }
            $this->assertSame(\array_slice($keys, 0, -1), $backward, "{$label}: paging backward in pages of two from the last row");
        }

        $twice = [Query::leftJoin($notes, 'n', [Query::on('$id', 'author')]), Query::leftJoin($notes, 'm', [Query::on('$id', 'author')])];
        $cursor = $database->find($authors, [...$twice, Query::orderAsc('n.label'), Query::limit(1)])[0];
        try {
            $database->find($authors, [...$twice, Query::orderAsc('label'), Query::cursorAfter($cursor)]);
            $this->fail('A bare name two joins declare must be refused, not read from one of them');
        } catch (QueryException $exception) {
            $this->assertStringContainsString('Attribute "label" is ambiguous across joins; qualify it with a join alias', $exception->getMessage());
        }

        $this->cleanupAggCollections($database, [$authors, $notes]);
    }

    /**
     * `alias.*` returns the joined row as a direct read of the joined collection returns it: its `$id`, `$sequence`,
     * `$createdAt`, `$updatedAt` and `$permissions` next to its attributes, never its `$tenant`, alone, next to main
     * attributes and next to `*`. A joined internal attribute named next to `*` is returned as well. A row an outer
     * join left without a note holds null for each of them.
     *
     * @param  list<string>  $rows
     */
    #[DataProvider('joinCursorShapes')]
    public function testJoinWildcardSelectReturnsTheJoinedInternalAttributes(Method $join, array $rows): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$authors, $notes] = $this->seedJoinCursorFixture($database);
        $joinQuery = new Query($join, $notes, [Query::on('$id', 'author')], 'n');

        $direct = [];
        foreach ($database->find($notes, [Query::limit(100)]) as $note) {
            $direct[$note->getId()] = $note;
        }

        $internals = [Document::ID, Document::SEQUENCE, Document::CREATED_AT, Document::UPDATED_AT, Document::PERMISSIONS];
        $expected = $rows;
        \sort($expected);

        foreach ([
            'the alias wildcard' => [['n.*'], $internals],
            'a main attribute and the alias wildcard' => [['name', 'n.*'], $internals],
            'every attribute and the alias wildcard' => [['*', 'n.*'], $internals],
            'every attribute and the joined creation time' => [['*', 'n.$createdAt'], [Document::ID, Document::CREATED_AT]],
            'every attribute, the joined sequence and update time' => [['*', 'n.$sequence', 'n.$updatedAt'], [Document::ID, Document::SEQUENCE, Document::UPDATED_AT]],
        ] as $label => [$select, $returned]) {
            $found = $database->find($authors, [$joinQuery, Query::select($select), Query::limit(100)]);
            $keys = \array_map($this->joinCursorKey(...), $found);
            \sort($keys);
            $this->assertSame($expected, $keys, "{$label}: each joined row once, with its joined \$id");

            foreach ($found as $row) {
                $this->assertFalse($row->offsetExists('n.'.Document::TENANT), "{$label}: the joined \$tenant is not returned");

                $id = $row->getAttribute('n.$id');
                if ($id === null) {
                    foreach ($returned as $internal) {
                        $this->assertTrue($row->offsetExists('n.'.$internal), "{$label}: an unmatched row returns n.{$internal}");
                        $this->assertNull($row->getAttribute('n.'.$internal), "{$label}: an unmatched row holds null for n.{$internal}");
                    }

                    continue;
                }

                $this->assertIsString($id);
                $note = $direct[$id];
                foreach ($returned as $internal) {
                    $this->assertNotNull($note->getAttribute($internal), "{$label}: the direct read returns {$internal}");
                    $this->assertSame($note->getAttribute($internal), $row->getAttribute('n.'.$internal), "{$label}: n.{$internal} of {$id} as a direct read returns it");
                }

                if (\in_array('n.*', $select, true)) {
                    $this->assertSame($note->getAttribute('label'), $row->getAttribute('n.label'), "{$label}: the joined attributes of {$id}");
                }
                if (\in_array('*', $select, true)) {
                    $this->assertSame($row->getId() === '' ? null : $row->getId(), $row->getAttribute('name'), "{$label}: every main attribute");
                    $this->assertSame($note->getAttribute('label'), $row->getAttribute('n.label'), "{$label}: the joined attributes of {$id} next to *");
                }
            }
        }

        $this->cleanupAggCollections($database, [$authors, $notes]);
    }

    /**
     * A joined read ordered by a joined internal attribute pages without a select naming that attribute: without a
     * select, with `*`, with `alias.*` and with main attributes next to `alias.*`, in pages of two after and before
     * every page, each joined row exactly once in both directions, also through `cursor()`. A read selecting
     * `alias.*` pages along the default order as well, which orders by the joined `$id`.
     *
     * @param  list<string>  $rows
     */
    #[DataProvider('joinCursorShapes')]
    public function testJoinCursorPagesAlongAJoinedInternalAttribute(Method $join, array $rows): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$authors, $notes] = $this->seedJoinCursorFixture($database);
        $joinQuery = new Query($join, $notes, [Query::on('$id', 'author')], 'n');
        $expected = $rows;
        \sort($expected);

        foreach ([
            'joined sequence ascending' => [Query::orderAsc('n.$sequence')],
            'joined sequence descending' => [Query::orderDesc('n.$sequence')],
            'joined creation time ascending' => [Query::orderAsc('n.$createdAt')],
            'joined creation time descending' => [Query::orderDesc('n.$createdAt')],
            'default order' => [],
        ] as $orderLabel => $order) {
            foreach ([
                'no select' => [],
                'every attribute' => [Query::select(['*'])],
                'the alias wildcard' => [Query::select(['n.*'])],
                'a main attribute and the alias wildcard' => [Query::select(['name', 'n.*'])],
            ] as $selectLabel => $select) {
                $label = "{$orderLabel}, {$selectLabel}";
                $queries = [$joinQuery, ...$select, ...$order];

                $all = \array_values($database->find($authors, [...$queries, Query::limit(100)]));
                $keys = \array_map($this->joinCursorKey(...), $all);
                $sorted = $keys;
                \sort($sorted);
                $this->assertSame($expected, $sorted, "{$label}: the unpaged read returns each joined row once");

                $forward = [];
                $cursor = null;
                $pages = 0;
                for ($page = 0; $page <= \count($all); $page++) {
                    $batch = $database->find($authors, [...$queries, Query::limit(2), ...($cursor === null ? [] : [Query::cursorAfter($cursor)])]);
                    \array_push($forward, ...\array_map($this->joinCursorKey(...), $batch));
                    $pages++;
                    if (\count($batch) < 2) {
                        break;
                    }
                    $cursor = $batch[1];
                }
                $this->assertGreaterThanOrEqual(3, $pages, "{$label}: the read spans at least three pages");
                $this->assertSame($keys, $forward, "{$label}: paging forward in pages of two");

                $backward = [];
                $cursor = $all[\count($all) - 1];
                for ($page = 0; $page <= \count($all); $page++) {
                    $batch = $database->find($authors, [...$queries, Query::limit(2), Query::cursorBefore($cursor)]);
                    $backward = [...\array_map($this->joinCursorKey(...), $batch), ...$backward];
                    if (\count($batch) < 2) {
                        break;
                    }
                    $cursor = $batch[0];
                }
                $this->assertSame(\array_slice($keys, 0, -1), $backward, "{$label}: paging backward in pages of two from the last row");

                $iterated = [];
                foreach ($database->cursor($authors, $queries, 2) as $row) {
                    $iterated[] = $this->joinCursorKey($row);
                    if (\count($iterated) > \count($all)) {
                        break;
                    }
                }
                $this->assertSame($keys, $iterated, "{$label}: cursor() in batches of two");
            }
        }

        $this->cleanupAggCollections($database, [$authors, $notes]);
    }

    public function testFullOuterJoinInRandomOrderReturnsEveryRow(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $customers = 'j47a_random_customers';
        $notes = 'j47a_random_notes';
        $this->cleanupAggCollections($database, [$customers, $notes]);

        $database->createCollection(Collection::create(id: $customers, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($customers, Attribute::string(key: 'name', size: 16, required: true));
        $database->createCollection(Collection::create(id: $notes, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($notes, Attribute::string(key: 'customerId', size: 16, required: true));
        $database->createAttribute($notes, Attribute::string(key: 'body', size: 16, required: true));

        foreach (['c1', 'c2', 'c3'] as $customer) {
            $database->createDocument($customers, new Document(['$id' => $customer, 'name' => $customer]));
        }
        foreach (['n1' => 'c1', 'n2' => 'c1', 'n3' => 'c2', 'n4' => 'cx'] as $note => $customer) {
            $database->createDocument($notes, new Document(['$id' => $note, 'customerId' => $customer, 'body' => $note]));
        }

        $join = Query::fullOuterJoin($notes, 'note', [Query::on('$id', 'customerId')]);
        $select = Query::select(['name', 'note.body']);
        /**
         * @param array<Document> $documents
         * @return list<string>
         */
        $rows = static function (array $documents): array {
            /** @var array<Document> $documents */
            $rows = [];
            foreach ($documents as $document) {
                $rows[] = \json_encode([$document->getAttribute('name'), $document->getAttribute('note.body')], JSON_THROW_ON_ERROR);
            }
            \sort($rows);

            return $rows;
        };

        try {
            $expected = $rows($database->find($customers, [$join, $select]));
            $this->assertCount(5, $expected);
            $this->assertSame($expected, $rows($database->find($customers, [$join, $select, Query::orderRandom(), Query::limit(100)])));
        } finally {
            $this->cleanupAggCollections($database, [$customers, $notes]);
        }
    }

    /**
     * A left-joined read ordered by main attributes up to a unique one returns every window and every cursor page of
     * the unpaged read: MariaDB and MySQL pick the main rows a page can reach before they join them, through main
     * documents without a readable note, hidden main documents, ties and nulls in the main order.
     */
    public function testLeftJoinedPagesMatchTheUnpagedRead(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $authors = 'j65_authors';
        $notes = 'j65_notes';
        $this->cleanupAggCollections($database, [$authors, $notes]);
        $permissions = [Permission::create(Role::any())];
        $database->createCollection(Collection::create(
            id: $authors,
            attributes: [Attribute::string(key: 'name', size: 16), Attribute::integer(key: 'score', required: false)],
            permissions: $permissions,
            documentSecurity: true,
        ));
        $database->createCollection(Collection::create(
            id: $notes,
            attributes: [Attribute::string(key: 'author', size: 16), Attribute::integer(key: 'score', required: false)],
            permissions: $permissions,
            documentSecurity: true,
        ));

        $readable = [Permission::read(Role::any())];
        $hidden = [Permission::read(Role::user('j65-nobody'))];
        foreach ([
            'a1' => [2, true], 'a2' => [1, true], 'a3' => [null, true], 'a4' => [2, false], 'a5' => [3, true],
            'a6' => [1, true], 'a7' => [null, false], 'a8' => [2, true], 'a9' => [4, true],
        ] as $id => [$score, $visible]) {
            $database->createDocument($authors, new Document(['$id' => $id, 'name' => 'author '.$id, 'score' => $score, '$permissions' => $visible ? $readable : $hidden]));
        }
        foreach ([
            'n1' => ['a1', 1, true], 'n2' => ['a1', 2, true], 'n3' => ['a1', 1, true], 'n4' => ['a2', 5, true],
            'n5' => ['a3', null, true], 'n6' => ['a3', 2, false], 'n7' => ['a4', 1, true], 'n8' => ['a5', 1, false],
            'n9' => ['a6', 3, true], 'n10' => ['a6', 3, true], 'n11' => ['a9', 1, true], 'n12' => ['zz', 1, true],
        ] as $id => [$author, $score, $visible]) {
            $database->createDocument($notes, new Document(['$id' => $id, 'author' => $author, 'score' => $score, '$permissions' => $visible ? $readable : $hidden]));
        }

        $join = Query::leftJoin($notes, 'n', [Query::on('$id', 'author')]);

        try {
            foreach ([
                'default order' => [],
                'score' => [Query::orderAsc('score')],
                'score descending' => [Query::orderDesc('score')],
                '$id descending' => [Query::orderDesc('$id')],
                'score, then the joined score' => [Query::orderAsc('score'), Query::orderAsc('$sequence'), Query::orderDesc('n.score')],
            ] as $label => $order) {
                $queries = [$join, ...$order];
                $all = \array_values($database->find($authors, [...$queries, Query::limit(100)]));
                $keys = \array_map($this->joinCursorKey(...), $all);
                $this->assertCount(10, $keys, "{$label}: every visible author with each visible note, or none");

                foreach ([1, 2, 3] as $limit) {
                    for ($offset = 0; $offset <= \count($keys); $offset++) {
                        $this->assertSame(
                            \array_slice($keys, $offset, $limit),
                            \array_map($this->joinCursorKey(...), $database->find($authors, [...$queries, Query::limit($limit), Query::offset($offset)])),
                            "{$label}: limit {$limit}, offset {$offset}",
                        );
                    }
                }

                foreach ($all as $index => $row) {
                    foreach ([1, 2] as $limit) {
                        $this->assertSame(
                            \array_slice($keys, $index + 1, $limit),
                            \array_map($this->joinCursorKey(...), $database->find($authors, [...$queries, Query::cursorAfter($row), Query::limit($limit)])),
                            "{$label}: {$limit} after {$keys[$index]}",
                        );
                        $this->assertSame(
                            \array_slice($keys, \max(0, $index - $limit), \min($limit, $index)),
                            \array_map($this->joinCursorKey(...), $database->find($authors, [...$queries, Query::cursorBefore($row), Query::limit($limit)])),
                            "{$label}: {$limit} before {$keys[$index]}",
                        );
                    }
                }

                $iterated = [];
                foreach ($database->cursor($authors, $queries, 2) as $row) {
                    $iterated[] = $this->joinCursorKey($row);
                }
                $this->assertSame($keys, $iterated, "{$label}: cursor()");
            }

            if ($database->getAdapter()->supports(Capability::IndexFulltext)) {
                $database->createIndex($authors, Index::fulltext(key: 'j65_name', attributes: ['name']));
                $queries = [$join, Query::search('name', 'author')];
                $keys = \array_map($this->joinCursorKey(...), \array_values($database->find($authors, [...$queries, Query::limit(100)])));
                $this->assertCount(10, $keys, 'search: every visible author with each visible note, or none');
                for ($offset = 0; $offset <= \count($keys); $offset++) {
                    $this->assertSame(
                        \array_slice($keys, $offset, 2),
                        \array_map($this->joinCursorKey(...), $database->find($authors, [...$queries, Query::limit(2), Query::offset($offset)])),
                        "search: limit 2, offset {$offset}",
                    );
                }
            }
        } finally {
            $this->cleanupAggCollections($database, [$authors, $notes]);
        }
    }

    /**
     * Inner-joined reads, left-joined reads filtered on joined attributes and searched reads, ordered by main
     * attributes up to a unique one, return every window and every cursor page of the unpaged read, through authors
     * whose notes are all hidden or fail the filter, hidden authors, and ties and nulls in the main order. MariaDB and
     * MySQL pick the main rows a left-joined searched page can reach before they join them, running the search there
     * only; the other reads keep the whole join.
     */
    public function testInnerAndFilteredJoinedPagesMatchTheUnpagedRead(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $authors = 'j67_authors';
        $notes = 'j67_notes';
        $this->cleanupAggCollections($database, [$authors, $notes]);
        $permissions = [Permission::create(Role::any())];
        $database->createCollection(Collection::create(
            id: $authors,
            attributes: [Attribute::string(key: 'name', size: 16), Attribute::integer(key: 'score', required: false)],
            permissions: $permissions,
            documentSecurity: true,
        ));
        $database->createCollection(Collection::create(
            id: $notes,
            attributes: [Attribute::string(key: 'author', size: 16), Attribute::integer(key: 'score', required: false)],
            permissions: $permissions,
            documentSecurity: true,
        ));

        $readable = [Permission::read(Role::any())];
        $hidden = [Permission::read(Role::user('j67-nobody'))];
        foreach ([
            'a1' => [2, true], 'a2' => [1, true], 'a3' => [null, true], 'a4' => [2, false], 'a5' => [3, true],
            'a6' => [1, true], 'a7' => [null, false], 'a8' => [2, true], 'a9' => [4, true],
        ] as $id => [$score, $visible]) {
            $metal = \in_array($id, ['a1', 'a3', 'a4', 'a6', 'a9'], true) ? 'gold' : 'iron';
            $database->createDocument($authors, new Document(['$id' => $id, 'name' => "author {$id} {$metal}", 'score' => $score, '$permissions' => $visible ? $readable : $hidden]));
        }
        foreach ([
            'n1' => ['a1', 1, true], 'n2' => ['a1', 2, true], 'n3' => ['a1', 1, true], 'n4' => ['a2', 5, true],
            'n5' => ['a3', null, true], 'n6' => ['a3', 2, false], 'n7' => ['a4', 1, true], 'n8' => ['a5', 1, false],
            'n9' => ['a6', 3, true], 'n10' => ['a6', 3, true], 'n11' => ['a9', 1, true], 'n12' => ['zz', 1, true],
        ] as $id => [$author, $score, $visible]) {
            $database->createDocument($notes, new Document(['$id' => $id, 'author' => $author, 'score' => $score, '$permissions' => $visible ? $readable : $hidden]));
        }

        $inner = Query::join($notes, 'n', [Query::on('$id', 'author')]);
        $left = Query::leftJoin($notes, 'n', [Query::on('$id', 'author')]);
        $everyNote = ['a1/n1', 'a1/n2', 'a1/n3', 'a2/n4', 'a3/n5', 'a6/n10', 'a6/n9', 'a9/n11'];
        $reads = [
            'inner join' => [[$inner], $everyNote],
            'left join, notes with a score of at least 1' => [[$left, Query::greaterThanEqual('n.score', 1)], ['a1/n1', 'a1/n2', 'a1/n3', 'a2/n4', 'a6/n10', 'a6/n9', 'a9/n11']],
            'inner join, notes without a score or below 3' => [[$inner, Query::or([Query::isNull('n.score'), Query::lessThan('n.score', 3)])], ['a1/n1', 'a1/n2', 'a1/n3', 'a3/n5', 'a9/n11']],
        ];

        try {
            if ($database->getAdapter()->supports(Capability::IndexFulltext)) {
                $database->createIndex($authors, Index::fulltext(key: 'j67_name', attributes: ['name']));
                $reads['inner join, searched'] = [[$inner, Query::search('name', 'gold')], ['a1/n1', 'a1/n2', 'a1/n3', 'a3/n5', 'a6/n10', 'a6/n9', 'a9/n11']];
                $reads['left join, searched'] = [[$left, Query::search('name', 'iron')], ['a2/n4', 'a5/-', 'a8/-']];
            }

            foreach ($reads as $read => [$queries, $expected]) {
                foreach ([
                    'default order' => [],
                    'score' => [Query::orderAsc('score')],
                    'score descending' => [Query::orderDesc('score')],
                    '$id descending' => [Query::orderDesc('$id')],
                ] as $order => $orders) {
                    $label = "{$read}, {$order}";
                    $all = \array_values($database->find($authors, [...$queries, ...$orders, Query::limit(100)]));
                    $keys = \array_map($this->joinCursorKey(...), $all);
                    $sorted = $keys;
                    \sort($sorted);
                    $this->assertSame($expected, $sorted, "{$label}: every matching visible author with each matching visible note");

                    foreach ([1, 2, 3] as $limit) {
                        for ($offset = 0; $offset <= \count($keys); $offset++) {
                            $this->assertSame(
                                \array_slice($keys, $offset, $limit),
                                \array_map($this->joinCursorKey(...), $database->find($authors, [...$queries, ...$orders, Query::limit($limit), Query::offset($offset)])),
                                "{$label}: limit {$limit}, offset {$offset}",
                            );
                        }
                    }

                    foreach ($all as $index => $row) {
                        foreach ([1, 2] as $limit) {
                            $this->assertSame(
                                \array_slice($keys, $index + 1, $limit),
                                \array_map($this->joinCursorKey(...), $database->find($authors, [...$queries, ...$orders, Query::cursorAfter($row), Query::limit($limit)])),
                                "{$label}: {$limit} after {$keys[$index]}",
                            );
                            $this->assertSame(
                                \array_slice($keys, \max(0, $index - $limit), \min($limit, $index)),
                                \array_map($this->joinCursorKey(...), $database->find($authors, [...$queries, ...$orders, Query::cursorBefore($row), Query::limit($limit)])),
                                "{$label}: {$limit} before {$keys[$index]}",
                            );
                        }
                    }

                    $iterated = [];
                    foreach ($database->cursor($authors, [...$queries, ...$orders], 2) as $row) {
                        $iterated[] = $this->joinCursorKey($row);
                    }
                    $this->assertSame($keys, $iterated, "{$label}: cursor()");
                }
            }
        } finally {
            $this->cleanupAggCollections($database, [$authors, $notes]);
        }
    }
}
