<?php

namespace Tests\E2E\Adapter\Scopes;

use PHPUnit\Framework\Attributes\DataProvider;
use Utopia\Database\Adapter\Postgres;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception\Authorization as AuthorizationException;
use Utopia\Database\Exception\Query as QueryException;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Index;
use Utopia\Database\Query;
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($pCol, [
            Query::leftJoin($rCol, '$id', 'prod_uid'),
            Query::count('*', 'cnt'),
            Query::avg('score', 'avg_score'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(3, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $name = $doc->getAttribute('name');
            $this->assertIsString($name);
            $mapped[$name] = $doc;
        }
        $this->assertEquals(3, $mapped['Product p1']->getAttribute('cnt'));
        $this->assertEqualsWithDelta(4.0, $this->numericAttribute($mapped['Product p1'], 'avg_score'), 0.1);
        $this->assertEquals(2, $mapped['Product p2']->getAttribute('cnt'));
        $this->assertEqualsWithDelta(3.0, $this->numericAttribute($mapped['Product p2'], 'avg_score'), 0.1);
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
        $this->assertEqualsWithDelta(300.0, $this->numericAttribute($results[0], 'avg_amount'), 0.1);
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid', 'status']),
        ]);

        $this->assertCount(4, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $custUid = $doc->getAttribute('cust_uid');
            $status = $doc->getAttribute('status');
            $this->assertIsString($custUid);
            $this->assertIsString($status);
            $key = $custUid . '_' . $status;
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::avg('amount', 'avg_amt'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::greaterThan('avg_amt', 100)]),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('c2', $results[0]->getAttribute('cust_uid'));
        $avgAmt = $results[0]->getAttribute('avg_amt');
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::min('amount', 'min_amt'),
            Query::max('amount', 'max_amt'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $cust_uid = $doc->getAttribute('cust_uid');
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            $cust_uid = $doc->getAttribute('cust_uid');
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($pCol, [
            Query::leftJoin($oCol, '$id', 'prod_uid'),
            Query::sum('amount', 'total'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $name = $doc->getAttribute('name');
            $this->assertIsString($name);
            $mapped[$name] = $doc;
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            $cust_uid = $doc->getAttribute('cust_uid');
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            $cust_uid = $doc->getAttribute('cust_uid');
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
        }

        $this->assertEquals(3, $mapped['c1']->getAttribute('cnt'));
        $this->assertEquals(600, $mapped['c1']->getAttribute('total'));
        $c1Avg = $mapped['c1']->getAttribute('avg_amt');
        $this->assertIsNumeric($c1Avg);
        $this->assertEqualsWithDelta(200.0, (float) $c1Avg, 0.1);
        $this->assertEquals(100, $mapped['c1']->getAttribute('min_amt'));
        $this->assertEquals(300, $mapped['c1']->getAttribute('max_amt'));

        $this->assertEquals(2, $mapped['c2']->getAttribute('cnt'));
        $this->assertEquals(200, $mapped['c2']->getAttribute('total'));
        $c2Avg = $mapped['c2']->getAttribute('avg_amt');
        $this->assertIsNumeric($c2Avg);
        $this->assertEqualsWithDelta(100.0, (float) $c2Avg, 0.1);
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            $cust_uid = $doc->getAttribute('cust_uid');
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
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

        $pCol = 'jtc_p_'.$joinMethod;
        $oCol = 'jtc_o_'.$joinMethod;
        $cols = [$pCol, $oCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            'join' => Query::join($oCol, '$id', 'prod_uid'),
            'leftJoin' => Query::leftJoin($oCol, '$id', 'prod_uid'),
            default => throw new \InvalidArgumentException('Unknown join method: '.$joinMethod),
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

        $oCol = 'jat_o_'.$aggMethod;
        $cCol = 'jat_c_'.$aggMethod;
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            $aggQuery,
        ]);

        $this->assertCount(1, $results);
        if ($aggMethod === 'avg') {
            $result = $results[0]->getAttribute('result');
            $this->assertIsNumeric($result);
            $this->assertEqualsWithDelta($expected, (float) $result, 0.1);
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

        $oCol = 'jho_o_'.$operator;
        $cCol = 'jho_c_'.$operator;
        $cols = [$oCol, $cCol];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderDesc('total'),
        ]);

        $this->assertCount(3, $results);
        $totals = array_map(fn (Document $d) => $this->intAttribute($d, 'total'), $results);
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'title', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            $cust_uid = $doc->getAttribute('cust_uid');
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
        }
        $this->assertEquals(3, $mapped['c1']->getAttribute('order_cnt'));
        $this->assertEquals(600, $this->intAttribute($mapped['c1'], 'total'));
        $this->assertEquals(2, $mapped['c2']->getAttribute('order_cnt'));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::equal('status', ['done']),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::orderAsc('total'),
        ]);

        $this->assertCount(3, $results);
        $totals = array_map(fn (Document $d) => $this->intAttribute($d, 'total'), $results);
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
        $this->assertEquals(1200, $this->intAttribute($results[0], 'total'));
        $this->assertEquals('c1', $results[1]->getAttribute('cust_uid'));
        $this->assertEquals(500, $this->intAttribute($results[1], 'total'));

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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::count('*', 'cnt'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid', 'status']),
            Query::having([Query::greaterThanEqual('cnt', 2)]),
        ]);

        $this->assertCount(2, $results);
        $keys = array_map(function (Document $document): string {
            $custUid = $document->getAttribute('cust_uid');
            $status = $document->getAttribute('status');
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::countDistinct('product', 'unique_products'),
            Query::groupBy(['cust_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $cust_uid = $doc->getAttribute('cust_uid');
            $this->assertIsString($cust_uid);
            $mapped[$cust_uid] = $doc;
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
        $this->assertEquals(300, $this->intAttribute($results[0], 'total'));
        $this->assertEquals('c3', $results[1]->getAttribute('cust_uid'));
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
        $results = $database->find($pCol, [
            Query::leftJoin($oCol, '$id', 'prod_uid'),
            Query::count('*', 'order_cnt'),
            Query::groupBy(['name']),
            Query::orderDesc('order_cnt'),
            Query::limit(3),
        ]);

        $this->assertCount(3, $results);
        $counts = [];
        foreach ($results as $document) {
            $count = $document->getAttribute('order_cnt');
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
        $results = $database->find($oCol, [
            Query::join($cCol, 'cust_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::groupBy(['cust_uid']),
            Query::having([Query::lessThanEqual('total', 200)]),
            Query::orderAsc('total'),
        ]);

        $this->assertCount(2, $results);
        $this->assertEquals('c1', $results[0]->getAttribute('cust_uid'));
        $c1Total = $results[0]->getAttribute('total');
        $this->assertIsNumeric($c1Total);
        $this->assertEquals(100, (int) $c1Total);
        $this->assertEquals('c2', $results[1]->getAttribute('cust_uid'));
        $c2Total = $results[1]->getAttribute('total');
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::rightJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $aCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($aCol, Attribute::string(key: 'label', size: 100, required: true));

        $database->createCollection(new Collection(id: $bCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::crossJoin($bCol),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::fullOuterJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::fullOuterJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'name', size: 100, required: true));

        try {
            $database->find($pCol, [
                Query::naturalJoin($rCol),
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

        $database->createCollection(new Collection(id: $lCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($lCol, Attribute::integer(key: 'value', required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::join($rCol, 'value', 'threshold', '>'),
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::join($oCol, '$id', 'cust_uid', '=', 'ord'),
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($oCol, Attribute::string(key: 'cust_uid', required: true));
        $database->createAttribute($oCol, Attribute::integer(key: 'amount', required: true));

        $database->createCollection(new Collection(id: $iCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::join($oCol, '$id', 'cust_uid', '=', 'ord'),
            Query::join($iCol, 'ord.$id', 'order_uid', '=', 'itm'),
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::join($oCol, '$id', 'cust_uid', '=', 'ord'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::rightJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::fullOuterJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $col, permissions: [Permission::create(Role::any())]));
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
                Query::join($col, 'tag', 'tag', '=', 'visible'),
                Query::join($col, 'tag', 'tag', '=', 'hidden'),
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any())]));
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
                Query::rightJoin($oCol, '$id', 'customerId'),
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

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($cCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $oCol, permissions: [Permission::create(Role::any())]));
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
                Query::fullOuterJoin($oCol, '$id', 'customerId'),
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

        $sharedTables = $database->getSharedTables();
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

            $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
            $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

            $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
                Query::rightJoin($rCol, '$id', 'prod_uid', '=', 'rev'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::join($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p2', [
            Query::leftJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p2', [
            Query::join($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p2', [
            Query::rightJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::join($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::join($rCol, '$id', 'prod_uid', '=', 'rev'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
                Query::join($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'name', size: 100, required: true));

        try {
            $database->getDocument($pCol, 'p1', [
                Query::naturalJoin($rCol),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::leftJoin($rCol, '$id', 'prod_uid', '=', 'rev'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::join($rCol, '$id', 'prod_uid'),
        ]);
        $this->assertSame(false, $inner->isEmpty());
        $this->assertSame('p1', $inner->getId());

        $left = $database->getDocument($pCol, 'p1', [
            Query::leftJoin($rCol, '$id', 'prod_uid'),
        ]);
        $this->assertSame(false, $left->isEmpty());
        $this->assertSame('p1', $left->getId());

        $unmatched = $database->getDocument($pCol, 'p2', [
            Query::leftJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($rCol, Attribute::string(key: 'prod_uid', required: true));
        $database->createAttribute($rCol, Attribute::integer(key: 'score', required: true));

        $database->createDocument($pCol, new Document([
            '$id' => 'p2',
            'name' => 'Product p2',
            '$permissions' => [Permission::read(Role::any())],
        ]));

        $document = $database->getDocument($pCol, 'p2', [
            Query::fullOuterJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any())]));
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
                Query::fullOuterJoin($rCol, '$id', 'prod_uid'),
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
                Query::leftJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::fullOuterJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::fullOuterJoin($rCol, '$id', 'prod_uid'),
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

        $database->createCollection(new Collection(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $jCol, permissions: [Permission::create(Role::any())], documentSecurity: false));
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
                Query::join($jCol, '$id', 'mainId'),
                Query::leftJoin($jCol, '$id', 'mainId'),
                Query::rightJoin($jCol, '$id', 'mainId'),
                Query::fullOuterJoin($jCol, '$id', 'mainId'),
                Query::crossJoin($jCol),
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
                    Query::join($jCol, '$id', 'mainId'),
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

        $database->createCollection(new Collection(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $jCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
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
                Query::join($jCol, '$id', 'mainId'),
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

        $database->createCollection(new Collection(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $jCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
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
                Query::leftJoin($jCol, '$id', 'mainId', '=', 'rev'),
                Query::select(['name', 'rev.score']),
            ]);

            $this->assertGreaterThanOrEqual(1, \count($results));
            $this->assertContains(999, $this->aliasedScores($results));

            $rewritten = Query::leftJoin('jp_dsoff_public', '$id', 'mainId', '=', 'rev');
            $rewritten->setAttribute($jCol);
            $rewrittenResults = $database->find($mCol, [
                $rewritten,
                Query::select(['name', 'rev.score']),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($rewrittenResults));
            $this->assertContains(999, $this->aliasedScores($rewrittenResults));

            $document = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($jCol, '$id', 'mainId', '=', 'rev'),
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
                Query::join($jCol, '$id', 'mainId'),
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
                Query::leftJoin($jCol, '$id', 'mainId'),
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
                Query::rightJoin($jCol, '$id', 'mainId'),
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
                Query::fullOuterJoin($jCol, '$id', 'mainId'),
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
                Query::crossJoin($jCol),
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
                Query::join($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(true, $innerSecret->isEmpty());

            $innerPublic = $database->getDocument($mCol, 'm2', [
                Query::join($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(false, $innerPublic->isEmpty());
            $this->assertSame('m2', $innerPublic->getId());
            $this->assertSecretJoinHidden($innerPublic, 'j-secret', 999);
            $publicScore = $innerPublic->getAttribute('j0.score');
            $this->assertTrue(\is_numeric($publicScore));
            $this->assertSame(10, (int) $publicScore);

            $leftSecret = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(false, $leftSecret->isEmpty());
            $this->assertSame('m1', $leftSecret->getId());
            $this->assertNotSame('j-secret', $leftSecret->getId());
            $this->assertSecretJoinHidden($leftSecret, 'j-secret', 999);
            $this->assertNullishScore($leftSecret);

            $leftPublic = $database->getDocument($mCol, 'm2', [
                Query::leftJoin($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(false, $leftPublic->isEmpty());
            $this->assertSame('m2', $leftPublic->getId());

            $rightSecret = $database->getDocument($mCol, 'm1', [
                Query::rightJoin($jCol, '$id', 'mainId'),
            ]);
            if (! $rightSecret->isEmpty()) {
                $this->assertSame('m1', $rightSecret->getId());
                $this->assertSecretJoinHidden($rightSecret, 'j-secret', 999);
            }

            $rightPublic = $database->getDocument($mCol, 'm2', [
                Query::rightJoin($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(false, $rightPublic->isEmpty());
            $this->assertSame('m2', $rightPublic->getId());
            $this->assertSecretJoinHidden($rightPublic, 'j-secret', 999);

            $fojSecret = $database->getDocument($mCol, 'm1', [
                Query::fullOuterJoin($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(false, $fojSecret->isEmpty());
            $this->assertSame('m1', $fojSecret->getId());
            $this->assertNotSame('j-secret', $fojSecret->getId());
            $this->assertSecretJoinHidden($fojSecret, 'j-secret', 999);
            $this->assertNullishScore($fojSecret);

            $fojPublic = $database->getDocument($mCol, 'm2', [
                Query::fullOuterJoin($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(false, $fojPublic->isEmpty());
            $this->assertSame('m2', $fojPublic->getId());
            $this->assertSecretJoinHidden($fojPublic, 'j-secret', 999);

            $fojUnmatched = $database->getDocument($mCol, 'm3', [
                Query::fullOuterJoin($jCol, '$id', 'mainId'),
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
                Query::leftJoin($jCol, '$id', 'mainId', '=', 'rev'),
                Query::select(['name', 'rev.score']),
            ]);
            foreach ($finds as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
            }
            $this->assertContains(10, $this->numericScores($finds));
            $this->assertSame(false, \in_array(999, $this->numericScores($finds), true));

            $document = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($jCol, '$id', 'mainId', '=', 'rev'),
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

        $sharedTables = $database->getSharedTables();
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
                    [Query::join($jCol, '$id', 'mainId')],
                    [Query::leftJoin($jCol, '$id', 'mainId')],
                    [Query::fullOuterJoin($jCol, '$id', 'mainId')],
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

        $database->createCollection(new Collection(id: $mCol, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $jCol, permissions: [Permission::create(Role::any())]));
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
                Query::join($jCol, '$id', 'mainId'),
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
                Query::join($jCol, '$id', 'mainId'),
            ]);
            $scores = $this->numericScores($results);
            $this->assertContains(999, $scores);
            $this->assertSame(false, \in_array(10, $scores, true));
            $this->assertSame(false, \in_array(20, $scores, true));
        });

        $this->withAuthorizationRoles($database, [Role::guests()->toString()], function () use ($database, $mCol, $jCol): void {
            $results = $database->find($mCol, [
                Query::join($jCol, '$id', 'mainId'),
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
                Query::join($jCol, '$id', 'mainId'),
            ]);
            $this->assertContains(10, $this->numericScores($inner));
            $this->assertSame(false, \in_array(999, $this->numericScores($inner), true));
            foreach ($inner as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
            }

            $left = $database->find($mCol, [
                Query::leftJoin($jCol, '$id', 'mainId'),
            ]);
            $this->assertGreaterThanOrEqual(1, \count($left));
            foreach ($left as $document) {
                $this->assertSecretJoinHidden($document, 'j-secret', 999);
            }

            $document = $database->getDocument($mCol, 'm1', [
                Query::join($jCol, '$id', 'mainId'),
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
            $join = Query::join($jCol, '$id', 'mainId', '=', 'rev');
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

            $aggregated = $database->find($mCol, [
                $join,
                Query::max('rev.score', 'max_score'),
                Query::groupBy(['name']),
                Query::having([Query::greaterThanEqual('max_score', 999)]),
            ]);
            $this->assertLessThanOrEqual(\count($baseline), \count($aggregated));
            $this->assertSecretJoinPayloadHidden($aggregated, 'j-secret', 999);

            $maxOnly = $database->find($mCol, [
                $join,
                Query::max('rev.score', 'max_score'),
                Query::groupBy(['name']),
            ]);
            $this->assertSecretJoinPayloadHidden($maxOnly, 'j-secret', 999);
            foreach ($maxOnly as $document) {
                $maxScore = $document->getAttribute('max_score');
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
                Query::join($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(1, \count($inner));
            $this->assertSame('m1', $inner[0]->getId());
            $this->assertContains(10, $this->numericScores($inner));
            $this->assertSecretJoinPayloadHidden($inner, 'j-secret', 999);

            $publicMains = $database->find($mCol);
            $this->assertSame(2, \count($publicMains));

            $left = $database->find($mCol, [
                Query::leftJoin($jCol, '$id', 'mainId'),
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
                Query::join($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(1, \count($inner));
            $this->assertSame('m1', $inner[0]->getId());
            $this->assertContains(10, $this->numericScores($inner));
            $this->assertSecretJoinPayloadHidden($inner, 'j-secret', 999, 'user:other');

            $left = $database->find($mCol, [
                Query::leftJoin($jCol, '$id', 'mainId'),
            ]);
            $this->assertSame(1, \count($left));
            $this->assertSame('m1', $left[0]->getId());
            $this->assertSecretJoinPayloadHidden($left, 'j-secret', 999, 'user:other');

            $document = $database->getDocument($mCol, 'm1', [
                Query::leftJoin($jCol, '$id', 'mainId'),
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

        $database->createCollection(new Collection(id: $aCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($aCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $bCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($bCol, Attribute::string(key: 'aId', required: true));

        $database->createCollection(new Collection(id: $cCol, permissions: [Permission::create(Role::any())], documentSecurity: false));
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
                    Query::join($bCol, '$id', 'aId', '=', 'b'),
                    Query::join($cCol, 'b.$id', 'bId', '=', 'c'),
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
                Query::leftJoin($jCol, '$id', 'mainId'),
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

        $database->createCollection(new Collection(id: $main, permissions: $mainGranted ? $granted : $documentLevel));
        $database->createAttribute($main, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $joined, permissions: $documentLevel));
        $database->createAttribute($joined, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($joined, Attribute::integer(key: 'score', required: true));
    }

    private function createMixedJoinPermissionCollections(Database $database, string $main, string $joined): void
    {
        $database->createCollection(new Collection(id: $main, permissions: [Permission::create(Role::any()), Permission::read(Role::any())], documentSecurity: false));
        $database->createAttribute($main, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $joined, permissions: [Permission::create(Role::any())]));
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
            Document::INTERNAL_ID,
            Document::SKIP_PERMISSIONS_UPDATE,
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

        $database->createCollection(new Collection(id: $pCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($pCol, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $rCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
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
            Query::join($first, '$id', 'mainId', '=', 'j1'),
            Query::join($second, '$id', 'mainId'),
            Query::select(['name', 'j1.score']),
        ]);
        $this->assertCount(1, $declaredFirst);
        $this->assertSame(1, $this->scoreOf($declaredFirst[0], 'j1.score'));

        $declaredLater = $database->find($main, [
            Query::join($first, '$id', 'mainId'),
            Query::join($second, '$id', 'mainId', '=', 'j0'),
            Query::select(['name', 'j0.score']),
        ]);
        $this->assertCount(1, $declaredLater);
        $this->assertSame(10, $this->scoreOf($declaredLater[0], 'j0.score'));

        foreach ([
            'the same alias twice' => [
                Query::join($first, '$id', 'mainId', '=', 'x'),
                Query::join($second, '$id', 'mainId', '=', 'x'),
            ],
            'the main collection alias' => [
                Query::join($first, '$id', 'mainId', '=', Query::DEFAULT_ALIAS),
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
            'find' => $database->find($main, [Query::join($first, '$id', 'mainId', '=', 'ord')]),
            'getDocument' => [$database->getDocument($main, 'm1', [Query::leftJoin($first, '$id', 'mainId', '=', 'ord')])],
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
        $database->createCollection(new Collection(id: $main, permissions: $permissions, documentSecurity: false));
        $database->createAttribute($main, Attribute::string(key: 'name', size: 64, required: true));
        $database->createCollection(new Collection(id: $joined, permissions: $permissions, documentSecurity: false));
        $database->createAttributes($joined, [
            Attribute::string(key: 'mainId', size: 64, required: true),
            Attribute::integer(key: 'total', required: true),
            Attribute::float(key: 'price', required: true),
            Attribute::boolean(key: 'paid', required: true),
            Attribute::datetime(key: 'placedAt', required: true),
            Attribute::string(key: 'tags', size: 32, array: true),
            Attribute::string(key: 'meta', size: 1024, filters: ['json']),
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
        $join = Query::join($joined, '$id', 'mainId', '=', 'dec');
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
                Query::fullOuterJoin($first, '$id', 'mainId', '=', 'b'),
                Query::rightJoin($second, '$id', 'mainId', '=', 'c'),
            ],
            'on the full outer joined collection' => [
                Query::fullOuterJoin($first, '$id', 'mainId', '=', 'b'),
                Query::rightJoin($second, 'b.mainId', 'mainId', '=', 'c'),
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
            Query::fullOuterJoin($first, '$id', 'mainId', '=', 'b'),
            Query::rightJoin($second, 'b.mainId', 'mainId', '=', 'c'),
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
            Query::fullOuterJoin($first, '$id', 'mainId', '=', 'b'),
            Query::fullOuterJoin($second, 'b.mainId', 'mainId', '=', 'c'),
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
        $database->createCollection(new Collection(id: $main, permissions: $permissions));
        $database->createAttribute($main, Attribute::vector(key: 'embedding', size: 3, required: true));
        $database->createCollection(new Collection(id: $meta, permissions: $permissions));
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
            Query::join($meta, '$id', 'mainId', '=', 'meta'),
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
        $join = Query::fullOuterJoin($joined, 'link', 'link', '=', 'b');

        $rows = $database->find($main, [
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

        $empty = $database->find($main, [
            $join,
            Query::equal('category', ['none']),
            Query::count('*', 'rows'),
            Query::sum('b.score', 'total'),
            Query::max('b.score', 'high'),
        ]);

        $this->assertCount(1, $empty);
        $this->assertSame(0, $this->intAttribute($empty[0], 'rows'));
        $this->assertNull($empty[0]->getAttribute('total'));
        $this->assertNull($empty[0]->getAttribute('high'));

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
            Query::fullOuterJoin($joined, 'link', 'link', '=', 'b'),
            Query::groupBy(['b.category']),
            Query::count('*', 'rows'),
            Query::sum('b.score', 'total'),
            Query::sum('score', 'mainTotal'),
            Query::orderDesc('rows'),
        ];
        $this->assertSame(
            [[null, 3, 6, 50], ['p', 2, 9, 10], ['q', 1, 7, 10]],
            $this->fullOuterJoinGroups($database->find($main, $grouped)),
        );
        $this->assertSame(
            [[null, 3, 6, 50], ['p', 2, 9, 10]],
            $this->fullOuterJoinGroups($database->find($main, [...$grouped, Query::having([Query::greaterThan('rows', 1)])])),
        );
        $this->assertSame(
            [['p', 2, 9, 10], ['q', 1, 7, 10]],
            $this->fullOuterJoinGroups($database->find($main, [...$grouped, Query::limit(2), Query::offset(1)])),
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
        $join = Query::fullOuterJoin($joined, 'link', 'link', '=', 'b');

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
            Query::fullOuterJoin($joined, 'link', 'link', '=', 'b'),
            Query::distinct(),
            Query::select(['b.category']),
            Query::orderAsc('score'),
        ]), 'find');
        $this->assertSame('A distinct() query over a full outer join can only be ordered by a selected attribute on this database, and score is not selected', $message);

        $this->cleanupAggCollections($database, $this->fullOuterJoinAggregateCollections());
    }

    public function testFullOuterJoinUnaliasedAggregatesKeepTheNamesTheEngineGivesThem(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        [$main, $joined] = $this->createFullOuterJoinAggregateCollections($database);
        $aggregates = [Query::count(), Query::sum('b.score'), Query::max('score')];

        $left = $database->find($main, [Query::leftJoin($joined, 'link', 'link', '=', 'b'), ...$aggregates]);
        $full = $database->find($main, [Query::fullOuterJoin($joined, 'link', 'link', '=', 'b'), ...$aggregates]);

        $this->assertCount(1, $left);
        $this->assertCount(1, $full);
        $this->assertSame(\array_keys($left[0]->getArrayCopy()), \array_keys($full[0]->getArrayCopy()));
        $this->assertSame(
            [6, 22, 30],
            \array_map(static fn (mixed $value): int => \is_numeric($value) ? (int) $value : -1, \array_values($full[0]->getArrayCopy())),
        );

        $this->cleanupAggCollections($database, $this->fullOuterJoinAggregateCollections());
    }

    /**
     * @param  array<Document>  $rows
     * @return list<array{mixed, int, int, int}>
     */
    private function fullOuterJoinGroups(array $rows): array
    {
        return \array_values(\array_map(
            fn (Document $row): array => [
                $row->getAttribute('category'),
                $this->intAttribute($row, 'rows'),
                $this->intAttribute($row, 'total'),
                $this->intAttribute($row, 'mainTotal'),
            ],
            $rows,
        ));
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
            $database->createCollection(new Collection(
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
        $database->createCollection(new Collection(id: $main, permissions: $permissions, documentSecurity: false));
        $database->createAttribute($main, Attribute::string(key: 'name', size: 64, required: true));
        foreach ([$first, $second] as $joined) {
            $database->createCollection(new Collection(id: $joined, permissions: $permissions, documentSecurity: false));
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
            Query::join($orders, '$id', 'customerId', '=', 'alpha'),
            Query::join($refunds, '$id', 'customerId', '=', 'beta'),
        ];

        foreach ([
            [Query::sum('amount', 'total')],
            [Query::count('*', 'rows'), Query::groupBy(['amount'])],
        ] as $aggregation) {
            try {
                $database->find($customers, [...$joins, ...$aggregation]);
                $this->fail('A bare attribute two joins declare was bound to one of them');
            } catch (QueryException $error) {
                $this->assertSame('Invalid query: Attribute "amount" is ambiguous across joins; qualify it with a join alias', $error->getMessage());
            }
        }

        $results = $database->find($customers, [
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

        $results = $database->find($customers, [
            Query::join($notes, '$id', 'customerId', '=', 'note'),
            Query::join($orders, '$id', 'customerId', '=', 'purchase'),
            Query::sum('amount', 'total'),
        ]);
        $this->assertCount(1, $results);
        $this->assertSame(150, $this->intAttribute($results[0], 'total'));

        $results = $database->find($customers, [
            Query::join($notes, '$id', 'customerId'),
            Query::join($orders, '$id', 'customerId'),
            Query::sum('amount', 'total'),
            Query::groupBy(['status']),
        ]);
        $totals = [];
        foreach ($results as $result) {
            $status = $result->getAttribute('status');
            $this->assertIsString($status);
            $totals[$status] = $this->intAttribute($result, 'total');
        }
        \ksort($totals);
        $this->assertSame(['open' => 50, 'paid' => 100], $totals);

        $results = $database->find($customers, [
            Query::leftJoin($notes, '$id', 'customerId', '=', 'note'),
            Query::count('$id', 'customers'),
        ]);
        $this->assertCount(1, $results);
        $this->assertSame(2, $this->intAttribute($results[0], 'customers'));

        $this->cleanupAggCollections($database, $collections);
    }

    public function testJoinSearchOnJoinedAttributeRequiresFulltextIndex(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins) || ! $database->getAdapter()->supports(Capability::Fulltext)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        [$customers, $orders, , $notes] = $collections = $this->seedJoinedAttributeCollections($database, 'jsfi');
        $unindexed = [
            Query::join($orders, '$id', 'customerId', '=', 'purchase'),
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
            Query::join($notes, '$id', 'customerId', '=', 'note'),
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
        $database->createCollection(new Collection(id: $customers, permissions: $permissions));
        $database->createAttribute($customers, Attribute::string(key: 'name', size: 100, required: true));

        $database->createCollection(new Collection(id: $orders, permissions: $permissions));
        $database->createAttribute($orders, Attribute::string(key: 'customerId', size: 64, required: true));
        $database->createAttribute($orders, Attribute::integer(key: 'amount', required: true));
        $database->createAttribute($orders, Attribute::string(key: 'status', size: 32, required: true));
        $database->createAttribute($orders, Attribute::string(key: 'memo', size: 256, required: true));

        $database->createCollection(new Collection(id: $refunds, permissions: $permissions));
        $database->createAttribute($refunds, Attribute::string(key: 'customerId', size: 64, required: true));
        $database->createAttribute($refunds, Attribute::integer(key: 'amount', required: true));

        $database->createCollection(new Collection(id: $notes, permissions: $permissions));
        $database->createAttribute($notes, Attribute::string(key: 'customerId', size: 64, required: true));
        $database->createAttribute($notes, Attribute::string(key: 'body', size: 256, required: true));
        if ($database->getAdapter()->supports(Capability::Fulltext)) {
            $database->createIndex($notes, Index::fullText(key: 'body_fulltext', attributes: ['body']));
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
        $database->createCollection(new Collection(id: $mCol, permissions: $granted));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createAttribute($mCol, Attribute::integer(key: 'visits', required: true));
        $database->createCollection(new Collection(id: $jCol, permissions: $granted));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::string(key: 'bio', size: 100, required: true));

        $database->createDocument($mCol, new Document(['$id' => 'open', 'name' => 'Open', 'visits' => 1, '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($mCol, new Document(['$id' => 'bare', 'name' => 'Bare', 'visits' => 10, '$permissions' => []]));
        $database->createDocument($jCol, new Document(['$id' => 'open-profile', 'mainId' => 'open', 'bio' => 'Hello', '$permissions' => [Permission::read(Role::any())]]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $join = Query::leftJoin($jCol, '$id', 'mainId', '=', 'profile');

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
        $database->createCollection(new Collection(id: $mCol, permissions: $granted));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createCollection(new Collection(id: $jCol, permissions: $granted));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::integer(key: 'amount', required: true));

        $database->createDocument($mCol, new Document(['$id' => 'customer', 'name' => 'Customer', '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($jCol, new Document(['$id' => 'public-order', 'mainId' => 'customer', 'amount' => 100, '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($jCol, new Document(['$id' => 'secret-order', 'mainId' => 'customer', 'amount' => 9999, '$permissions' => [Permission::read(Role::user('other'))]]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $join = Query::join($jCol, '$id', 'mainId', '=', 'ord');

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

        $database->createCollection(new Collection(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createCollection(new Collection(id: $jCol, permissions: [Permission::create(Role::any())]));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));
        $database->createAttribute($jCol, Attribute::string(key: 'text', size: 100, required: true));

        $database->createDocument($mCol, new Document(['$id' => 'customer', 'name' => 'Customer', '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($jCol, new Document(['$id' => 'alice-note', 'mainId' => 'customer', 'text' => 'mine', '$permissions' => [Permission::read(Role::user('alice'))]]));
        $database->createDocument($jCol, new Document(['$id' => 'bob-note', 'mainId' => 'customer', 'text' => 'theirs', '$permissions' => [Permission::read(Role::user('bob'))]]));

        $this->withAuthorizationRoles($database, [Role::any()->toString(), Role::user('alice')->toString()], function () use ($database, $mCol, $jCol): void {
            $join = Query::join($jCol, '$id', 'mainId', '=', 'note');

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

        $database->createCollection(new Collection(id: $mCol, permissions: [Permission::create(Role::any()), Permission::read(Role::any())]));
        $database->createAttribute($mCol, Attribute::string(key: 'name', size: 100, required: true));
        $database->createAttribute($mCol, Attribute::integer(key: 'visits', required: true));
        $database->createCollection(new Collection(id: $jCol, permissions: [Permission::create(Role::any())], documentSecurity: false));
        $database->createAttribute($jCol, Attribute::string(key: 'mainId', required: true));

        $database->createDocument($mCol, new Document(['$id' => 'customer', 'name' => 'Customer', 'visits' => 1, '$permissions' => [Permission::read(Role::any())]]));
        $database->createDocument($jCol, new Document(['$id' => 'entry', 'mainId' => 'customer', '$permissions' => [Permission::read(Role::any())]]));

        $this->withAuthorizationRoles($database, [Role::any()->toString()], function () use ($database, $mCol, $jCol): void {
            $join = Query::join($jCol, '$id', 'mainId', '=', 'ledger');
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
        if (! $database->getSharedTables() || ! $database->getAdapter()->supports(Capability::Joins)) {
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

    public function testSharedTablesChainedJoinsKeepTheSelectedTenantsUnmatchedRows(): void
    {
        $database = static::getDatabase();
        if (! $database->getSharedTables() || ! $database->getAdapter()->supports(Capability::Joins)) {
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
        $database->createCollection(new Collection(id: $authors, permissions: $permissions, documentSecurity: false));
        $database->createAttribute($authors, Attribute::string(key: 'name', size: 64, required: true));
        foreach ([$books => 'pages', $reviews => 'stars'] as $collection => $number) {
            $database->createCollection(new Collection(id: $collection, permissions: $permissions, documentSecurity: false));
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
            Method::Join => Query::join($collection, '$id', 'authorId', '=', $alias),
            Method::LeftJoin => Query::leftJoin($collection, '$id', 'authorId', '=', $alias),
            Method::RightJoin => Query::rightJoin($collection, '$id', 'authorId', '=', $alias),
            Method::FullOuterJoin => Query::fullOuterJoin($collection, '$id', 'authorId', '=', $alias),
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
        if (! $database->getSharedTables() || ! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();

            return;
        }

        $collections = ['jtx_authors', 'jtx_books', 'jtx_reviews'];
        [$authors, $books, $reviews] = $collections;
        $extras = 'jtx_extras';
        $tenant = $database->getTenant();

        $rowsByChain = [
            'a cross join, then a right join' => [
                'joins' => [Query::crossJoin($extras, 'extra'), Query::rightJoin($reviews, '$id', 'authorId', '=', 'review')],
                'numbers' => ['extra.weight', 'review.stars'],
                'rows' => [
                    1 => [[null, null, 5], [null, null, 4], [null, null, 3], [null, null, 2]],
                    2 => [['two-shared', 7, 1]],
                ],
            ],
            'a cross join, then a right join on it' => [
                'joins' => [Query::crossJoin($extras, 'extra'), Query::rightJoin($reviews, 'extra.authorId', 'authorId', '=', 'review')],
                'numbers' => ['extra.weight', 'review.stars'],
                'rows' => [
                    1 => [[null, null, 5], [null, null, 4], [null, null, 3], [null, null, 2]],
                    2 => [['two-a1', 7, 1], ['two-a2', 7, 1], ['two-shared', 7, 1]],
                ],
            ],
            'a right join, then a right join on it' => [
                'joins' => [Query::rightJoin($books, '$id', 'authorId', '=', 'book'), Query::rightJoin($reviews, 'book.authorId', 'authorId', '=', 'review')],
                'numbers' => ['book.pages', 'review.stars'],
                'rows' => [
                    1 => [['one-a1', 11, 5], [null, null, 4], [null, null, 3], [null, 12, 2]],
                    2 => [[null, null, 1]],
                ],
            ],
            'a full outer join, then a right join' => [
                'joins' => [Query::fullOuterJoin($books, '$id', 'authorId', '=', 'book'), Query::rightJoin($reviews, '$id', 'authorId', '=', 'review')],
                'numbers' => ['book.pages', 'review.stars'],
                'rows' => [
                    1 => [['one-a1', 11, 5], ['one-a2', null, 4], [null, null, 2], [null, null, 3]],
                    2 => [['two-shared', null, 1]],
                ],
            ],
            'a right join, then a full outer join on it' => [
                'joins' => [Query::rightJoin($books, '$id', 'authorId', '=', 'book'), Query::fullOuterJoin($reviews, 'book.authorId', 'authorId', '=', 'review')],
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
        $database->createCollection(new Collection(id: $authors, permissions: $permissions, documentSecurity: $documentSecurity));
        $database->createAttribute($authors, Attribute::string(key: 'name', size: 64, required: true));
        foreach ([$books => 'pages', $reviews => 'stars', $extras => 'weight'] as $collection => $number) {
            $database->createCollection(new Collection(id: $collection, permissions: $permissions, documentSecurity: $documentSecurity));
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
                Method::Join => Query::join($collections[$collection], $on, 'authorId', '=', $alias),
                Method::LeftJoin => Query::leftJoin($collections[$collection], $on, 'authorId', '=', $alias),
                Method::RightJoin => Query::rightJoin($collections[$collection], $on, 'authorId', '=', $alias),
                Method::FullOuterJoin => Query::fullOuterJoin($collections[$collection], $on, 'authorId', '=', $alias),
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
        $database->createCollection(new Collection(
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
                Method::Join => Query::join($collections[$collection], $left, 'authorId', '=', $alias),
                Method::LeftJoin => Query::leftJoin($collections[$collection], $left, 'authorId', '=', $alias),
                Method::RightJoin => Query::rightJoin($collections[$collection], $left, 'authorId', '=', $alias),
                Method::FullOuterJoin => Query::fullOuterJoin($collections[$collection], $left, 'authorId', '=', $alias),
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
            $this->orderColumnSummaries($database->find($main, [Query::join($joined, 'link', 'link', '=', $alias)]), $alias),
            'find',
        );
        $this->assertSame(
            [['m1', 'first', 'j1', 1]],
            $this->orderColumnSummaries([$database->getDocument($main, 'm1', [Query::leftJoin($joined, 'link', 'link', '=', $alias)])], $alias),
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
        $leftJoined = $database->find($main, [Query::leftJoin($joined, 'link', 'link', '=', $alias)]);
        $this->assertCount(2, $leftJoined);
        $columns = $this->orderColumnKeys($leftJoined[0]);

        foreach ($orders as $label => [$order, $key, $ordered]) {
            $rows = $database->find($main, [Query::fullOuterJoin($joined, 'link', 'link', '=', $alias), $order]);

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
        $database->createCollection(new Collection(
            id: $main,
            attributes: [
                Attribute::string(key: 'link', size: 16, required: true),
                Attribute::string(key: 'foj_ord_note', size: 64, required: false),
            ],
            permissions: $permissions,
            documentSecurity: false,
        ));
        $database->createCollection(new Collection(
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
}
