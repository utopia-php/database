<?php

namespace Tests\E2E\Adapter\Scopes;

use PHPUnit\Framework\Attributes\DataProvider;
use Utopia\Database\Attribute;
use Utopia\Database\Capability;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;
use Utopia\Query\Schema\ColumnType;

trait AggregationTests
{
    private function createProducts(Database $database, string $collection = 'agg_products'): void
    {
        if ($database->exists($database->getDatabase(), $collection)) {
            $database->deleteCollection($collection);
        }

        $database->createCollection($collection);
        $database->createAttribute($collection, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));
        $database->createAttribute($collection, new Attribute(key: 'category', type: ColumnType::String, size: 50, required: true));
        $database->createAttribute($collection, new Attribute(key: 'price', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute($collection, new Attribute(key: 'stock', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute($collection, new Attribute(key: 'rating', type: ColumnType::Double, size: 0, required: false, default: 0.0));

        $products = [
            ['$id' => 'laptop', 'name' => 'Laptop', 'category' => 'electronics', 'price' => 1200, 'stock' => 50, 'rating' => 4.5],
            ['$id' => 'phone', 'name' => 'Phone', 'category' => 'electronics', 'price' => 800, 'stock' => 100, 'rating' => 4.2],
            ['$id' => 'tablet', 'name' => 'Tablet', 'category' => 'electronics', 'price' => 500, 'stock' => 75, 'rating' => 3.8],
            ['$id' => 'shirt', 'name' => 'Shirt', 'category' => 'clothing', 'price' => 30, 'stock' => 200, 'rating' => 4.0],
            ['$id' => 'pants', 'name' => 'Pants', 'category' => 'clothing', 'price' => 50, 'stock' => 150, 'rating' => 3.5],
            ['$id' => 'jacket', 'name' => 'Jacket', 'category' => 'clothing', 'price' => 120, 'stock' => 80, 'rating' => 4.7],
            ['$id' => 'novel', 'name' => 'Novel', 'category' => 'books', 'price' => 15, 'stock' => 300, 'rating' => 4.8],
            ['$id' => 'textbook', 'name' => 'Textbook', 'category' => 'books', 'price' => 60, 'stock' => 40, 'rating' => 3.2],
            ['$id' => 'comic', 'name' => 'Comic', 'category' => 'books', 'price' => 10, 'stock' => 500, 'rating' => 4.1],
        ];

        foreach ($products as $product) {
            $database->createDocument($collection, new Document(array_merge($product, [
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ])));
        }
    }

    private function createOrders(Database $database, string $collection = 'agg_orders'): void
    {
        if ($database->exists($database->getDatabase(), $collection)) {
            $database->deleteCollection($collection);
        }

        $database->createCollection($collection);
        $database->createAttribute($collection, new Attribute(key: 'product_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($collection, new Attribute(key: 'customer_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($collection, new Attribute(key: 'quantity', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute($collection, new Attribute(key: 'total', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute($collection, new Attribute(key: 'status', type: ColumnType::String, size: 20, required: true));

        $orders = [
            ['$id' => 'ord1', 'product_uid' => 'laptop', 'customer_uid' => 'alice', 'quantity' => 1, 'total' => 1200, 'status' => 'completed'],
            ['$id' => 'ord2', 'product_uid' => 'phone', 'customer_uid' => 'alice', 'quantity' => 2, 'total' => 1600, 'status' => 'completed'],
            ['$id' => 'ord3', 'product_uid' => 'shirt', 'customer_uid' => 'alice', 'quantity' => 3, 'total' => 90, 'status' => 'pending'],
            ['$id' => 'ord4', 'product_uid' => 'laptop', 'customer_uid' => 'bob', 'quantity' => 1, 'total' => 1200, 'status' => 'completed'],
            ['$id' => 'ord5', 'product_uid' => 'novel', 'customer_uid' => 'bob', 'quantity' => 5, 'total' => 75, 'status' => 'completed'],
            ['$id' => 'ord6', 'product_uid' => 'tablet', 'customer_uid' => 'charlie', 'quantity' => 1, 'total' => 500, 'status' => 'cancelled'],
            ['$id' => 'ord7', 'product_uid' => 'jacket', 'customer_uid' => 'charlie', 'quantity' => 2, 'total' => 240, 'status' => 'completed'],
            ['$id' => 'ord8', 'product_uid' => 'phone', 'customer_uid' => 'diana', 'quantity' => 1, 'total' => 800, 'status' => 'pending'],
            ['$id' => 'ord9', 'product_uid' => 'pants', 'customer_uid' => 'diana', 'quantity' => 4, 'total' => 200, 'status' => 'completed'],
            ['$id' => 'ord10', 'product_uid' => 'comic', 'customer_uid' => 'diana', 'quantity' => 10, 'total' => 100, 'status' => 'completed'],
        ];

        foreach ($orders as $order) {
            $database->createDocument($collection, new Document(array_merge($order, [
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ])));
        }
    }

    private function createCustomers(Database $database, string $collection = 'agg_customers'): void
    {
        if ($database->exists($database->getDatabase(), $collection)) {
            $database->deleteCollection($collection);
        }

        $database->createCollection($collection);
        $database->createAttribute($collection, new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));
        $database->createAttribute($collection, new Attribute(key: 'email', type: ColumnType::String, size: 200, required: true));
        $database->createAttribute($collection, new Attribute(key: 'country', type: ColumnType::String, size: 50, required: true));
        $database->createAttribute($collection, new Attribute(key: 'tier', type: ColumnType::String, size: 20, required: true));

        $customers = [
            ['$id' => 'alice', 'name' => 'Alice', 'email' => 'alice@test.com', 'country' => 'US', 'tier' => 'premium'],
            ['$id' => 'bob', 'name' => 'Bob', 'email' => 'bob@test.com', 'country' => 'US', 'tier' => 'basic'],
            ['$id' => 'charlie', 'name' => 'Charlie', 'email' => 'charlie@test.com', 'country' => 'UK', 'tier' => 'vip'],
            ['$id' => 'diana', 'name' => 'Diana', 'email' => 'diana@test.com', 'country' => 'UK', 'tier' => 'premium'],
            ['$id' => 'eve', 'name' => 'Eve', 'email' => 'eve@test.com', 'country' => 'DE', 'tier' => 'basic'],
        ];

        foreach ($customers as $customer) {
            $database->createDocument($collection, new Document(array_merge($customer, [
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ])));
        }
    }

    private function createReviews(Database $database, string $collection = 'agg_reviews'): void
    {
        if ($database->exists($database->getDatabase(), $collection)) {
            $database->deleteCollection($collection);
        }

        $database->createCollection($collection);
        $database->createAttribute($collection, new Attribute(key: 'product_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($collection, new Attribute(key: 'customer_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute($collection, new Attribute(key: 'score', type: ColumnType::Integer, size: 0, required: true));
        $database->createAttribute($collection, new Attribute(key: 'comment', type: ColumnType::String, size: 500, required: false, default: ''));

        $reviews = [
            ['product_uid' => 'laptop', 'customer_uid' => 'alice', 'score' => 5, 'comment' => 'Excellent'],
            ['product_uid' => 'laptop', 'customer_uid' => 'bob', 'score' => 4, 'comment' => 'Good'],
            ['product_uid' => 'laptop', 'customer_uid' => 'charlie', 'score' => 3, 'comment' => 'Average'],
            ['product_uid' => 'phone', 'customer_uid' => 'alice', 'score' => 4, 'comment' => 'Nice'],
            ['product_uid' => 'phone', 'customer_uid' => 'diana', 'score' => 5, 'comment' => 'Great'],
            ['product_uid' => 'shirt', 'customer_uid' => 'bob', 'score' => 2, 'comment' => 'Poor fit'],
            ['product_uid' => 'shirt', 'customer_uid' => 'charlie', 'score' => 4, 'comment' => 'Nice fabric'],
            ['product_uid' => 'novel', 'customer_uid' => 'diana', 'score' => 5, 'comment' => 'Loved it'],
            ['product_uid' => 'novel', 'customer_uid' => 'alice', 'score' => 5, 'comment' => 'Must read'],
            ['product_uid' => 'novel', 'customer_uid' => 'eve', 'score' => 4, 'comment' => 'Good story'],
            ['product_uid' => 'jacket', 'customer_uid' => 'charlie', 'score' => 5, 'comment' => 'Perfect'],
            ['product_uid' => 'textbook', 'customer_uid' => 'eve', 'score' => 1, 'comment' => 'Boring'],
        ];

        foreach ($reviews as $review) {
            $database->createDocument($collection, new Document(array_merge($review, [
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
            ])));
        }
    }

    private function cleanupAggCollections(Database $database, array $collections): void
    {
        foreach ($collections as $col) {
            if ($database->exists($database->getDatabase(), $col)) {
                $database->deleteCollection($col);
            }
        }
    }


    public function testCountAll(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'cnt_all');
        $results = $database->find('cnt_all', [Query::count('*', 'total')]);
        $this->assertCount(1, $results);
        $this->assertEquals(9, $results[0]->getAttribute('total'));
        $database->deleteCollection('cnt_all');
    }

    public function testCountWithAlias(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'cnt_alias');
        $results = $database->find('cnt_alias', [Query::count('*', 'num_products')]);
        $this->assertCount(1, $results);
        $this->assertEquals(9, $results[0]->getAttribute('num_products'));
        $database->deleteCollection('cnt_alias');
    }

    public function testCountWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'cnt_filter');

        $results = $database->find('cnt_filter', [
            Query::equal('category', ['electronics']),
            Query::count('*', 'total'),
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals(3, $results[0]->getAttribute('total'));

        $results = $database->find('cnt_filter', [
            Query::equal('category', ['clothing']),
            Query::count('*', 'total'),
        ]);
        $this->assertEquals(3, $results[0]->getAttribute('total'));

        $results = $database->find('cnt_filter', [
            Query::greaterThan('price', 100),
            Query::count('*', 'total'),
        ]);
        $this->assertEquals(4, $results[0]->getAttribute('total'));

        $database->deleteCollection('cnt_filter');
    }

    public function testCountEmptyCollection(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = 'cnt_empty';
        if ($database->exists($database->getDatabase(), $col)) {
            $database->deleteCollection($col);
        }
        $database->createCollection($col);
        $database->createAttribute($col, new Attribute(key: 'value', type: ColumnType::Integer, size: 0, required: true));

        $results = $database->find($col, [Query::count('*', 'total')]);
        $this->assertCount(1, $results);
        $this->assertEquals(0, $results[0]->getAttribute('total'));

        $database->deleteCollection($col);
    }

    public function testCountWithMultipleFilters(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'cnt_multi');

        $results = $database->find('cnt_multi', [
            Query::equal('category', ['electronics']),
            Query::greaterThan('price', 600),
            Query::count('*', 'total'),
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals(2, $results[0]->getAttribute('total'));

        $database->deleteCollection('cnt_multi');
    }

    public function testCountDistinct(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'cnt_distinct');
        $results = $database->find('cnt_distinct', [Query::countDistinct('category', 'unique_cats')]);
        $this->assertCount(1, $results);
        $this->assertEquals(3, $results[0]->getAttribute('unique_cats'));
        $database->deleteCollection('cnt_distinct');
    }

    public function testCountDistinctWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'cnt_dist_f');
        $results = $database->find('cnt_dist_f', [
            Query::greaterThan('price', 50),
            Query::countDistinct('category', 'unique_cats'),
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals(3, $results[0]->getAttribute('unique_cats'));
        $database->deleteCollection('cnt_dist_f');
    }


    public function testSumAll(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'sum_all');
        $results = $database->find('sum_all', [Query::sum('price', 'total_price')]);
        $this->assertCount(1, $results);
        $this->assertEquals(2785, $results[0]->getAttribute('total_price'));
        $database->deleteCollection('sum_all');
    }

    public function testSumWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'sum_filt');
        $results = $database->find('sum_filt', [
            Query::equal('category', ['electronics']),
            Query::sum('price', 'total'),
        ]);
        $this->assertEquals(2500, $results[0]->getAttribute('total'));
        $database->deleteCollection('sum_filt');
    }

    public function testSumEmptyResult(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'sum_empty');
        $results = $database->find('sum_empty', [
            Query::equal('category', ['nonexistent']),
            Query::sum('price', 'total'),
        ]);
        $this->assertCount(1, $results);
        $this->assertNull($results[0]->getAttribute('total'));
        $database->deleteCollection('sum_empty');
    }

    public function testSumOfStock(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'sum_stock');
        $results = $database->find('sum_stock', [Query::sum('stock', 'total_stock')]);
        $this->assertEquals(1495, $results[0]->getAttribute('total_stock'));
        $database->deleteCollection('sum_stock');
    }


    public function testAvgAll(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'avg_all');
        $results = $database->find('avg_all', [Query::avg('price', 'avg_price')]);
        $this->assertCount(1, $results);
        $avgPrice = (float) $results[0]->getAttribute('avg_price');
        $this->assertEqualsWithDelta(309.44, $avgPrice, 1.0);
        $database->deleteCollection('avg_all');
    }

    public function testAvgWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'avg_filt');
        $results = $database->find('avg_filt', [
            Query::equal('category', ['electronics']),
            Query::avg('price', 'avg_price'),
        ]);
        $avgPrice = (float) $results[0]->getAttribute('avg_price');
        $this->assertEqualsWithDelta(833.33, $avgPrice, 1.0);
        $database->deleteCollection('avg_filt');
    }

    public function testAvgOfRating(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'avg_rating');
        $results = $database->find('avg_rating', [Query::avg('rating', 'avg_rating')]);
        $avgRating = (float) $results[0]->getAttribute('avg_rating');
        $this->assertEqualsWithDelta(4.09, $avgRating, 0.1);
        $database->deleteCollection('avg_rating');
    }


    public function testMinAll(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'min_all');
        $results = $database->find('min_all', [Query::min('price', 'min_price')]);
        $this->assertEquals(10, $results[0]->getAttribute('min_price'));
        $database->deleteCollection('min_all');
    }

    public function testMinWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'min_filt');
        $results = $database->find('min_filt', [
            Query::equal('category', ['electronics']),
            Query::min('price', 'cheapest'),
        ]);
        $this->assertEquals(500, $results[0]->getAttribute('cheapest'));
        $database->deleteCollection('min_filt');
    }

    public function testMaxAll(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'max_all');
        $results = $database->find('max_all', [Query::max('price', 'max_price')]);
        $this->assertEquals(1200, $results[0]->getAttribute('max_price'));
        $database->deleteCollection('max_all');
    }

    public function testMaxWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'max_filt');
        $results = $database->find('max_filt', [
            Query::equal('category', ['books']),
            Query::max('price', 'expensive'),
        ]);
        $this->assertEquals(60, $results[0]->getAttribute('expensive'));
        $database->deleteCollection('max_filt');
    }

    public function testMinMaxTogether(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'minmax');
        $results = $database->find('minmax', [
            Query::min('price', 'cheapest'),
            Query::max('price', 'priciest'),
        ]);
        $this->assertCount(1, $results);
        $this->assertEquals(10, $results[0]->getAttribute('cheapest'));
        $this->assertEquals(1200, $results[0]->getAttribute('priciest'));
        $database->deleteCollection('minmax');
    }


    public function testMultipleAggregationsTogether(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'multi_agg');
        $results = $database->find('multi_agg', [
            Query::count('*', 'total_count'),
            Query::sum('price', 'total_price'),
            Query::avg('price', 'avg_price'),
            Query::min('price', 'min_price'),
            Query::max('price', 'max_price'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(9, $results[0]->getAttribute('total_count'));
        $this->assertEquals(2785, $results[0]->getAttribute('total_price'));
        $this->assertEqualsWithDelta(309.44, (float) $results[0]->getAttribute('avg_price'), 1.0);
        $this->assertEquals(10, $results[0]->getAttribute('min_price'));
        $this->assertEquals(1200, $results[0]->getAttribute('max_price'));
        $database->deleteCollection('multi_agg');
    }

    public function testMultipleAggregationsWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'multi_agg_f');
        $results = $database->find('multi_agg_f', [
            Query::equal('category', ['clothing']),
            Query::count('*', 'cnt'),
            Query::sum('price', 'total'),
            Query::avg('stock', 'avg_stock'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(3, $results[0]->getAttribute('cnt'));
        $this->assertEquals(200, $results[0]->getAttribute('total'));
        $this->assertEqualsWithDelta(143.33, (float) $results[0]->getAttribute('avg_stock'), 1.0);
        $database->deleteCollection('multi_agg_f');
    }


    public function testGroupBySingleColumn(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'grp_single');
        $results = $database->find('grp_single', [
            Query::count('*', 'cnt'),
            Query::groupBy(['category']),
        ]);

        $this->assertCount(3, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('category')] = $doc;
        }
        $this->assertEquals(3, $mapped['electronics']->getAttribute('cnt'));
        $this->assertEquals(3, $mapped['clothing']->getAttribute('cnt'));
        $this->assertEquals(3, $mapped['books']->getAttribute('cnt'));
        $database->deleteCollection('grp_single');
    }

    public function testGroupByWithSum(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'grp_sum');
        $results = $database->find('grp_sum', [
            Query::sum('price', 'total_price'),
            Query::groupBy(['category']),
        ]);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('category')] = $doc;
        }
        $this->assertEquals(2500, $mapped['electronics']->getAttribute('total_price'));
        $this->assertEquals(200, $mapped['clothing']->getAttribute('total_price'));
        $this->assertEquals(85, $mapped['books']->getAttribute('total_price'));
        $database->deleteCollection('grp_sum');
    }

    public function testGroupByWithAvg(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'grp_avg');
        $results = $database->find('grp_avg', [
            Query::avg('price', 'avg_price'),
            Query::groupBy(['category']),
        ]);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('category')] = (float) $doc->getAttribute('avg_price');
        }
        $this->assertEqualsWithDelta(833.33, $mapped['electronics'], 1.0);
        $this->assertEqualsWithDelta(66.67, $mapped['clothing'], 1.0);
        $this->assertEqualsWithDelta(28.33, $mapped['books'], 1.0);
        $database->deleteCollection('grp_avg');
    }

    public function testGroupByWithMinMax(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'grp_minmax');
        $results = $database->find('grp_minmax', [
            Query::min('price', 'cheapest'),
            Query::max('price', 'priciest'),
            Query::groupBy(['category']),
        ]);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('category')] = $doc;
        }
        $this->assertEquals(500, $mapped['electronics']->getAttribute('cheapest'));
        $this->assertEquals(1200, $mapped['electronics']->getAttribute('priciest'));
        $this->assertEquals(30, $mapped['clothing']->getAttribute('cheapest'));
        $this->assertEquals(120, $mapped['clothing']->getAttribute('priciest'));
        $this->assertEquals(10, $mapped['books']->getAttribute('cheapest'));
        $this->assertEquals(60, $mapped['books']->getAttribute('priciest'));
        $database->deleteCollection('grp_minmax');
    }

    public function testGroupByWithMultipleAggregations(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'grp_multi');
        $results = $database->find('grp_multi', [
            Query::count('*', 'cnt'),
            Query::sum('price', 'total'),
            Query::avg('rating', 'avg_rating'),
            Query::min('stock', 'min_stock'),
            Query::max('stock', 'max_stock'),
            Query::groupBy(['category']),
        ]);

        $this->assertCount(3, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('category')] = $doc;
        }

        $this->assertEquals(3, $mapped['electronics']->getAttribute('cnt'));
        $this->assertEquals(2500, $mapped['electronics']->getAttribute('total'));
        $this->assertEquals(50, $mapped['electronics']->getAttribute('min_stock'));
        $this->assertEquals(100, $mapped['electronics']->getAttribute('max_stock'));

        $this->assertEquals(3, $mapped['books']->getAttribute('cnt'));
        $this->assertEquals(85, $mapped['books']->getAttribute('total'));
        $this->assertEquals(40, $mapped['books']->getAttribute('min_stock'));
        $this->assertEquals(500, $mapped['books']->getAttribute('max_stock'));

        $database->deleteCollection('grp_multi');
    }

    public function testGroupByWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'grp_filt');
        $results = $database->find('grp_filt', [
            Query::greaterThan('price', 50),
            Query::count('*', 'cnt'),
            Query::groupBy(['category']),
        ]);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('category')] = $doc;
        }
        $this->assertEquals(3, $mapped['electronics']->getAttribute('cnt'));
        $this->assertEquals(1, $mapped['clothing']->getAttribute('cnt'));
        $this->assertEquals(1, $mapped['books']->getAttribute('cnt'));
        $database->deleteCollection('grp_filt');
    }

    public function testGroupByOrdersStatus(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createOrders($database, 'grp_status');
        $results = $database->find('grp_status', [
            Query::count('*', 'cnt'),
            Query::sum('total', 'revenue'),
            Query::groupBy(['status']),
        ]);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('status')] = $doc;
        }
        $this->assertEquals(7, $mapped['completed']->getAttribute('cnt'));
        $this->assertEquals(2, $mapped['pending']->getAttribute('cnt'));
        $this->assertEquals(1, $mapped['cancelled']->getAttribute('cnt'));
        $database->deleteCollection('grp_status');
    }

    public function testGroupByCustomerOrders(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createOrders($database, 'grp_cust');
        $results = $database->find('grp_cust', [
            Query::count('*', 'order_count'),
            Query::sum('total', 'total_spent'),
            Query::avg('total', 'avg_order'),
            Query::groupBy(['customer_uid']),
        ]);

        $this->assertCount(4, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('customer_uid')] = $doc;
        }
        $this->assertEquals(3, $mapped['alice']->getAttribute('order_count'));
        $this->assertEquals(2890, $mapped['alice']->getAttribute('total_spent'));
        $this->assertEquals(2, $mapped['bob']->getAttribute('order_count'));
        $this->assertEquals(1275, $mapped['bob']->getAttribute('total_spent'));
        $database->deleteCollection('grp_cust');
    }


    public function testHavingGreaterThan(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'having_gt');
        $results = $database->find('having_gt', [
            Query::sum('price', 'total_price'),
            Query::groupBy(['category']),
            Query::having([Query::greaterThan('total_price', 100)]),
        ]);

        $this->assertCount(2, $results);
        $categories = array_map(fn ($d) => $d->getAttribute('category'), $results);
        $this->assertContains('electronics', $categories);
        $this->assertContains('clothing', $categories);
        $this->assertNotContains('books', $categories);
        $database->deleteCollection('having_gt');
    }

    public function testHavingLessThan(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'having_lt');
        $results = $database->find('having_lt', [
            Query::count('*', 'cnt'),
            Query::sum('price', 'total'),
            Query::groupBy(['category']),
            Query::having([Query::lessThan('total', 500)]),
        ]);

        $this->assertCount(2, $results);
        $categories = array_map(fn ($d) => $d->getAttribute('category'), $results);
        $this->assertContains('clothing', $categories);
        $this->assertContains('books', $categories);
        $database->deleteCollection('having_lt');
    }

    public function testHavingWithCount(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createReviews($database, 'having_cnt');
        $results = $database->find('having_cnt', [
            Query::count('*', 'review_count'),
            Query::groupBy(['product_uid']),
            Query::having([Query::greaterThanEqual('review_count', 3)]),
        ]);

        $productIds = array_map(fn ($d) => $d->getAttribute('product_uid'), $results);
        $this->assertContains('laptop', $productIds);
        $this->assertContains('novel', $productIds);
        $this->assertNotContains('jacket', $productIds);
        $database->deleteCollection('having_cnt');
    }


    public function testInnerJoinBasic(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createOrders($database, 'ij_orders');
        $this->createCustomers($database, 'ij_customers');

        $results = $database->find('ij_orders', [
            Query::join('ij_customers', 'customer_uid', '$id'),
            Query::count('*', 'total'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals(10, $results[0]->getAttribute('total'));

        $this->cleanupAggCollections($database, ['ij_orders', 'ij_customers']);
    }

    public function testInnerJoinWithGroupBy(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createOrders($database, 'ij_grp_o');
        $this->createCustomers($database, 'ij_grp_c');

        $results = $database->find('ij_grp_o', [
            Query::join('ij_grp_c', 'customer_uid', '$id'),
            Query::sum('total', 'total_spent'),
            Query::count('*', 'order_count'),
            Query::groupBy(['customer_uid']),
        ]);

        $this->assertCount(4, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('customer_uid')] = $doc;
        }
        $this->assertEquals(2890, $mapped['alice']->getAttribute('total_spent'));
        $this->assertEquals(3, $mapped['alice']->getAttribute('order_count'));
        $this->assertEquals(1275, $mapped['bob']->getAttribute('total_spent'));
        $this->assertEquals(2, $mapped['bob']->getAttribute('order_count'));

        $this->cleanupAggCollections($database, ['ij_grp_o', 'ij_grp_c']);
    }

    public function testInnerJoinWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createOrders($database, 'ij_filt_o');
        $this->createCustomers($database, 'ij_filt_c');

        $results = $database->find('ij_filt_o', [
            Query::join('ij_filt_c', 'customer_uid', '$id'),
            Query::equal('status', ['completed']),
            Query::sum('total', 'revenue'),
            Query::groupBy(['customer_uid']),
        ]);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('customer_uid')] = $doc;
        }
        $this->assertEquals(2800, $mapped['alice']->getAttribute('revenue'));
        $this->assertEquals(1275, $mapped['bob']->getAttribute('revenue'));
        $this->assertEquals(240, $mapped['charlie']->getAttribute('revenue'));
        $this->assertEquals(300, $mapped['diana']->getAttribute('revenue'));

        $this->cleanupAggCollections($database, ['ij_filt_o', 'ij_filt_c']);
    }

    public function testInnerJoinWithHaving(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createOrders($database, 'ij_hav_o');
        $this->createCustomers($database, 'ij_hav_c');

        $results = $database->find('ij_hav_o', [
            Query::join('ij_hav_c', 'customer_uid', '$id'),
            Query::sum('total', 'total_spent'),
            Query::groupBy(['customer_uid']),
            Query::having([Query::greaterThan('total_spent', 1000)]),
        ]);

        $this->assertCount(3, $results);
        $customerIds = array_map(fn ($d) => $d->getAttribute('customer_uid'), $results);
        $this->assertContains('alice', $customerIds);
        $this->assertContains('bob', $customerIds);
        $this->assertContains('diana', $customerIds);

        $this->cleanupAggCollections($database, ['ij_hav_o', 'ij_hav_c']);
    }

    public function testInnerJoinProductReviewStats(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'ij_prs_p');
        $this->createReviews($database, 'ij_prs_r');

        $results = $database->find('ij_prs_p', [
            Query::join('ij_prs_r', '$id', 'product_uid'),
            Query::count('*', 'review_count'),
            Query::avg('score', 'avg_score'),
            Query::groupBy(['name']),
        ]);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('name')] = $doc;
        }

        $this->assertEquals(3, $mapped['Laptop']->getAttribute('review_count'));
        $this->assertEqualsWithDelta(4.0, (float) $mapped['Laptop']->getAttribute('avg_score'), 0.1);
        $this->assertEquals(3, $mapped['Novel']->getAttribute('review_count'));
        $this->assertEqualsWithDelta(4.67, (float) $mapped['Novel']->getAttribute('avg_score'), 0.1);

        $this->cleanupAggCollections($database, ['ij_prs_p', 'ij_prs_r']);
    }


    public function testLeftJoinBasic(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'lj_basic_p');
        $this->createReviews($database, 'lj_basic_r');

        $results = $database->find('lj_basic_p', [
            Query::leftJoin('lj_basic_r', '$id', 'product_uid'),
            Query::count('*', 'review_count'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(9, $results);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('name')] = $doc;
        }

        $this->assertEquals(3, $mapped['Laptop']->getAttribute('review_count'));
        $this->assertEquals(2, $mapped['Phone']->getAttribute('review_count'));
        $this->assertEquals(1, $mapped['Tablet']->getAttribute('review_count'));
        $this->assertEquals(1, $mapped['Comic']->getAttribute('review_count'));

        $this->cleanupAggCollections($database, ['lj_basic_p', 'lj_basic_r']);
    }

    public function testLeftJoinWithFilter(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createProducts($database, 'lj_filt_p');
        $this->createOrders($database, 'lj_filt_o');

        $results = $database->find('lj_filt_p', [
            Query::leftJoin('lj_filt_o', '$id', 'product_uid'),
            Query::equal('category', ['electronics']),
            Query::count('*', 'order_count'),
            Query::sum('quantity', 'total_qty'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(3, $results);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('name')] = $doc;
        }
        $this->assertEquals(2, $mapped['Laptop']->getAttribute('order_count'));
        $this->assertEquals(2, $mapped['Phone']->getAttribute('order_count'));

        $this->cleanupAggCollections($database, ['lj_filt_p', 'lj_filt_o']);
    }

    public function testLeftJoinCustomerOrderSummary(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $this->createCustomers($database, 'lj_cos_c');
        $this->createOrders($database, 'lj_cos_o');

        $results = $database->find('lj_cos_c', [
            Query::leftJoin('lj_cos_o', '$id', 'customer_uid'),
            Query::count('*', 'order_count'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(5, $results);

        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('name')] = $doc;
        }

        $this->assertEquals(3, $mapped['Alice']->getAttribute('order_count'));
        $this->assertEquals(2, $mapped['Bob']->getAttribute('order_count'));
        $this->assertEquals(2, $mapped['Charlie']->getAttribute('order_count'));
        $this->assertEquals(3, $mapped['Diana']->getAttribute('order_count'));
        $this->assertEquals(1, $mapped['Eve']->getAttribute('order_count'));

        $this->cleanupAggCollections($database, ['lj_cos_c', 'lj_cos_o']);
    }


    public function testJoinAggregationWithPermissionsGrouped(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $cols = ['jp_apg_o', 'jp_apg_c'];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection('jp_apg_c');
        $database->createAttribute('jp_apg_c', new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));
        $database->createCollection('jp_apg_o', documentSecurity: true);
        $database->createAttribute('jp_apg_o', new Attribute(key: 'customer_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('jp_apg_o', new Attribute(key: 'amount', type: ColumnType::Integer, size: 0, required: true));

        foreach (['u1', 'u2'] as $uid) {
            $database->createDocument('jp_apg_c', new Document([
                '$id' => $uid, 'name' => 'User ' . $uid,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $database->createDocument('jp_apg_o', new Document([
            'customer_uid' => 'u1', 'amount' => 100,
            '$permissions' => [Permission::read(Role::user('viewer'))],
        ]));
        $database->createDocument('jp_apg_o', new Document([
            'customer_uid' => 'u1', 'amount' => 200,
            '$permissions' => [Permission::read(Role::user('viewer'))],
        ]));
        $database->createDocument('jp_apg_o', new Document([
            'customer_uid' => 'u2', 'amount' => 500,
            '$permissions' => [Permission::read(Role::user('admin'))],
        ]));
        $database->createDocument('jp_apg_o', new Document([
            'customer_uid' => 'u2', 'amount' => 50,
            '$permissions' => [Permission::read(Role::user('viewer'))],
        ]));

        $database->getAuthorization()->cleanRoles();
        $database->getAuthorization()->addRole(Role::user('viewer')->toString());

        $results = $database->find('jp_apg_o', [
            Query::join('jp_apg_c', 'customer_uid', '$id'),
            Query::sum('amount', 'total'),
            Query::count('*', 'cnt'),
            Query::groupBy(['customer_uid']),
        ]);

        $this->assertCount(2, $results);
        $mapped = [];
        foreach ($results as $doc) {
            $mapped[$doc->getAttribute('customer_uid')] = $doc;
        }
        $this->assertEquals(300, $mapped['u1']->getAttribute('total'));
        $this->assertEquals(2, $mapped['u1']->getAttribute('cnt'));
        $this->assertEquals(50, $mapped['u2']->getAttribute('total'));
        $this->assertEquals(1, $mapped['u2']->getAttribute('cnt'));

        $database->getAuthorization()->cleanRoles();
        $database->getAuthorization()->addRole('any');

        $this->cleanupAggCollections($database, $cols);
    }

    public function testLeftJoinPermissionFiltered(): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Joins)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $cols = ['jp_ljpf_p', 'jp_ljpf_r'];
        $this->cleanupAggCollections($database, $cols);

        $database->createCollection('jp_ljpf_p', documentSecurity: true);
        $database->createAttribute('jp_ljpf_p', new Attribute(key: 'name', type: ColumnType::String, size: 100, required: true));
        $database->createCollection('jp_ljpf_r');
        $database->createAttribute('jp_ljpf_r', new Attribute(key: 'product_uid', type: ColumnType::String, size: 255, required: true));
        $database->createAttribute('jp_ljpf_r', new Attribute(key: 'score', type: ColumnType::Integer, size: 0, required: true));

        $database->createDocument('jp_ljpf_p', new Document([
            '$id' => 'visible', 'name' => 'Visible Product',
            '$permissions' => [Permission::read(Role::user('tester'))],
        ]));
        $database->createDocument('jp_ljpf_p', new Document([
            '$id' => 'hidden', 'name' => 'Hidden Product',
            '$permissions' => [Permission::read(Role::user('admin'))],
        ]));

        foreach (['visible', 'visible', 'hidden'] as $pid) {
            $database->createDocument('jp_ljpf_r', new Document([
                'product_uid' => $pid, 'score' => 5,
                '$permissions' => [Permission::read(Role::any())],
            ]));
        }

        $database->getAuthorization()->cleanRoles();
        $database->getAuthorization()->addRole(Role::user('tester')->toString());

        $results = $database->find('jp_ljpf_p', [
            Query::leftJoin('jp_ljpf_r', '$id', 'product_uid'),
            Query::count('*', 'review_count'),
            Query::groupBy(['name']),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals('Visible Product', $results[0]->getAttribute('name'));
        $this->assertEquals(2, $results[0]->getAttribute('review_count'));

        $database->getAuthorization()->cleanRoles();
        $database->getAuthorization()->addRole('any');

        $this->cleanupAggCollections($database, $cols);
    }

    /**
     * @return array<string, array{string, string, string, array<Query>, int|float}>
     */
    public static function singleAggregationProvider(): array
    {
        return [
            'count all products' => ['cnt', 'count', '*', 'total', [], 9],
            'count electronics' => ['cnt', 'count', '*', 'total', [Query::equal('category', ['electronics'])], 3],
            'count clothing' => ['cnt', 'count', '*', 'total', [Query::equal('category', ['clothing'])], 3],
            'count books' => ['cnt', 'count', '*', 'total', [Query::equal('category', ['books'])], 3],
            'count price > 100' => ['cnt', 'count', '*', 'total', [Query::greaterThan('price', 100)], 4],
            'count price <= 50' => ['cnt', 'count', '*', 'total', [Query::lessThanEqual('price', 50)], 4],
            'sum all prices' => ['sum', 'sum', 'price', 'total', [], 2785],
            'sum electronics' => ['sum', 'sum', 'price', 'total', [Query::equal('category', ['electronics'])], 2500],
            'sum clothing' => ['sum', 'sum', 'price', 'total', [Query::equal('category', ['clothing'])], 200],
            'sum books' => ['sum', 'sum', 'price', 'total', [Query::equal('category', ['books'])], 85],
            'sum stock' => ['sum', 'sum', 'stock', 'total', [], 1495],
            'sum stock electronics' => ['sum', 'sum', 'stock', 'total', [Query::equal('category', ['electronics'])], 225],
            'min all price' => ['min', 'min', 'price', 'val', [], 10],
            'min electronics price' => ['min', 'min', 'price', 'val', [Query::equal('category', ['electronics'])], 500],
            'min clothing price' => ['min', 'min', 'price', 'val', [Query::equal('category', ['clothing'])], 30],
            'min books price' => ['min', 'min', 'price', 'val', [Query::equal('category', ['books'])], 10],
            'min stock' => ['min', 'min', 'stock', 'val', [], 40],
            'max all price' => ['max', 'max', 'price', 'val', [], 1200],
            'max electronics price' => ['max', 'max', 'price', 'val', [Query::equal('category', ['electronics'])], 1200],
            'max clothing price' => ['max', 'max', 'price', 'val', [Query::equal('category', ['clothing'])], 120],
            'max books price' => ['max', 'max', 'price', 'val', [Query::equal('category', ['books'])], 60],
            'max stock' => ['max', 'max', 'stock', 'val', [], 500],
            'count distinct categories' => ['cntd', 'countDistinct', 'category', 'val', [], 3],
            'count distinct price > 50' => ['cntd', 'countDistinct', 'category', 'val', [Query::greaterThan('price', 50)], 3],
        ];
    }

    /**
     * @param  array<Query>  $filters
     */
    #[DataProvider('singleAggregationProvider')]
    public function testSingleAggregation(string $collSuffix, string $method, string $attribute, string $alias, array $filters, int|float $expected): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = 'dp_agg_' . $collSuffix;
        $this->createProducts($database, $col);

        $aggQuery = match ($method) {
            'count' => Query::count($attribute, $alias),
            'sum' => Query::sum($attribute, $alias),
            'avg' => Query::avg($attribute, $alias),
            'min' => Query::min($attribute, $alias),
            'max' => Query::max($attribute, $alias),
            'countDistinct' => Query::countDistinct($attribute, $alias),
        };

        $queries = array_merge($filters, [$aggQuery]);
        $results = $database->find($col, $queries);
        $this->assertCount(1, $results);

        if ($method === 'avg') {
            $this->assertEqualsWithDelta($expected, (float) $results[0]->getAttribute($alias), 1.0);
        } else {
            $this->assertEquals($expected, $results[0]->getAttribute($alias));
        }

        $database->deleteCollection($col);
    }

    /**
     * @return array<string, array{string, array<string>, array<Query>, int}>
     */
    public static function groupByCountProvider(): array
    {
        return [
            'group by category no filter' => ['category', [], 3],
            'group by category price > 50' => ['category', [Query::greaterThan('price', 50)], 3],
            'group by category price > 200' => ['category', [Query::greaterThan('price', 200)], 1],
        ];
    }

    /**
     * @param  array<Query>  $filters
     */
    #[DataProvider('groupByCountProvider')]
    public function testGroupByCount(string $groupCol, array $filters, int $expectedGroups): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = 'dp_grpby';
        $this->createProducts($database, $col);

        $queries = array_merge($filters, [
            Query::count('*', 'cnt'),
            Query::groupBy([$groupCol]),
        ]);
        $results = $database->find($col, $queries);
        $this->assertCount($expectedGroups, $results);
        $database->deleteCollection($col);
    }

    /**
     * @return array<string, array{string, int}>
     */
    public static function orderStatusAggProvider(): array
    {
        return [
            'completed orders revenue' => ['completed', 4615],
            'pending orders revenue' => ['pending', 890],
            'cancelled orders revenue' => ['cancelled', 500],
        ];
    }

    #[DataProvider('orderStatusAggProvider')]
    public function testOrderStatusAggregation(string $status, int $expectedRevenue): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = 'dp_osa_' . $status;
        $this->createOrders($database, $col);

        $results = $database->find($col, [
            Query::equal('status', [$status]),
            Query::sum('total', 'revenue'),
        ]);

        $this->assertCount(1, $results);
        $this->assertEquals($expectedRevenue, $results[0]->getAttribute('revenue'));
        $database->deleteCollection($col);
    }

    /**
     * @return array<string, array{string, string, int|float}>
     */
    public static function categoryAggProvider(): array
    {
        return [
            'electronics count' => ['electronics', 'count', 3],
            'electronics sum' => ['electronics', 'sum', 2500],
            'electronics min' => ['electronics', 'min', 500],
            'electronics max' => ['electronics', 'max', 1200],
            'clothing count' => ['clothing', 'count', 3],
            'clothing sum' => ['clothing', 'sum', 200],
            'clothing min' => ['clothing', 'min', 30],
            'clothing max' => ['clothing', 'max', 120],
            'books count' => ['books', 'count', 3],
            'books sum' => ['books', 'sum', 85],
            'books min' => ['books', 'min', 10],
            'books max' => ['books', 'max', 60],
        ];
    }

    #[DataProvider('categoryAggProvider')]
    public function testCategoryAggregation(string $category, string $method, int|float $expected): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = 'dp_cat_' . $category . '_' . $method;
        $this->createProducts($database, $col);

        $aggQuery = match ($method) {
            'count' => Query::count('*', 'val'),
            'sum' => Query::sum('price', 'val'),
            'min' => Query::min('price', 'val'),
            'max' => Query::max('price', 'val'),
        };

        $results = $database->find($col, [
            Query::equal('category', [$category]),
            $aggQuery,
        ]);
        $this->assertEquals($expected, $results[0]->getAttribute('val'));
        $database->deleteCollection($col);
    }

    /**
     * @return array<string, array{string, int}>
     */
    public static function reviewCountProvider(): array
    {
        return [
            'laptop reviews' => ['laptop', 3],
            'phone reviews' => ['phone', 2],
            'shirt reviews' => ['shirt', 2],
            'novel reviews' => ['novel', 3],
            'jacket reviews' => ['jacket', 1],
            'textbook reviews' => ['textbook', 1],
        ];
    }

    #[DataProvider('reviewCountProvider')]
    public function testReviewCounts(string $productId, int $expectedCount): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = 'dp_rc_' . $productId;
        $this->createReviews($database, $col);

        $results = $database->find($col, [
            Query::equal('product_uid', [$productId]),
            Query::count('*', 'cnt'),
        ]);
        $this->assertEquals($expectedCount, $results[0]->getAttribute('cnt'));
        $database->deleteCollection($col);
    }

    /**
     * @return array<string, array{int, int}>
     */
    public static function priceRangeCountProvider(): array
    {
        return [
            'price 0-20' => [0, 20, 2],
            'price 0-50' => [0, 50, 4],
            'price 0-100' => [0, 100, 5],
            'price 50-200' => [50, 200, 3],
            'price 100-500' => [100, 500, 2],
            'price 500-1500' => [500, 1500, 3],
            'price 0-10000' => [0, 10000, 9],
        ];
    }

    #[DataProvider('priceRangeCountProvider')]
    public function testPriceRangeCount(int $min, int $max, int $expected): void
    {
        $database = static::getDatabase();
        if (! $database->getAdapter()->supports(Capability::Aggregations)) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $col = 'dp_prc_' . $min . '_' . $max;
        $this->createProducts($database, $col);

        $results = $database->find($col, [
            Query::between('price', $min, $max),
            Query::count('*', 'cnt'),
        ]);
        $this->assertEquals($expected, $results[0]->getAttribute('cnt'));
        $database->deleteCollection($col);
    }

}
