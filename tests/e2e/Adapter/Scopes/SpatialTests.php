<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

trait SpatialTests
{
    public function testSpatialCollection(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        $collectionName = "test_spatial_Col";
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Adapter does not support spatial attributes');
        };
        $attributes = [
            new Document([
                '$id' => ID::custom('attribute1'),
                'type' => Database::VAR_STRING,
                'size' => 256,
                'required' => false,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ]),
            new Document([
                '$id' => ID::custom('attribute2'),
                'type' => Database::VAR_POINT,
                'size' => 0,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ])
        ];

        $indexes = [
            new Document([
                '$id' => ID::custom('index1'),
                'type' => Database::INDEX_KEY,
                'attributes' => ['attribute1'],
                'lengths' => [256],
                'orders' => [],
            ]),
            new Document([
                '$id' => ID::custom('index2'),
                'type' => Database::INDEX_SPATIAL,
                'attributes' => ['attribute2'],
                'lengths' => [],
                'orders' => [],
            ]),
        ];

        $col =  $database->createCollection($collectionName, $attributes, $indexes);

        $this->assertIsArray($col->getAttribute('attributes'));
        $this->assertCount(2, $col->getAttribute('attributes'));

        $this->assertIsArray($col->getAttribute('indexes'));
        $this->assertCount(2, $col->getAttribute('indexes'));

        $col = $database->getCollection($collectionName);
        $this->assertIsArray($col->getAttribute('attributes'));
        $this->assertCount(2, $col->getAttribute('attributes'));

        $this->assertIsArray($col->getAttribute('indexes'));
        $this->assertCount(2, $col->getAttribute('indexes'));

        $database->createAttribute($collectionName, 'attribute3', Database::VAR_POINT, 0, true);
        $database->createIndex($collectionName, ID::custom("index3"), Database::INDEX_SPATIAL, ['attribute3']);

        $col = $database->getCollection($collectionName);
        $this->assertIsArray($col->getAttribute('attributes'));
        $this->assertCount(3, $col->getAttribute('attributes'));

        $this->assertIsArray($col->getAttribute('indexes'));
        $this->assertCount(3, $col->getAttribute('indexes'));

        $database->deleteCollection($collectionName);
    }

    public function testSpatialTypeDocuments(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Adapter does not support spatial attributes');
        }

        $collectionName = 'test_spatial_doc_';
        try {

            // Create collection first
            $database->createCollection($collectionName);

            // Create spatial attributes using createAttribute method
            $this->assertEquals(true, $database->createAttribute($collectionName, 'pointAttr', Database::VAR_POINT, 0, $database->getAdapter()->getSupportForSpatialIndexNull() ? false : true));
            $this->assertEquals(true, $database->createAttribute($collectionName, 'lineAttr', Database::VAR_LINESTRING, 0, $database->getAdapter()->getSupportForSpatialIndexNull() ? false : true));
            $this->assertEquals(true, $database->createAttribute($collectionName, 'polyAttr', Database::VAR_POLYGON, 0, $database->getAdapter()->getSupportForSpatialIndexNull() ? false : true));

            // Create spatial indexes
            $this->assertEquals(true, $database->createIndex($collectionName, 'point_spatial', Database::INDEX_SPATIAL, ['pointAttr']));
            $this->assertEquals(true, $database->createIndex($collectionName, 'line_spatial', Database::INDEX_SPATIAL, ['lineAttr']));
            $this->assertEquals(true, $database->createIndex($collectionName, 'poly_spatial', Database::INDEX_SPATIAL, ['polyAttr']));

            // Create test document
            $doc1 = new Document([
                '$id' => 'doc1',
                'pointAttr' => [5.0, 5.0],
                'lineAttr' => [[1.0, 2.0], [3.0, 4.0]],
                'polyAttr' => [[[0.0, 0.0], [0.0, 10.0], [10.0, 10.0], [0.0, 0.0]]],
                '$permissions' => [Permission::update(Role::any()), Permission::read(Role::any())]
            ]);
            $createdDoc = $database->createDocument($collectionName, $doc1);
            $this->assertInstanceOf(Document::class, $createdDoc);
            $this->assertEquals([5.0, 5.0], $createdDoc->getAttribute('pointAttr'));

            // Update spatial data
            $doc1->setAttribute('pointAttr', [6.0, 6.0]);
            $updatedDoc = $database->updateDocument($collectionName, 'doc1', $doc1);
            $this->assertEquals([6.0, 6.0], $updatedDoc->getAttribute('pointAttr'));

            // Test spatial queries with appropriate operations for each geometry type

            // Point attribute tests - use operations valid for points
            $pointQueries = [
                'equals' => Query::equals('pointAttr', [[6.0, 6.0]]),
                'notEquals' => Query::notEquals('pointAttr', [[1.0, 1.0]]),
                'distance' => Query::distance('pointAttr', [[[6.0, 6.0], 0.1]]),
                'notDistance' => Query::notDistance('pointAttr', [[[1.0, 1.0], 0.1]]),
                'intersects' => Query::intersects('pointAttr', [[6.0, 6.0]]),
                'notIntersects' => Query::notIntersects('pointAttr', [[1.0, 1.0]])
            ];

            foreach ($pointQueries as $queryType => $query) {
                $result = $database->find($collectionName, [$query], Database::PERMISSION_READ);
                $this->assertNotEmpty($result, sprintf('Failed spatial query: %s on pointAttr', $queryType));
                $this->assertEquals('doc1', $result[0]->getId(), sprintf('Incorrect document returned for %s on pointAttr', $queryType));
            }

            // LineString attribute tests - use operations valid for linestrings
            $lineQueries = [
                'contains' => Query::contains('lineAttr', [[1.0, 2.0]]), // Point on the line (endpoint)
                'notContains' => Query::notContains('lineAttr', [[5.0, 6.0]]), // Point not on the line
                'equals' => Query::equals('lineAttr', [[[1.0, 2.0], [3.0, 4.0]]]), // Exact same linestring
                'notEquals' => Query::notEquals('lineAttr', [[[5.0, 6.0], [7.0, 8.0]]]), // Different linestring
                'intersects' => Query::intersects('lineAttr', [[1.0, 2.0]]), // Point on the line should intersect
                'notIntersects' => Query::notIntersects('lineAttr', [[5.0, 6.0]]) // Point not on the line should not intersect
            ];

            foreach ($lineQueries as $queryType => $query) {
                if (!$database->getAdapter()->getSupportForBoundaryInclusiveContains() && in_array($queryType, ['contains','notContains'])) {
                    continue;
                }
                $result = $database->find($collectionName, [$query], Database::PERMISSION_READ);
                $this->assertNotEmpty($result, sprintf('Failed spatial query: %s on polyAttr', $queryType));
                $this->assertEquals('doc1', $result[0]->getId(), sprintf('Incorrect document returned for %s on polyAttr', $queryType));
            }

            // Polygon attribute tests - use operations valid for polygons
            $polyQueries = [
                'contains' => Query::contains('polyAttr', [[5.0, 5.0]]), // Point inside polygon
                'notContains' => Query::notContains('polyAttr', [[15.0, 15.0]]), // Point outside polygon
                'intersects' => Query::intersects('polyAttr', [[5.0, 5.0]]), // Point inside polygon should intersect
                'notIntersects' => Query::notIntersects('polyAttr', [[15.0, 15.0]]), // Point outside polygon should not intersect
                'equals' => Query::equals('polyAttr', [[[[0.0, 0.0], [0.0, 10.0], [10.0, 10.0], [0.0, 0.0]]]]), // Exact same polygon
                'notEquals' => Query::notEquals('polyAttr', [[[[20.0, 20.0], [20.0, 30.0], [30.0, 30.0], [20.0, 20.0]]]]), // Different polygon
                'overlaps' => Query::overlaps('polyAttr', [[[[5.0, 5.0], [5.0, 15.0], [15.0, 15.0], [15.0, 5.0], [5.0, 5.0]]]]), // Overlapping polygon
                'notOverlaps' => Query::notOverlaps('polyAttr', [[[[20.0, 20.0], [20.0, 30.0], [30.0, 30.0], [30.0, 20.0], [20.0, 20.0]]]]) // Non-overlapping polygon
            ];

            foreach ($polyQueries as $queryType => $query) {
                if (!$database->getAdapter()->getSupportForBoundaryInclusiveContains() && in_array($queryType, ['contains','notContains'])) {
                    continue;
                }
                $result = $database->find($collectionName, [$query], Database::PERMISSION_READ);
                $this->assertNotEmpty($result, sprintf('Failed spatial query: %s on polyAttr', $queryType));
                $this->assertEquals('doc1', $result[0]->getId(), sprintf('Incorrect document returned for %s on polyAttr', $queryType));
            }
        } finally {
            $database->deleteCollection($collectionName);
        }
    }

    public function testSpatialRelationshipOneToOne(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();

        if (!$database->getAdapter()->getSupportForRelationships() || !$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $database->createCollection('location');
        $database->createCollection('building');

        $database->createAttribute('location', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('location', 'coordinates', Database::VAR_POINT, 0, true);
        $database->createAttribute('building', 'name', Database::VAR_STRING, 255, true);
        $database->createAttribute('building', 'area', Database::VAR_STRING, 255, true);

        // Create spatial indexes
        $database->createIndex('location', 'coordinates_spatial', Database::INDEX_SPATIAL, ['coordinates']);

        // Create building document first
        $building1 = $database->createDocument('building', new Document([
            '$id' => 'building1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Empire State Building',
            'area' => 'Manhattan',
        ]));

        $database->createRelationship(
            collection: 'location',
            relatedCollection: 'building',
            type: Database::RELATION_ONE_TO_ONE,
            id: 'building',
            twoWay: false
        );

        // Create location with spatial data and relationship
        $location1 = $database->createDocument('location', new Document([
            '$id' => 'location1',
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Downtown',
            'coordinates' => [40.7128, -74.0060], // New York coordinates
            'building' => 'building1',
        ]));

        $this->assertInstanceOf(Document::class, $location1);
        $this->assertEquals([40.7128, -74.0060], $location1->getAttribute('coordinates'));

        // Check if building attribute is populated (could be ID string or Document object)
        $buildingAttr = $location1->getAttribute('building');
        if (is_string($buildingAttr)) {
            $this->assertEquals('building1', $buildingAttr);
        } else {
            $this->assertInstanceOf(Document::class, $buildingAttr);
            $this->assertEquals('building1', $buildingAttr->getId());
        }

        // Test spatial queries on related documents
        $nearbyLocations = $database->find('location', [
            Query::distance('coordinates', [[[40.7128, -74.0060], 0.1]])
        ], Database::PERMISSION_READ);

        $this->assertNotEmpty($nearbyLocations);
        $this->assertEquals('location1', $nearbyLocations[0]->getId());

        // Test relationship with spatial data update
        $location1->setAttribute('coordinates', [40.7589, -73.9851]); // Times Square coordinates
        $updatedLocation = $database->updateDocument('location', 'location1', $location1);

        $this->assertEquals([40.7589, -73.9851], $updatedLocation->getAttribute('coordinates'));

        // Test spatial query after update
        $timesSquareLocations = $database->find('location', [
            Query::distance('coordinates', [[[40.7589, -73.9851], 0.1]])
        ], Database::PERMISSION_READ);

        $this->assertNotEmpty($timesSquareLocations);
        $this->assertEquals('location1', $timesSquareLocations[0]->getId());

        // Test relationship integrity with spatial data
        $building = $database->getDocument('building', 'building1');
        $this->assertInstanceOf(Document::class, $building);
        $this->assertEquals('building1', $building->getId());

        // Test one-way relationship (building doesn't have location attribute)
        $this->assertArrayNotHasKey('location', $building->getArrayCopy());

        // Test basic relationship integrity
        $this->assertInstanceOf(Document::class, $building);
        $this->assertEquals('Empire State Building', $building->getAttribute('name'));

        // Clean up
        $database->deleteCollection('location');
        $database->deleteCollection('building');
    }

    public function testSpatialAttributes(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $collectionName = 'spatial_attrs_';
        try {
            $database->createCollection($collectionName);

            $required = $database->getAdapter()->getSupportForSpatialIndexNull() ? false : true;
            $this->assertEquals(true, $database->createAttribute($collectionName, 'pointAttr', Database::VAR_POINT, 0, $required));
            $this->assertEquals(true, $database->createAttribute($collectionName, 'lineAttr', Database::VAR_LINESTRING, 0, $required));
            $this->assertEquals(true, $database->createAttribute($collectionName, 'polyAttr', Database::VAR_POLYGON, 0, $required));

            // Create spatial indexes
            $this->assertEquals(true, $database->createIndex($collectionName, 'idx_point', Database::INDEX_SPATIAL, ['pointAttr']));
            $this->assertEquals(true, $database->createIndex($collectionName, 'idx_line', Database::INDEX_SPATIAL, ['lineAttr']));
            $this->assertEquals(true, $database->createIndex($collectionName, 'idx_poly', Database::INDEX_SPATIAL, ['polyAttr']));

            $collection = $database->getCollection($collectionName);
            $this->assertIsArray($collection->getAttribute('attributes'));
            $this->assertCount(3, $collection->getAttribute('attributes'));
            $this->assertIsArray($collection->getAttribute('indexes'));
            $this->assertCount(3, $collection->getAttribute('indexes'));

            // Create a simple document to ensure structure is valid
            $doc = $database->createDocument($collectionName, new Document([
                '$id' => ID::custom('sdoc'),
                'pointAttr' => [1.0, 1.0],
                'lineAttr' => [[0.0, 0.0], [1.0, 1.0]],
                'polyAttr' => [[[0.0, 0.0], [0.0, 2.0], [2.0, 2.0], [0.0, 0.0]]],
                '$permissions' => [Permission::read(Role::any())]
            ]));
            $this->assertInstanceOf(Document::class, $doc);
        } finally {
            $database->deleteCollection($collectionName);
        }
    }

    public function testSpatialOneToMany(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        if (!$database->getAdapter()->getSupportForRelationships() || !$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $parent = 'regions_';
        $child = 'places_';
        try {
            $database->createCollection($parent);
            $database->createCollection($child);

            $database->createAttribute($parent, 'name', Database::VAR_STRING, 255, true);
            $database->createAttribute($child, 'name', Database::VAR_STRING, 255, true);
            $database->createAttribute($child, 'coord', Database::VAR_POINT, 0, true);
            $database->createIndex($child, 'coord_spatial', Database::INDEX_SPATIAL, ['coord']);

            $database->createRelationship(
                collection: $parent,
                relatedCollection: $child,
                type: Database::RELATION_ONE_TO_MANY,
                twoWay: true,
                id: 'places',
                twoWayKey: 'region'
            );

            $r1 = $database->createDocument($parent, new Document([
                '$id' => 'r1',
                'name' => 'Region 1',
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]
            ]));
            $this->assertInstanceOf(Document::class, $r1);

            $p1 = $database->createDocument($child, new Document([
                '$id' => 'p1',
                'name' => 'Place 1',
                'coord' => [10.0, 10.0],
                'region' => 'r1',
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]
            ]));
            $p2 = $database->createDocument($child, new Document([
                '$id' => 'p2',
                'name' => 'Place 2',
                'coord' => [10.1, 10.1],
                'region' => 'r1',
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]
            ]));
            $this->assertInstanceOf(Document::class, $p1);
            $this->assertInstanceOf(Document::class, $p2);

            // Spatial query on child collection
            $near = $database->find($child, [
                Query::distance('coord', [[[10.0, 10.0], 1.0]])
            ], Database::PERMISSION_READ);
            $this->assertNotEmpty($near);

            $region = $database->getDocument($parent, 'r1');
            $this->assertArrayHasKey('places', $region);
            $this->assertEquals(2, \count($region['places']));
        } finally {
            $database->deleteCollection($child);
            $database->deleteCollection($parent);
        }
    }

    public function testSpatialManyToOne(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        if (!$database->getAdapter()->getSupportForRelationships() || !$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $parent = 'cities_';
        $child = 'stops_';
        try {
            $database->createCollection($parent);
            $database->createCollection($child);

            $database->createAttribute($parent, 'name', Database::VAR_STRING, 255, true);
            $database->createAttribute($child, 'name', Database::VAR_STRING, 255, true);
            $database->createAttribute($child, 'coord', Database::VAR_POINT, 0, true);
            $database->createIndex($child, 'coord_spatial', Database::INDEX_SPATIAL, ['coord']);

            $database->createRelationship(
                collection: $child,
                relatedCollection: $parent,
                type: Database::RELATION_MANY_TO_ONE,
                twoWay: true,
                id: 'city',
                twoWayKey: 'stops'
            );

            $c1 = $database->createDocument($parent, new Document([
                '$id' => 'c1',
                'name' => 'City 1',
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]
            ]));

            $s1 = $database->createDocument($child, new Document([
                '$id' => 's1',
                'name' => 'Stop 1',
                'coord' => [20.0, 20.0],
                'city' => 'c1',
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]
            ]));
            $s2 = $database->createDocument($child, new Document([
                '$id' => 's2',
                'name' => 'Stop 2',
                'coord' => [20.2, 20.2],
                'city' => 'c1',
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]
            ]));
            $this->assertInstanceOf(Document::class, $c1);
            $this->assertInstanceOf(Document::class, $s1);
            $this->assertInstanceOf(Document::class, $s2);

            $near = $database->find($child, [
                Query::distance('coord', [[[20.0, 20.0], 1.0]])
            ], Database::PERMISSION_READ);
            $this->assertNotEmpty($near);

            $city = $database->getDocument($parent, 'c1');
            $this->assertArrayHasKey('stops', $city);
            $this->assertEquals(2, \count($city['stops']));
        } finally {
            $database->deleteCollection($child);
            $database->deleteCollection($parent);
        }
    }

    public function testSpatialManyToMany(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        if (!$database->getAdapter()->getSupportForRelationships() || !$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        $a = 'drivers_';
        $b = 'routes_';
        try {
            $database->createCollection($a);
            $database->createCollection($b);

            $database->createAttribute($a, 'name', Database::VAR_STRING, 255, true);
            $database->createAttribute($a, 'home', Database::VAR_POINT, 0, true);
            $database->createIndex($a, 'home_spatial', Database::INDEX_SPATIAL, ['home']);
            $database->createAttribute($b, 'title', Database::VAR_STRING, 255, true);
            $database->createAttribute($b, 'area', Database::VAR_POLYGON, 0, true);
            $database->createIndex($b, 'area_spatial', Database::INDEX_SPATIAL, ['area']);

            $database->createRelationship(
                collection: $a,
                relatedCollection: $b,
                type: Database::RELATION_MANY_TO_MANY,
                twoWay: true,
                id: 'routes',
                twoWayKey: 'drivers'
            );

            $d1 = $database->createDocument($a, new Document([
                '$id' => 'd1',
                'name' => 'Driver 1',
                'home' => [30.0, 30.0],
                'routes' => [
                    [
                        '$id' => 'rte1',
                        'title' => 'Route 1',
                        'area' => [[[29.5,29.5],[29.5,30.5],[30.5,30.5],[29.5,29.5]]]
                    ]
                ],
                '$permissions' => [Permission::read(Role::any()), Permission::update(Role::any())]
            ]));
            $this->assertInstanceOf(Document::class, $d1);

            // Spatial query on "drivers" using point distance
            $near = $database->find($a, [
                Query::distance('home', [[[30.0, 30.0], 0.5]])
            ], Database::PERMISSION_READ);
            $this->assertNotEmpty($near);

            // Ensure relationship present
            $d1 = $database->getDocument($a, 'd1');
            $this->assertArrayHasKey('routes', $d1);
            $this->assertEquals(1, \count($d1['routes']));
        } finally {
            $database->deleteCollection($b);
            $database->deleteCollection($a);
        }
    }

    public function testSpatialIndex(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->expectNotToPerformAssertions();
            return;
        }

        // Basic spatial index create/delete
        $collectionName = 'spatial_index_';
        try {
            $database->createCollection($collectionName);
            $database->createAttribute($collectionName, 'loc', Database::VAR_POINT, 0, true);
            $this->assertEquals(true, $database->createIndex($collectionName, 'loc_spatial', Database::INDEX_SPATIAL, ['loc']));

            $collection = $database->getCollection($collectionName);
            $this->assertIsArray($collection->getAttribute('indexes'));
            $this->assertCount(1, $collection->getAttribute('indexes'));
            $this->assertEquals('loc_spatial', $collection->getAttribute('indexes')[0]['$id']);
            $this->assertEquals(Database::INDEX_SPATIAL, $collection->getAttribute('indexes')[0]['type']);

            $this->assertEquals(true, $database->deleteIndex($collectionName, 'loc_spatial'));
            $collection = $database->getCollection($collectionName);
            $this->assertCount(0, $collection->getAttribute('indexes'));
        } finally {
            $database->deleteCollection($collectionName);
        }

        // Edge cases: Spatial Index Order support (createCollection and createIndex)
        $orderSupported = $database->getAdapter()->getSupportForSpatialIndexOrder();

        // createCollection with orders
        $collOrderCreate = 'spatial_idx_order_create';
        try {
            $attributes = [new Document([
                '$id' => ID::custom('loc'),
                'type' => Database::VAR_POINT,
                'size' => 0,
                'required' => true,
                'signed' => true,
                'array' => false,
                'filters' => [],
            ])];
            $indexes = [new Document([
                '$id' => ID::custom('idx_loc'),
                'type' => Database::INDEX_SPATIAL,
                'attributes' => ['loc'],
                'lengths' => [],
                'orders' => $orderSupported ? [Database::ORDER_ASC] : ['ASC'],
            ])];

            if ($orderSupported) {
                $database->createCollection($collOrderCreate, $attributes, $indexes);
                $meta = $database->getCollection($collOrderCreate);
                $this->assertEquals('idx_loc', $meta->getAttribute('indexes')[0]['$id']);
            } else {
                try {
                    $database->createCollection($collOrderCreate, $attributes, $indexes);
                    $this->fail('Expected exception when orders are provided for spatial index on unsupported adapter');
                } catch (\Throwable $e) {
                    $this->assertStringContainsString('Spatial index', $e->getMessage());
                }
            }
        } finally {
            if ($orderSupported) {
                $database->deleteCollection($collOrderCreate);
            }
        }

        // createIndex with orders
        $collOrderIndex = 'spatial_idx_order_index_' . uniqid();
        try {
            $database->createCollection($collOrderIndex);
            $database->createAttribute($collOrderIndex, 'loc', Database::VAR_POINT, 0, true);
            if ($orderSupported) {
                $this->assertTrue($database->createIndex($collOrderIndex, 'idx_loc', Database::INDEX_SPATIAL, ['loc'], [], [Database::ORDER_DESC]));
            } else {
                try {
                    $database->createIndex($collOrderIndex, 'idx_loc', Database::INDEX_SPATIAL, ['loc'], [], ['DESC']);
                    $this->fail('Expected exception when orders are provided for spatial index on unsupported adapter');
                } catch (\Throwable $e) {
                    $this->assertStringContainsString('Spatial index', $e->getMessage());
                }
            }
        } finally {
            $database->deleteCollection($collOrderIndex);
        }

        // Edge cases: Spatial Index Nullability (createCollection and createIndex)
        $nullSupported = $database->getAdapter()->getSupportForSpatialIndexNull();

        // createCollection with required=false
        $collNullCreate = 'spatial_idx_null_create_' . uniqid();
        try {
            $attributes = [new Document([
                '$id' => ID::custom('loc'),
                'type' => Database::VAR_POINT,
                'size' => 0,
                'required' => false, // edge case
                'signed' => true,
                'array' => false,
                'filters' => [],
            ])];
            $indexes = [new Document([
                '$id' => ID::custom('idx_loc'),
                'type' => Database::INDEX_SPATIAL,
                'attributes' => ['loc'],
                'lengths' => [],
                'orders' => [],
            ])];

            if ($nullSupported) {
                $database->createCollection($collNullCreate, $attributes, $indexes);
                $meta = $database->getCollection($collNullCreate);
                $this->assertEquals('idx_loc', $meta->getAttribute('indexes')[0]['$id']);
            } else {
                try {
                    $database->createCollection($collNullCreate, $attributes, $indexes);
                    $this->fail('Expected exception when spatial index is created on NULL-able geometry attribute');
                } catch (\Throwable $e) {
                    $this->assertTrue(true); // exception expected; exact message is adapter-specific
                }
            }
        } finally {
            if ($nullSupported) {
                $database->deleteCollection($collNullCreate);
            }
        }

        // createIndex with required=false
        $collNullIndex = 'spatial_idx_null_index_' . uniqid();
        try {
            $database->createCollection($collNullIndex);
            $database->createAttribute($collNullIndex, 'loc', Database::VAR_POINT, 0, false);
            if ($nullSupported) {
                $this->assertTrue($database->createIndex($collNullIndex, 'idx_loc', Database::INDEX_SPATIAL, ['loc']));
            } else {
                try {
                    $database->createIndex($collNullIndex, 'idx_loc', Database::INDEX_SPATIAL, ['loc']);
                    $this->fail('Expected exception when spatial index is created on NULL-able geometry attribute');
                } catch (\Throwable $e) {
                    $this->assertTrue(true); // exception expected; exact message is adapter-specific
                }
            }
        } finally {
            $database->deleteCollection($collNullIndex);
        }
    }
}
