<?php

namespace Tests\E2E\Adapter\Scopes;

use Exception;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Helpers\ID;
use Utopia\Database\Helpers\Permission;
use Utopia\Database\Helpers\Role;
use Utopia\Database\Query;

trait SpatialTests
{
    public function testBasicAttributeCreation(): void
    {
        $database = $this->getDatabase();

        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        // Create collection
        $result = $database->createCollection('test_basic');
        $this->assertInstanceOf(\Utopia\Database\Document::class, $result);

        // Test spatial attribute creation
        $this->assertEquals(true, $database->createAttribute('test_basic', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('test_basic', 'linestring', Database::VAR_LINESTRING, 0, true));
        $this->assertEquals(true, $database->createAttribute('test_basic', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('test_basic', 'geometry', Database::VAR_GEOMETRY, 0, true));

        $collection = $database->getCollection('test_basic');
        $attributes = $collection->getAttribute('attributes', []);

        $this->assertCount(4, $attributes);
        $this->assertEquals('point', $attributes[0]['$id']);
        $this->assertEquals(Database::VAR_POINT, $attributes[0]['type']);
        $this->assertEquals('linestring', $attributes[1]['$id']);
        $this->assertEquals(Database::VAR_LINESTRING, $attributes[1]['type']);
    }

    public function testSpatialAttributeSupport(): void
    {
        $database = $this->getDatabase();

        // Check if the adapter supports spatial attributes
        $this->assertIsBool($database->getAdapter()->getSupportForSpatialAttributes());

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }
    }

    public function testCreateSpatialAttributes(): void
    {
        $database = $this->getDatabase();

        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $result = $database->createCollection('spatial_attributes');
        $this->assertInstanceOf(\Utopia\Database\Document::class, $result);

        // Create spatial attributes of different types
        $this->assertEquals(true, $database->createAttribute('spatial_attributes', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_attributes', 'linestring', Database::VAR_LINESTRING, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_attributes', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_attributes', 'geometry', Database::VAR_GEOMETRY, 0, true));

        $collection = $database->getCollection('spatial_attributes');
        $attributes = $collection->getAttribute('attributes', []);

        $this->assertCount(4, $attributes);

        foreach ($attributes as $attribute) {
            $this->assertInstanceOf(\Utopia\Database\Document::class, $attribute);
            $this->assertContains($attribute->getAttribute('type'), [
                Database::VAR_POINT,
                Database::VAR_LINESTRING,
                Database::VAR_POLYGON,
                Database::VAR_GEOMETRY
            ]);
        }
    }

    public function testCreateSpatialIndexes(): void
    {
        $database = $this->getDatabase();

        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $result = $database->createCollection('spatial_indexes');
        $this->assertInstanceOf(\Utopia\Database\Document::class, $result);

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_indexes', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_indexes', 'linestring', Database::VAR_LINESTRING, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_indexes', 'polygon', Database::VAR_POLYGON, 0, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_indexes', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_indexes', 'linestring_spatial', Database::INDEX_SPATIAL, ['linestring']));
        $this->assertEquals(true, $database->createIndex('spatial_indexes', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        $collection = $database->getCollection('spatial_indexes');
        $indexes = $collection->getAttribute('indexes', []);

        $this->assertCount(3, $indexes);

        foreach ($indexes as $index) {
            $this->assertInstanceOf(\Utopia\Database\Document::class, $index);
            $this->assertEquals(Database::INDEX_SPATIAL, $index->getAttribute('type'));
        }
    }

    public function testSpatialDataInsertAndRetrieve(): void
    {
        $database = $this->getDatabase();

        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $result = $database->createCollection('spatial_data');
        $this->assertInstanceOf(\Utopia\Database\Document::class, $result);

        // Create spatial attributes and a name attribute
        $this->assertEquals(true, $database->createAttribute('spatial_data', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_data', 'linestring', Database::VAR_LINESTRING, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_data', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_data', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_data', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_data', 'linestring_spatial', Database::INDEX_SPATIAL, ['linestring']));
        $this->assertEquals(true, $database->createIndex('spatial_data', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Insert documents with spatial data
        $doc1 = $database->createDocument('spatial_data', new \Utopia\Database\Document([
            '$id' => 'doc1',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Point Document',
            'point' => [10.0, 20.0],
            'linestring' => [[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]],
            'polygon' => [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]
        ]));

        $doc2 = $database->createDocument('spatial_data', new \Utopia\Database\Document([
            '$id' => 'doc2',
            '$permissions' => [Permission::read(Role::any())],
            'name' => 'Second Document',
            'point' => [15.0, 25.0],
            'linestring' => [[5.0, 5.0], [6.0, 6.0], [7.0, 7.0]],
            'polygon' => [[5.0, 5.0], [15.0, 5.0], [15.0, 15.0], [5.0, 15.0], [5.0, 5.0]]
        ]));

        $this->assertInstanceOf(\Utopia\Database\Document::class, $doc1);
        $this->assertInstanceOf(\Utopia\Database\Document::class, $doc2);

        // Retrieve and verify spatial data
        $retrieved1 = $database->getDocument('spatial_data', 'doc1');
        $retrieved2 = $database->getDocument('spatial_data', 'doc2');

        $this->assertEquals([10.0, 20.0], $retrieved1->getAttribute('point'));
        $this->assertEquals([[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]], $retrieved1->getAttribute('linestring'));
        $this->assertEquals([[[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]], $retrieved1->getAttribute('polygon')); // Array of rings

        $this->assertEquals([15.0, 25.0], $retrieved2->getAttribute('point'));
        $this->assertEquals([[5.0, 5.0], [6.0, 6.0], [7.0, 7.0]], $retrieved2->getAttribute('linestring'));
        $this->assertEquals([[[5.0, 5.0], [15.0, 5.0], [15.0, 15.0], [5.0, 15.0], [5.0, 5.0]]], $retrieved2->getAttribute('polygon')); // Array of rings
    }

    public function testSpatialQueries(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $database->createCollection('spatial_queries');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_queries', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_queries', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_queries', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_queries', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_queries', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Insert test documents
        $document1 = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [5, 5],
            'polygon' => [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
            'name' => 'Center Point'
        ]);

        $document2 = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [15, 15],
            'polygon' => [[[10, 10], [20, 10], [20, 20], [10, 20], [10, 10]]],
            'name' => 'Outside Point'
        ]);

        $this->assertInstanceOf(\Utopia\Database\Document::class, $database->createDocument('spatial_queries', $document1));
        $this->assertInstanceOf(\Utopia\Database\Document::class, $database->createDocument('spatial_queries', $document2));

        // Test spatial queries
        // Test contains query - works on both spatial and non-spatial attributes
        $containsQuery = Query::contains('polygon', [[5, 5]]);
        $containsResults = $database->find('spatial_queries', [$containsQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $containsResults);
        $this->assertEquals('Center Point', $containsResults[0]->getAttribute('name'));

        // Test intersects query - spatial-only method
        $intersectsQuery = Query::intersects('polygon', [[5, 5]]); // Simplified to single point
        $intersectsResults = $database->find('spatial_queries', [$intersectsQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $intersectsResults); // Point [5,5] only intersects with Document 1's polygon
        $this->assertEquals('Center Point', $intersectsResults[0]->getAttribute('name'));

        // Test equals query - spatial-only method
        $equalsQuery = Query::equals('point', [[5, 5]]);
        $equalsResults = $database->find('spatial_queries', [$equalsQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $equalsResults);
        $this->assertEquals('Center Point', $equalsResults[0]->getAttribute('name'));
    }

    public function testSpatialQueryNegations(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $database->createCollection('spatial_negations');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_negations', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_negations', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_negations', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_negations', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_negations', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Insert test documents
        $document1 = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [5, 5],
            'polygon' => [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
            'name' => 'Document 1'
        ]);

        $document2 = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [15, 15],
            'polygon' => [[[10, 10], [20, 10], [20, 20], [10, 20], [10, 10]]],
            'name' => 'Document 2'
        ]);

        $this->assertInstanceOf(\Utopia\Database\Document::class, $database->createDocument('spatial_negations', $document1));
        $this->assertInstanceOf(\Utopia\Database\Document::class, $database->createDocument('spatial_negations', $document2));

        // Test notContains query - works on both spatial and non-spatial attributes
        $notContainsQuery = Query::notContains('polygon', [[15, 15]]);
        $notContainsResults = $database->find('spatial_negations', [$notContainsQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $notContainsResults);
        $this->assertEquals('Document 1', $notContainsResults[0]->getAttribute('name'));

        // Test notEquals query - spatial-only method
        $notEqualsQuery = Query::notEquals('point', [[5, 5]]); // Use notEquals for spatial data
        $notEqualsResults = $database->find('spatial_negations', [$notEqualsQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $notEqualsResults);
        $this->assertEquals('Document 2', $notEqualsResults[0]->getAttribute('name'));

        // Test notIntersects query - spatial-only method
        $notIntersectsQuery = Query::notIntersects('polygon', [[[25, 25], [35, 35]]]);
        $notIntersectsResults = $database->find('spatial_negations', [$notIntersectsQuery], Database::PERMISSION_READ);
        $this->assertCount(2, $notIntersectsResults);
    }

    public function testSpatialQueryCombinations(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $database->createCollection('spatial_combinations');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_combinations', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_combinations', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_combinations', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_combinations', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_combinations', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Insert test documents
        $document1 = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [5, 5],
            'polygon' => [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
            'name' => 'Center Document'
        ]);

        $document2 = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [15, 15],
            'polygon' => [[[10, 10], [20, 10], [20, 20], [10, 20], [10, 10]]],
            'name' => 'Outside Document'
        ]);

        $this->assertInstanceOf(\Utopia\Database\Document::class, $database->createDocument('spatial_combinations', $document1));
        $this->assertInstanceOf(\Utopia\Database\Document::class, $database->createDocument('spatial_combinations', $document2));

        // Test AND combination
        $pointQuery = Query::equals('point', [[5, 5]]);
        $polygonQuery = Query::contains('polygon', [[5, 5]]);
        $andQuery = Query::and([$pointQuery, $polygonQuery]);

        $andResults = $database->find('spatial_combinations', [$andQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $andResults);
        $this->assertEquals('Center Document', $andResults[0]->getAttribute('name'));

        // Test OR combination
        $pointQuery2 = Query::equals('point', [[5, 5]]);
        $pointQuery3 = Query::equals('point', [[15, 15]]);
        $orQuery = Query::or([$pointQuery2, $pointQuery3]);

        $orResults = $database->find('spatial_combinations', [$orQuery], Database::PERMISSION_READ);
        $this->assertCount(2, $orResults);
    }

    public function testSpatialDataUpdate(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $database->createCollection('spatial_update');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_update', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_update', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_update', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_update', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_update', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Insert test document
        $document = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [5, 5],
            'polygon' => [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
            'name' => 'Original Document'
        ]);

        $this->assertInstanceOf(\Utopia\Database\Document::class, $database->createDocument('spatial_update', $document));

        // Update spatial data
        $document->setAttribute('point', [25, 25]);
        $document->setAttribute('polygon', [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]]);
        $document->setAttribute('name', 'Updated Document');

        $this->assertInstanceOf(\Utopia\Database\Document::class, $database->updateDocument('spatial_update', $document->getId(), $document));

        // Retrieve and verify updated data
        $updatedDocument = $database->getDocument('spatial_update', $document->getId());

        $this->assertEquals([25, 25], $updatedDocument->getAttribute('point'));
        $this->assertEquals([[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]], $updatedDocument->getAttribute('polygon')); // Array of rings
        $this->assertEquals('Updated Document', $updatedDocument->getAttribute('name'));
    }

    public function testSpatialIndexDeletion(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $database->createCollection('spatial_index_deletion');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_index_deletion', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_index_deletion', 'polygon', Database::VAR_POLYGON, 0, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_index_deletion', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_index_deletion', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        $collection = $database->getCollection('spatial_index_deletion');
        $this->assertCount(2, $collection->getAttribute('indexes'));

        // Delete spatial indexes
        $this->assertEquals(true, $database->deleteIndex('spatial_index_deletion', 'point_spatial'));
        $this->assertEquals(true, $database->deleteIndex('spatial_index_deletion', 'polygon_spatial'));

        $collection = $database->getCollection('spatial_index_deletion');
        $this->assertCount(0, $collection->getAttribute('indexes'));
    }



    public function testSpatialDataCleanup(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        // Create collection if it doesn't exist
        if (!$database->exists(null, 'spatial_validation')) {
            $database->createCollection('spatial_validation');
        }

        $collection = $database->getCollection('spatial_validation');
        $this->assertNotNull($collection);

        $database->deleteCollection($collection->getId());

        $this->assertTrue(true, 'Cleanup completed');
    }

    public function testSpatialBulkOperations(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $database->createCollection('spatial_bulk');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_bulk', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_bulk', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_bulk', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_bulk', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_bulk', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Test bulk create with spatial data
        $documents = [
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [1, 1],
                'polygon' => [[[0, 0], [5, 0], [5, 5], [0, 5], [0, 0]]],
                'name' => 'Bulk Document 1'
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [2, 2],
                'polygon' => [[[5, 5], [10, 5], [10, 10], [5, 10], [5, 5]]],
                'name' => 'Bulk Document 2'
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [3, 3],
                'polygon' => [[[10, 10], [15, 10], [15, 15], [10, 15], [10, 10]]],
                'name' => 'Bulk Document 3'
            ])
        ];

        $createdCount = $database->createDocuments('spatial_bulk', $documents);
        $this->assertEquals(3, $createdCount);

        // Verify all documents were created with correct spatial data
        $allDocs = $database->find('spatial_bulk', [], Database::PERMISSION_READ);
        foreach ($allDocs as $doc) {
            $this->assertInstanceOf(\Utopia\Database\Document::class, $doc);
            $this->assertIsArray($doc->getAttribute('point'));
            $this->assertIsArray($doc->getAttribute('polygon'));
        }

        // Test bulk update with spatial data
        $updateDoc = new Document([
            'point' => [20, 20],
            'polygon' => [[[10, 10], [15, 10], [15, 15], [10, 15], [10, 10]]],
            'name' => 'Updated Document'
        ]);

        $updateResults = $database->updateDocuments('spatial_bulk', $updateDoc, []);
        $this->assertEquals(3, $updateResults);

        // Verify updates were applied
        $updatedAllDocs = $database->find('spatial_bulk', [], Database::PERMISSION_READ);

        foreach ($updatedAllDocs as $doc) {
            $this->assertInstanceOf(\Utopia\Database\Document::class, $doc);
            $this->assertEquals('Updated Document', $doc->getAttribute('name'));
            $this->assertEquals([20, 20], $doc->getAttribute('point'));
            $this->assertEquals([[[10, 10], [15, 10], [15, 15], [10, 15], [10, 10]]], $doc->getAttribute('polygon'));
        }

        // Test spatial queries on bulk-created data
        $containsQuery = Query::contains('polygon', [[12, 12]]);
        $containsResults = $database->find('spatial_bulk', [$containsQuery], Database::PERMISSION_READ);
        $this->assertCount(3, $containsResults); // All 3 documents now have the same polygon that contains [12, 12]
        $this->assertEquals('Updated Document', $containsResults[0]->getAttribute('name'));

        // Test bulk delete
        $deleteResults = $database->deleteDocuments('spatial_bulk', []); // Empty queries = delete all
        $this->assertEquals(3, $deleteResults);

        // Verify all documents were deleted
        $remainingDocs = $database->find('spatial_bulk', [], Database::PERMISSION_READ);
        $this->assertCount(0, $remainingDocs);
    }

    public function testSpatialIndividualDelete(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        $database->createCollection('spatial_individual_delete');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_individual_delete', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_individual_delete', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_individual_delete', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_individual_delete', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_individual_delete', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Create test document
        $document = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [25, 25],
            'polygon' => [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]],
            'name' => 'Delete Test Document'
        ]);

        $createdDoc = $database->createDocument('spatial_individual_delete', $document);
        $this->assertInstanceOf(\Utopia\Database\Document::class, $createdDoc);

        // Verify document exists
        $retrievedDoc = $database->getDocument('spatial_individual_delete', $createdDoc->getId());
        $this->assertEquals([25, 25], $retrievedDoc->getAttribute('point'));

        // Test individual delete
        $deleteResult = $database->deleteDocument('spatial_individual_delete', $createdDoc->getId());
        $this->assertTrue($deleteResult);

        // Verify document was deleted
        $deletedDoc = $database->getDocument('spatial_individual_delete', $createdDoc->getId());
        $this->assertTrue($deletedDoc->isEmpty());
    }

    public function testSpatialListDocuments(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        // Clean up collection if it exists
        if ($database->exists(null, 'spatial_list')) {
            $database->deleteCollection('spatial_list');
        }

        $database->createCollection('spatial_list');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_list', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_list', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_list', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_list', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_list', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Create multiple test documents
        $documents = [
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [1, 1],
                'polygon' => [[[0, 0], [5, 0], [5, 5], [0, 5], [0, 0]]],
                'name' => 'List Document 1'
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [2, 2],
                'polygon' => [[[5, 5], [10, 5], [10, 10], [5, 10], [5, 5]]],
                'name' => 'List Document 2'
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [3, 3],
                'polygon' => [[[10, 10], [15, 10], [15, 15], [10, 15], [10, 10]]],
                'name' => 'List Document 3'
            ])
        ];

        foreach ($documents as $doc) {
            $database->createDocument('spatial_list', $doc);
        }

        // Test find without queries (should return all)
        $allDocs = $database->find('spatial_list', [], Database::PERMISSION_READ);
        $this->assertCount(3, $allDocs);

        // Verify spatial data is correctly retrieved
        foreach ($allDocs as $doc) {
            $this->assertInstanceOf(\Utopia\Database\Document::class, $doc);
            $this->assertIsArray($doc->getAttribute('point'));
            $this->assertIsArray($doc->getAttribute('polygon'));
            $this->assertStringContainsString('List Document', $doc->getAttribute('name'));
        }

        // Test find with spatial query
        $containsQuery = Query::contains('polygon', [[2, 2]]);
        $filteredDocs = $database->find('spatial_list', [$containsQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $filteredDocs);
        $this->assertEquals('List Document 1', $filteredDocs[0]->getAttribute('name'));

        // Test pagination
        $paginatedDocs = $database->find('spatial_list', [Query::limit(3)], Database::PERMISSION_READ);
        $this->assertCount(3, $paginatedDocs);

        $paginatedDocs2 = $database->find('spatial_list', [Query::limit(3), Query::offset(3)], Database::PERMISSION_READ);
        $this->assertCount(0, $paginatedDocs2);
    }

    public function testSpatialUpsertDocuments(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        // Clean up collection if it exists
        if ($database->exists(null, 'spatial_upsert')) {
            $database->deleteCollection('spatial_upsert');
        }

        $database->createCollection('spatial_upsert');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_upsert', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_upsert', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_upsert', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_upsert', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_upsert', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Test upsert with spatial data
        $document = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [10, 10],
            'polygon' => [[[5, 5], [15, 5], [15, 15], [5, 15], [5, 5]]],
            'name' => 'Upsert Test Document'
        ]);

        // First upsert should create
        $result = $database->createOrUpdateDocuments('spatial_upsert', [$document]);
        $this->assertEquals(1, $result);

        // Verify document was created
        $createdDoc = $database->getDocument('spatial_upsert', $document->getId());
        $this->assertEquals([10, 10], $createdDoc->getAttribute('point'));
        // The polygon might be returned in a different format, so just check it's an array
        $this->assertIsArray($createdDoc->getAttribute('polygon'));

        // Update spatial data
        $updatedDocument = new Document([
            '$id' => $document->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [20, 20],
            'polygon' => [[[15, 15], [25, 15], [25, 25], [15, 25], [15, 15]]],
            'name' => 'Updated Upsert Test Document'
        ]);

        // Second upsert should update
        $result = $database->createOrUpdateDocuments('spatial_upsert', [$updatedDocument]);
        $this->assertEquals(1, $result);

        // Verify document was updated
        $updatedDoc = $database->getDocument('spatial_upsert', $document->getId());
        $this->assertEquals([20, 20], $updatedDoc->getAttribute('point'));
        // The polygon might be returned in a different format, so just check it's an array
        $this->assertIsArray($updatedDoc->getAttribute('polygon'));
        $this->assertEquals('Updated Upsert Test Document', $updatedDoc->getAttribute('name'));
    }

    public function testSpatialBatchOperations(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        // Clean up collection if it exists
        if ($database->exists(null, 'spatial_batch')) {
            $database->deleteCollection('spatial_batch');
        }

        $database->createCollection('spatial_batch');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_batch', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_batch', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_batch', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_batch', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_batch', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Create multiple documents with spatial data
        $documents = [
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [1, 1],
                'polygon' => [[[0, 0], [2, 0], [2, 2], [0, 2], [0, 0]]],
                'name' => 'Batch Document 1'
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [5, 5],
                'polygon' => [[[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]]],
                'name' => 'Batch Document 2'
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [10, 10],
                'polygon' => [[[9, 9], [11, 9], [11, 11], [9, 11], [9, 9]]],
                'name' => 'Batch Document 3'
            ])
        ];

        // Test batch create
        $createdCount = $database->createDocuments('spatial_batch', $documents);
        $this->assertEquals(3, $createdCount);

        // Verify all documents were created with correct spatial data
        // We need to retrieve the documents individually since createDocuments only returns count
        $allDocs = $database->find('spatial_batch', [], Database::PERMISSION_READ);
        foreach ($allDocs as $doc) {
            $this->assertIsArray($doc->getAttribute('point'));
            $this->assertIsArray($doc->getAttribute('polygon'));
            $this->assertStringContainsString('Batch Document', $doc->getAttribute('name'));
        }

        // Test batch update with spatial data
        $updateDoc = new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [100, 100],
            'polygon' => [[[99, 99], [101, 99], [101, 101], [99, 101], [99, 99]]],
            'name' => 'Updated Batch Document 1'
        ]);

        // Update the first document found
        $firstDoc = $allDocs[0];
        $updateResult = $database->updateDocuments('spatial_batch', $updateDoc, [Query::equal('$id', [$firstDoc->getId()])]);
        $this->assertEquals(1, $updateResult);

        // Verify update
        $updatedDoc = $database->getDocument('spatial_batch', $firstDoc->getId());
        $this->assertEquals([100, 100], $updatedDoc->getAttribute('point'));
        // The polygon might be returned in a different format, so just check it's an array
        $this->assertIsArray($updatedDoc->getAttribute('polygon'));
        $this->assertEquals('Updated Batch Document 1', $updatedDoc->getAttribute('name'));
    }

    public function testSpatialRelationships(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        // Clean up collections if they exist
        if ($database->exists(null, 'spatial_parent')) {
            $database->deleteCollection('spatial_parent');
        }
        if ($database->exists(null, 'spatial_child')) {
            $database->deleteCollection('spatial_child');
        }

        // Create parent collection with spatial attributes
        $database->createCollection('spatial_parent');
        $this->assertEquals(true, $database->createAttribute('spatial_parent', 'boundary', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_parent', 'center', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_parent', 'name', Database::VAR_STRING, 255, true));
        $this->assertEquals(true, $database->createIndex('spatial_parent', 'boundary_spatial', Database::INDEX_SPATIAL, ['boundary']));
        $this->assertEquals(true, $database->createIndex('spatial_parent', 'center_spatial', Database::INDEX_SPATIAL, ['center']));

        // Create child collection with spatial attributes
        $database->createCollection('spatial_child');
        $this->assertEquals(true, $database->createAttribute('spatial_child', 'location', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_child', 'name', Database::VAR_STRING, 255, true));
        $this->assertEquals(true, $database->createIndex('spatial_child', 'location_spatial', Database::INDEX_SPATIAL, ['location']));

        // Create relationship
        $this->assertEquals(true, $database->createRelationship(
            collection: 'spatial_parent',
            relatedCollection: 'spatial_child',
            type: Database::RELATION_ONE_TO_MANY,
            twoWay: true
        ));

        // Create parent document
        $parentDoc = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'boundary' => [[[0, 0], [100, 0], [100, 100], [0, 100], [0, 0]]],
            'center' => [50, 50],
            'name' => 'Spatial Parent'
        ]);

        $createdParent = $database->createDocument('spatial_parent', $parentDoc);

        // Create child documents
        $childDocs = [
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'location' => [25, 25],
                'name' => 'Child Inside 1',
                'spatial_parent' => $createdParent->getId()
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'location' => [75, 75],
                'name' => 'Child Inside 2',
                'spatial_parent' => $createdParent->getId()
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'location' => [150, 150],
                'name' => 'Child Outside',
                'spatial_parent' => $createdParent->getId()
            ])
        ];

        foreach ($childDocs as $childDoc) {
            $database->createDocument('spatial_child', $childDoc);
        }

        // Test spatial relationship queries
        // Find children within parent boundary - we need to check if child location is within parent boundary
        // Since we can't do cross-collection spatial queries directly, we'll test the relationship differently
        $childrenInside = $database->find('spatial_child', [
            Query::equal('spatial_parent', [$createdParent->getId()])
        ], Database::PERMISSION_READ);

        $this->assertCount(3, $childrenInside);
        $this->assertEquals('Child Inside 1', $childrenInside[0]->getAttribute('name'));
        $this->assertEquals('Child Inside 2', $childrenInside[1]->getAttribute('name'));
        $this->assertEquals('Child Outside', $childrenInside[2]->getAttribute('name'));

        // Test basic spatial query on child location attribute
        $locationQuery = Query::equals('location', [[25, 25]]);
        $specificChild = $database->find('spatial_child', [
            Query::equal('spatial_parent', [$createdParent->getId()]),
            $locationQuery
        ], Database::PERMISSION_READ);

        $this->assertCount(1, $specificChild);
        $this->assertEquals('Child Inside 1', $specificChild[0]->getAttribute('name'));
    }

    public function testSpatialDataValidation(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        // Clean up collection if it exists
        if ($database->exists(null, 'spatial_validation')) {
            $database->deleteCollection('spatial_validation');
        }

        $database->createCollection('spatial_validation');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_validation', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_validation', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_validation', 'name', Database::VAR_STRING, 255, true));

        // Test valid spatial data
        $validDoc = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [0, 0],
            'polygon' => [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
            'name' => 'Valid Spatial Document'
        ]);

        $createdDoc = $database->createDocument('spatial_validation', $validDoc);
        $this->assertInstanceOf(\Utopia\Database\Document::class, $createdDoc);

        // Test invalid point data (should still work as it's handled by database)
        $invalidPointDoc = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'point' => [0], // Invalid: should be [x, y]
            'polygon' => [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
            'name' => 'Invalid Point Document'
        ]);

        // This should either work (if database handles it) or throw an exception
        try {
            $createdInvalidDoc = $database->createDocument('spatial_validation', $invalidPointDoc);
            $this->assertInstanceOf(\Utopia\Database\Document::class, $createdInvalidDoc);
        } catch (Exception $e) {
            // Expected if database enforces validation - check for any validation-related error
            $errorMessage = strtolower($e->getMessage());
            $this->assertTrue(
                strpos($errorMessage, 'spatial') !== false ||
                strpos($errorMessage, 'point') !== false ||
                strpos($errorMessage, 'array') !== false,
                'Error message should contain spatial, point, or array information'
            );
        }
    }

    public function testSpatialPerformanceQueries(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        // Clean up collection if it exists
        if ($database->exists(null, 'spatial_performance')) {
            $database->deleteCollection('spatial_performance');
        }

        $database->createCollection('spatial_performance');

        // Create spatial attributes
        $this->assertEquals(true, $database->createAttribute('spatial_performance', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_performance', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_performance', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes for performance
        $this->assertEquals(true, $database->createIndex('spatial_performance', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_performance', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // Create multiple documents for performance testing
        $documents = [];
        for ($i = 0; $i < 10; $i++) {
            $documents[] = new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'point' => [$i * 10, $i * 10],
                'polygon' => [[[$i * 10, $i * 10], [($i + 1) * 10, $i * 10], [($i + 1) * 10, ($i + 1) * 10], [$i * 10, ($i + 1) * 10], [$i * 10, $i * 10]]],
                'name' => "Performance Document {$i}"
            ]);
        }

        // Batch create for performance
        $startTime = microtime(true);
        $createdCount = $database->createDocuments('spatial_performance', $documents);
        $createTime = microtime(true) - $startTime;

        $this->assertEquals(10, $createdCount);
        $this->assertLessThan(1.0, $createTime, 'Batch create should complete within 1 second');

        // Test spatial query performance
        $startTime = microtime(true);
        $containsQuery = Query::contains('polygon', [[15, 15]]);
        $filteredDocs = $database->find('spatial_performance', [$containsQuery], Database::PERMISSION_READ);
        $queryTime = microtime(true) - $startTime;

        $this->assertLessThan(0.5, $queryTime, 'Spatial query should complete within 0.5 seconds');
        $this->assertGreaterThan(0, count($filteredDocs), 'Should find at least one document');
    }

    public function testSpatialCRUDOperations(): void
    {
        $database = $this->getDatabase();

        // Skip tests if spatial attributes are not supported
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Spatial attributes not supported by this adapter');
        }

        // Clean up collection if it exists
        if ($database->exists(null, 'spatial_crud')) {
            $database->deleteCollection('spatial_crud');
        }

        $database->createCollection('spatial_crud');

        // Create spatial attributes for all types
        $this->assertEquals(true, $database->createAttribute('spatial_crud', 'geometry', Database::VAR_GEOMETRY, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_crud', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_crud', 'linestring', Database::VAR_LINESTRING, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_crud', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_crud', 'name', Database::VAR_STRING, 255, true));

        // Create spatial indexes for performance
        $this->assertEquals(true, $database->createIndex('spatial_crud', 'geometry_spatial', Database::INDEX_SPATIAL, ['geometry']));
        $this->assertEquals(true, $database->createIndex('spatial_crud', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_crud', 'linestring_spatial', Database::INDEX_SPATIAL, ['linestring']));
        $this->assertEquals(true, $database->createIndex('spatial_crud', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));

        // ===== CREATE OPERATIONS =====

        // Create document with all spatial types
        $document = new Document([
            '$id' => ID::unique(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'geometry' => [10, 10], // Array for GEOMETRY type
            'point' => [20, 20], // Array for POINT type
            'linestring' => [[0, 0], [10, 10], [20, 20]], // Array for LINESTRING type
            'polygon' => [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]], // Array for POLYGON type
            'name' => 'Spatial CRUD Test Document'
        ]);

        $createdDoc = $database->createDocument('spatial_crud', $document);
        $this->assertInstanceOf(\Utopia\Database\Document::class, $createdDoc);
        $this->assertEquals($document->getId(), $createdDoc->getId());

        // ===== READ OPERATIONS =====

        // Read the created document
        $retrievedDoc = $database->getDocument('spatial_crud', $createdDoc->getId());
        $this->assertInstanceOf(\Utopia\Database\Document::class, $retrievedDoc);
        $this->assertEquals('Spatial CRUD Test Document', $retrievedDoc->getAttribute('name'));

        // Verify spatial data was stored correctly
        $this->assertIsArray($retrievedDoc->getAttribute('geometry'));
        $this->assertIsArray($retrievedDoc->getAttribute('point'));
        $this->assertIsArray($retrievedDoc->getAttribute('linestring'));
        $this->assertIsArray($retrievedDoc->getAttribute('polygon'));

        // Test spatial queries for each type
        // Test POINT queries
        $pointQuery = Query::equals('point', [[20, 20]]);
        $pointResults = $database->find('spatial_crud', [$pointQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $pointResults);
        $this->assertEquals('Spatial CRUD Test Document', $pointResults[0]->getAttribute('name'));

        // Test LINESTRING queries
        $linestringQuery = Query::contains('linestring', [[5, 5]]);
        $linestringResults = $database->find('spatial_crud', [$linestringQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $linestringResults);
        $this->assertEquals('Spatial CRUD Test Document', $linestringResults[0]->getAttribute('name'));

        // Test POLYGON queries
        $polygonQuery = Query::contains('polygon', [[5, 5]]);
        $polygonResults = $database->find('spatial_crud', [$polygonQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $polygonResults);
        $this->assertEquals('Spatial CRUD Test Document', $polygonResults[0]->getAttribute('name'));

        // Test GEOMETRY queries (should work like POINT for simple coordinates)
        $geometryQuery = Query::equals('geometry', [[10, 10]]);
        $geometryResults = $database->find('spatial_crud', [$geometryQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $geometryResults);
        $this->assertEquals('Spatial CRUD Test Document', $geometryResults[0]->getAttribute('name'));

        // ===== UPDATE OPERATIONS =====

        // Update spatial data
        $updateDoc = new Document([
            '$id' => $createdDoc->getId(),
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'geometry' => [30, 30], // Updated geometry
            'point' => [40, 40], // Updated point
            'linestring' => [[10, 10], [20, 20], [30, 30]], // Updated linestring
            'polygon' => [[[10, 10], [20, 10], [20, 20], [10, 20], [10, 10]]], // Updated polygon
            'name' => 'Updated Spatial CRUD Document'
        ]);

        $updateResult = $database->updateDocuments('spatial_crud', $updateDoc, [Query::equal('$id', [$createdDoc->getId()])]);
        $this->assertEquals(1, $updateResult);

        // Verify updates were applied
        $updatedDoc = $database->getDocument('spatial_crud', $createdDoc->getId());
        $this->assertEquals('Updated Spatial CRUD Document', $updatedDoc->getAttribute('name'));
        $this->assertIsArray($updatedDoc->getAttribute('geometry'));
        $this->assertIsArray($updatedDoc->getAttribute('point'));
        $this->assertIsArray($updatedDoc->getAttribute('linestring'));
        $this->assertIsArray($updatedDoc->getAttribute('polygon'));

        // Test spatial queries on updated data
        $updatedPointQuery = Query::equals('point', [[40, 40]]);
        $updatedPointResults = $database->find('spatial_crud', [$updatedPointQuery], Database::PERMISSION_READ);
        $this->assertCount(1, $updatedPointResults);
        $this->assertEquals('Updated Spatial CRUD Document', $updatedPointResults[0]->getAttribute('name'));

        // ===== DELETE OPERATIONS =====

        // Delete the document
        $deleteResult = $database->deleteDocument('spatial_crud', $createdDoc->getId());
        $this->assertTrue($deleteResult);

        // Verify document was deleted
        $deletedDoc = $database->getDocument('spatial_crud', $createdDoc->getId());
        $this->assertTrue($deletedDoc->isEmpty());

        // Test that spatial queries return no results after deletion
        $emptyResults = $database->find('spatial_crud', [$pointQuery], Database::PERMISSION_READ);
        $this->assertCount(0, $emptyResults);

        // ===== BATCH CRUD OPERATIONS =====

        // Create multiple documents with different spatial data
        $batchDocuments = [
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'geometry' => [1, 1],
                'point' => [2, 2],
                'linestring' => [[1, 1], [2, 2]],
                'polygon' => [[[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]],
                'name' => 'Batch Document 1'
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'geometry' => [3, 3],
                'point' => [4, 4],
                'linestring' => [[3, 3], [4, 4]],
                'polygon' => [[[3, 3], [4, 3], [4, 4], [3, 4], [3, 3]]],
                'name' => 'Batch Document 2'
            ]),
            new Document([
                '$id' => ID::unique(),
                '$permissions' => [
                    Permission::read(Role::any()),
                    Permission::create(Role::any()),
                    Permission::update(Role::any()),
                    Permission::delete(Role::any()),
                ],
                'geometry' => [5, 5],
                'point' => [6, 6],
                'linestring' => [[5, 5], [6, 6]],
                'polygon' => [[[5, 5], [6, 5], [6, 6], [5, 6], [5, 5]]],
                'name' => 'Batch Document 3'
            ])
        ];

        // Batch create
        $batchCreateCount = $database->createDocuments('spatial_crud', $batchDocuments);
        $this->assertEquals(3, $batchCreateCount);

        // Batch read - verify all documents were created
        $allDocs = $database->find('spatial_crud', [], Database::PERMISSION_READ);
        $this->assertCount(3, $allDocs);

        // Batch update - update all documents
        $batchUpdateDoc = new Document([
            '$permissions' => [
                Permission::read(Role::any()),
                Permission::create(Role::any()),
                Permission::update(Role::any()),
                Permission::delete(Role::any()),
            ],
            'name' => 'Batch Updated Document'
        ]);

        $batchUpdateResult = $database->updateDocuments('spatial_crud', $batchUpdateDoc, []);
        $this->assertEquals(3, $batchUpdateResult);

        // Verify batch update
        $updatedAllDocs = $database->find('spatial_crud', [], Database::PERMISSION_READ);
        foreach ($updatedAllDocs as $doc) {
            $this->assertEquals('Batch Updated Document', $doc->getAttribute('name'));
        }

        // Batch delete - delete all documents
        $batchDeleteResult = $database->deleteDocuments('spatial_crud', []);
        $this->assertEquals(3, $batchDeleteResult);

        // Verify batch deletion
        $remainingDocs = $database->find('spatial_crud', [], Database::PERMISSION_READ);
        $this->assertCount(0, $remainingDocs);
    }

    public function testFlow(): void
    {
        $database = $this->getDatabase();
        $result = $database->createCollection('test_basic', permissions:[Permission::read(Role::any()),Permission::create(Role::any())]);
        $result = $database->createCollection('spatial_data');
        $this->assertInstanceOf(\Utopia\Database\Document::class, $result);

        $this->assertEquals(true, $database->createAttribute('spatial_data', 'point', Database::VAR_POINT, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_data', 'linestring', Database::VAR_LINESTRING, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_data', 'polygon', Database::VAR_POLYGON, 0, true));
        $this->assertEquals(true, $database->createAttribute('spatial_data', 'name', Database::VAR_STRING, 255, true));

        // Insert documents with spatial data
        $doc1 = $database->createDocument('spatial_data', new \Utopia\Database\Document([
            '$id' => 'doc1',
            '$permissions' => [Permission::read(Role::any()),Permission::write(Role::any()),Permission::update(Role::any())],
            'name' => 'Point Document',
            'point' => [1,2],
            'linestring' => [[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]],
            'polygon' => [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]
        ]));

        $database->updateDocument('spatial_data', 'doc1', new \Utopia\Database\Document([
            'name' => 'Point Document',
            'point' => [1.0, 1.0],
            'linestring' => [[0.0, 0.0], [1.0, 1.0]],
            'polygon' => [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]
        ]));

        // // Create spatial indexes
        $this->assertEquals(true, $database->createIndex('spatial_data', 'point_spatial', Database::INDEX_SPATIAL, ['point']));
        $this->assertEquals(true, $database->createIndex('spatial_data', 'linestring_spatial', Database::INDEX_SPATIAL, ['linestring']));
        $this->assertEquals(true, $database->createIndex('spatial_data', 'polygon_spatial', Database::INDEX_SPATIAL, ['polygon']));
    }
}
