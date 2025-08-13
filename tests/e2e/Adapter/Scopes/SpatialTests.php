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
    public function testSpatialTypeDocuments(): void
    {
        /** @var Database $database */
        $database = static::getDatabase();
        if (!$database->getAdapter()->getSupportForSpatialAttributes()) {
            $this->markTestSkipped('Adapter does not support spatial attributes');
        }

        $collectionName = 'test_spatial_doc_' . uniqid();
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
                // TODO: for MARIADB and POSTGRES it is changing
                // 'contains' => Query::contains('lineAttr', [[1.0, 2.0]]), // Point on the line (endpoint)
                // 'notContains' => Query::notContains('lineAttr', [[5.0, 6.0]]), // Point not on the line
                'equals' => Query::equals('lineAttr', [[[1.0, 2.0], [3.0, 4.0]]]), // Exact same linestring
                'notEquals' => Query::notEquals('lineAttr', [[[5.0, 6.0], [7.0, 8.0]]]), // Different linestring
                'intersects' => Query::intersects('lineAttr', [[1.0, 2.0]]), // Point on the line should intersect
                'notIntersects' => Query::notIntersects('lineAttr', [[5.0, 6.0]]) // Point not on the line should not intersect
            ];

            foreach ($lineQueries as $queryType => $query) {
                $result = $database->find($collectionName, [$query], Database::PERMISSION_READ);
                $this->assertNotEmpty($result, sprintf('Failed spatial query: %s on lineAttr', $queryType));
                $this->assertEquals('doc1', $result[0]->getId(), sprintf('Incorrect document returned for %s on lineAttr', $queryType));
            }

            // Polygon attribute tests - use operations valid for polygons
            $polyQueries = [
                // TODO: for MARIADB and POSTGRES it is changing
                // 'contains' => Query::contains('polyAttr', [[5.0, 5.0]]), // Point inside polygon
                // 'notContains' => Query::notContains('polyAttr', [[15.0, 15.0]]), // Point outside polygon
                'intersects' => Query::intersects('polyAttr', [[5.0, 5.0]]), // Point inside polygon should intersect
                'notIntersects' => Query::notIntersects('polyAttr', [[15.0, 15.0]]), // Point outside polygon should not intersect
                'equals' => Query::equals('polyAttr', [[[[0.0, 0.0], [0.0, 10.0], [10.0, 10.0], [0.0, 0.0]]]]), // Exact same polygon
                'notEquals' => Query::notEquals('polyAttr', [[[[20.0, 20.0], [20.0, 30.0], [30.0, 30.0], [20.0, 20.0]]]]), // Different polygon
                'overlaps' => Query::overlaps('polyAttr', [[[[5.0, 5.0], [5.0, 15.0], [15.0, 15.0], [15.0, 5.0], [5.0, 5.0]]]]), // Overlapping polygon
                'notOverlaps' => Query::notOverlaps('polyAttr', [[[[20.0, 20.0], [20.0, 30.0], [30.0, 30.0], [30.0, 20.0], [20.0, 20.0]]]]) // Non-overlapping polygon
            ];

            foreach ($polyQueries as $queryType => $query) {
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
}
