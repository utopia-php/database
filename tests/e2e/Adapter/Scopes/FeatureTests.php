<?php

namespace Tests\E2E\Adapter\Scopes;

use Utopia\Database\Adapter\Feature;
use Utopia\Database\Attribute;
use Utopia\Database\Collection;
use Utopia\Database\Database;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Query\Schema\ColumnType;

trait FeatureTests
{
    public function testSpatialFeatureIsWhetherAPointAttributeCanBeCreated(): void
    {
        /** @var Database $database */
        $database = $this->getDatabase();
        $spatial = $database->getAdapter()->hasFeature(Feature\Spatial::class);

        $this->assertSame($spatial, $database->profile()->hasFeature(Feature\Spatial::class));

        $collection = 'feature_spatial';
        $database->createCollection(Collection::create(id: $collection));

        try {
            if (! $spatial) {
                $this->expectException(DatabaseException::class);
                $this->expectExceptionMessage('Spatial attributes are not supported');
            }

            $database->createAttribute($collection, Attribute::point(key: 'location', required: true));

            $types = [];
            foreach ($database->getCollection($collection)->attributes() as $attribute) {
                $types[$attribute->key] = $attribute->type;
            }
            $this->assertSame(ColumnType::Point, $types['location'] ?? null);
        } finally {
            $database->deleteCollection($collection);
        }
    }
}
